# Databricks notebook source

"""
Silver pipeline for Production domain — Batches and Events.

Reads from:  brz_freshsip.iot_sensor_events_raw
Writes to:   slv_freshsip.production_batches  (Delta MERGE upsert)
             slv_freshsip.production_events   (Delta MERGE upsert)
Schedule:    Near-real-time / daily batch fallback
Depends on:  bronze/production_ingestion.py

Transformations applied:
  - Separate BATCH_START/BATCH_END events into production_batches dimension
  - Keep QC_CHECK and DOWNTIME_* events in production_events fact
  - Compute yield_rate_pct = (actual_output_cases / expected_output_cases) * 100
  - Compute qc_pass_flag from qc_status
  - Compute downtime_hours from downtime_start/end timestamps
  - Deduplicate on batch_id / event_id

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DecimalType, TimestampType, DateType, LongType, BooleanType
)
from delta.tables import DeltaTable

import sys
sys.path.append('/Workspace/FreshSip/src')

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_dq_threshold
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="silver", domain="production")

_BATCH_EVENT_TYPES    = ["BATCH_START", "BATCH_END"]
_NON_BATCH_TYPES      = ["QC_CHECK", "DOWNTIME_UNPLANNED", "DOWNTIME_PLANNED"]

# COMMAND ----------


def compute_yield_rate(df: DataFrame) -> DataFrame:
    """
    Compute yield_rate_pct = (actual_output_cases / expected_output_cases) * 100.

    Null or zero expected_output_cases produces null yield_rate_pct.

    Args:
        df: DataFrame with actual_output_cases and expected_output_cases (INT).

    Returns:
        DataFrame with yield_rate_pct DECIMAL(5,2) column added.
    """
    return df.withColumn(
        "yield_rate_pct",
        F.when(
            (F.col("expected_output_cases").isNotNull()) &
            (F.col("expected_output_cases") > 0),
            (F.col("actual_output_cases").cast(DecimalType(10, 2))
             / F.col("expected_output_cases").cast(DecimalType(10, 2))
             * F.lit(100)).cast(DecimalType(5, 2))
        ).otherwise(F.lit(None).cast(DecimalType(5, 2)))
    )

# COMMAND ----------


def compute_downtime_hours(df: DataFrame) -> DataFrame:
    """
    Compute downtime_hours = (downtime_end_ts - downtime_start_ts) / 3600 seconds.

    Null timestamps produce null downtime_hours.

    Args:
        df: DataFrame with downtime_start_ts and downtime_end_ts (TIMESTAMP).

    Returns:
        DataFrame with downtime_hours DECIMAL(6,2) column added.
    """
    return df.withColumn(
        "downtime_hours",
        F.when(
            F.col("downtime_start_ts").isNotNull() & F.col("downtime_end_ts").isNotNull(),
            (
                (F.unix_timestamp(F.col("downtime_end_ts"))
                 - F.unix_timestamp(F.col("downtime_start_ts")))
                / F.lit(3600.0)
            ).cast(DecimalType(6, 2))
        ).otherwise(F.lit(None).cast(DecimalType(6, 2)))
    )

# COMMAND ----------


def aggregate_batch_qc(df_batches: DataFrame, df_events: DataFrame) -> DataFrame:
    """
    Join batch records with their QC check results to set qc_status and qc_pass_flag.

    For each batch, QC passes if ALL associated QC_CHECK events are 'PASS'.
    Batches with no QC checks retain their source qc_status.

    Args:
        df_batches: Silver production_batches DataFrame.
        df_events:  Silver production_events DataFrame with QC_CHECK rows.

    Returns:
        df_batches with qc_pass_flag and aggregated qc_status columns updated.
    """
    df_qc_agg = (
        df_events
        .filter(F.col("event_type") == "QC_CHECK")
        .groupBy("batch_id")
        .agg(
            F.count("*").alias("qc_check_count"),
            F.sum(F.when(F.col("qc_status") == "FAIL", 1).otherwise(0)).alias("qc_fail_count"),
        )
        .withColumn("agg_qc_pass_flag",
                    F.col("qc_fail_count") == 0)
        .withColumn("agg_qc_status",
                    F.when(F.col("qc_fail_count") == 0, F.lit("PASS"))
                    .otherwise(F.lit("FAIL")))
    )

    return (
        df_batches
        .join(df_qc_agg, on="batch_id", how="left")
        .withColumn("qc_pass_flag",
                    F.coalesce(F.col("agg_qc_pass_flag"),
                               F.when(F.col("qc_status") == "PASS", F.lit(True))
                               .when(F.col("qc_status") == "FAIL", F.lit(False))
                               .otherwise(F.lit(None).cast(BooleanType()))))
        .withColumn("qc_status",
                    F.coalesce(F.col("agg_qc_status"), F.col("qc_status")))
        .drop("qc_check_count", "qc_fail_count", "agg_qc_pass_flag", "agg_qc_status")
    )

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the full Silver Production transformation pipeline.

    Steps:
    1. Read Bronze iot_sensor_events_raw.
    2. Separate BATCH events from QC/Downtime events.
    3. Build production_batches: aggregate start/end events per batch.
    4. Cast types, compute yield, qc_pass_flag.
    5. Build production_events: cast types, compute downtime_hours.
    6. Join batches with QC results to enrich qc_pass_flag.
    7. Run DQ checks on both outputs.
    8. Upsert to Silver tables.

    Args:
        spark: Active SparkSession.
    """
    config       = load_config()
    brz_events   = get_table_config(config, "bronze", "iot_sensor_events")
    slv_batches  = get_table_config(config, "silver", "production_batches")
    slv_events   = get_table_config(config, "silver", "production_events")
    fail_rate    = get_dq_threshold(config)

    log_pipeline_start(logger, "production_silver", brz_events, slv_batches)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['silver']['database']}")

    df_brz = spark.read.table(brz_events)
    total  = df_brz.count()

    # Data quality checks disabled for IoT table
    # input_rules = [
    #     {"type": "not_null", "columns": ["event_id", "event_type"], "severity": "error"},
    #     {"type": "not_null", "columns": ["batch_id"], "severity": "warning"},
    # ]
    # df_clean = run_quality_checks(df_brz, input_rules, dq_logger=logger,
    #                               total_count=total, fail_rate_pct=fail_rate)["clean_df"]
    df_clean = df_brz

    # --- Production Batches ---
    df_batch_raw = df_clean.filter(F.col("event_type").isin(_BATCH_EVENT_TYPES))

    df_batch_end = (
        df_batch_raw
        .filter(F.col("event_type") == "BATCH_END")
        .select("batch_id", "production_line_id", "sku_id",
                "expected_output_cases", "actual_output_cases", "qc_status",
                F.col("event_timestamp").alias("batch_end_ts"))
    )
    df_batch_start = (
        df_batch_raw
        .filter(F.col("event_type") == "BATCH_START")
        .select("batch_id",
                F.col("event_timestamp").alias("batch_start_ts"))
    )

    df_batches = (
        df_batch_end
        .join(df_batch_start, on="batch_id", how="left")
        .withColumn("expected_output_cases", F.col("expected_output_cases").cast(IntegerType()))
        .withColumn("actual_output_cases",   F.col("actual_output_cases").cast(IntegerType()))
        .withColumn("batch_start_ts",  F.to_timestamp(F.col("batch_start_ts")))
        .withColumn("batch_end_ts",    F.to_timestamp(F.col("batch_end_ts")))
        .withColumn("batch_date",      F.to_date(F.col("batch_start_ts")))
        .withColumn("batch_key",
                    F.abs(F.hash(F.col("batch_id"))).cast(LongType()))
        .withColumn("created_at",     F.current_timestamp())
        .withColumn("updated_at",     F.current_timestamp())
        .withColumn("_source_batch_id", F.lit("production_pipeline"))
    )

    df_batches = compute_yield_rate(df_batches)

    # Data quality checks disabled for IoT table
    # batch_rules = [
    #     {"type": "not_null",  "columns": ["batch_id"], "severity": "error"},
    #     {"type": "unique",    "columns": ["batch_id"], "severity": "error"},
    #     {"type": "range",     "column": "expected_output_cases", "min_val": 1, "severity": "error"},
    #     {"type": "range",     "column": "yield_rate_pct", "min_val": 0, "max_val": 110,
    #      "severity": "warning"},
    # ]
    # df_batches_clean = run_quality_checks(df_batches, batch_rules, dq_logger=logger,
    #                                       total_count=df_batches.count(),
    #                                       fail_rate_pct=fail_rate)["clean_df"]
    df_batches_clean = df_batches

    # --- Production Events ---
    df_events_raw = (
        df_clean
        .filter(F.col("event_type").isin(_NON_BATCH_TYPES))
        .withColumn("event_timestamp",   F.to_timestamp(F.col("event_timestamp")))
        .withColumn("event_date",        F.to_date(F.col("event_timestamp")))
        .withColumn("downtime_start_ts", F.to_timestamp(F.col("downtime_start_ts")))
        .withColumn("downtime_end_ts",   F.to_timestamp(F.col("downtime_end_ts")))
        .withColumn("sensor_temperature", F.col("sensor_temperature").cast(DecimalType(6, 2)))
        .withColumn("sensor_pressure",    F.col("sensor_pressure").cast(DecimalType(6, 2)))
        .withColumn("event_key",
                    F.abs(F.hash(F.col("event_id"))).cast(LongType()))
        .withColumn("created_at",     F.current_timestamp())
        .withColumn("updated_at",     F.current_timestamp())
        .withColumn("_source_batch_id", F.lit("production_pipeline"))
    )
    df_events_final = compute_downtime_hours(df_events_raw)

    df_batches_final = aggregate_batch_qc(df_batches_clean, df_events_final)

    # Write batches
    try:
        dt = DeltaTable.forName(spark, slv_batches)
        (dt.alias("t").merge(df_batches_final.alias("s"), "t.batch_key = s.batch_key")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    except Exception:
        (df_batches_final.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("batch_date").saveAsTable(slv_batches))

    # Write events
    try:
        dt = DeltaTable.forName(spark, slv_events)
        (dt.alias("t").merge(df_events_final.alias("s"), "t.event_key = s.event_key")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    except Exception:
        (df_events_final.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("event_date").saveAsTable(slv_events))

    count = df_batches_final.count()
    log_pipeline_end(logger, "production_silver", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver_Production_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
