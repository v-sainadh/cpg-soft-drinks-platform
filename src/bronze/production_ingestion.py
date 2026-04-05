"""
Bronze pipeline for Production domain — Batches, QC checks, and Downtime events.

Reads from: data/synthetic/production/batches.json
            data/synthetic/production/quality_checks.json
            data/synthetic/production/downtime_events.json
Writes to:  brz_freshsip.iot_sensor_events_raw  (append-only, unified event stream)
Schedule:   Near-real-time / daily batch fallback
Depends on: None

The three production source files are unified into a single event stream table
where each record is one IoT-style sensor event. Event types:
  - BATCH_START / BATCH_END  (from batches.json)
  - QC_CHECK                 (from quality_checks.json)
  - DOWNTIME_UNPLANNED / DOWNTIME_PLANNED  (from downtime_events.json)

Author: Data Engineer Agent
"""

import uuid
import datetime
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_source_path
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="bronze", domain="production")

# Downtime categories that map to PLANNED event type
_PLANNED_CATEGORIES = {"PLANNED_MAINTENANCE", "CHANGEOVER"}


def _add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    """
    Add five standard Bronze metadata columns. Cast all data columns to STRING.

    Args:
        df: Input DataFrame.
        source_path: Source file path for lineage.
        batch_id: UUID string for this ingestion run.

    Returns:
        DataFrame with STRING data columns and 5 metadata columns.
    """
    string_cols = [F.col(c).cast(StringType()).alias(c) for c in df.columns]
    df = df.select(string_cols)
    run_id = f"manual_{datetime.date.today().isoformat()}"

    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_pipeline_run_id", F.lit(run_id))
        .withColumn("ingestion_date", F.to_date(F.col("_ingested_at")))
    )


def _to_event_schema(df: DataFrame) -> DataFrame:
    """
    Ensure a DataFrame has exactly the Bronze iot_sensor_events_raw schema columns.
    Missing columns are added as null STRING. Extra columns are dropped.

    Args:
        df: Input DataFrame with some event columns.

    Returns:
        DataFrame with standardised event schema.
    """
    expected_cols = [
        "event_id", "batch_id", "production_line_id", "event_type",
        "event_timestamp", "sku_id", "expected_output_cases",
        "actual_output_cases", "qc_status", "downtime_start_ts",
        "downtime_end_ts", "sensor_temperature", "sensor_pressure", "operator_id",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(StringType()))
    return df.select(expected_cols)


def ingest_batches(spark: SparkSession, config: dict, batch_id: str) -> DataFrame:
    """
    Read production batches JSON and convert to IoT event rows.

    Each batch produces TWO event rows:
    - BATCH_START  using start_time as event_timestamp
    - BATCH_END    using end_time as event_timestamp

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this ingestion run.

    Returns:
        DataFrame with batch start + end events in IoT event schema.
    """
    source_path = get_source_path(config, "production", "batches.json")
    log_pipeline_start(logger, "production_batches_bronze", source_path,
                       config["tables"]["bronze"]["iot_sensor_events"])

    df_raw = spark.read.json(source_path)

    # BATCH_START events
    df_start = (
        df_raw
        .withColumn("event_id", F.concat(F.col("batch_id"), F.lit("_START")))
        .withColumn("event_type", F.lit("BATCH_START"))
        .withColumn("event_timestamp", F.col("start_time"))
        .withColumn("production_line_id", F.col("production_line"))
        .withColumn("expected_output_cases", F.col("target_quantity").cast(StringType()))
        .withColumn("actual_output_cases", F.lit(None).cast(StringType()))
        .withColumn("qc_status", F.lit(None).cast(StringType()))
    )

    # BATCH_END events
    df_end = (
        df_raw
        .withColumn("event_id", F.concat(F.col("batch_id"), F.lit("_END")))
        .withColumn("event_type", F.lit("BATCH_END"))
        .withColumn("event_timestamp", F.col("end_time"))
        .withColumn("production_line_id", F.col("production_line"))
        .withColumn("expected_output_cases", F.col("target_quantity").cast(StringType()))
        .withColumn("actual_output_cases", F.col("actual_quantity").cast(StringType()))
        .withColumn("qc_status", F.col("status"))
    )

    df_events = df_start.unionByName(df_end, allowMissingColumns=True)
    return _to_event_schema(df_events)


def ingest_quality_checks(spark: SparkSession, config: dict, batch_id: str) -> DataFrame:
    """
    Read production QC check JSON and convert to IoT QC_CHECK event rows.

    Each QC check record becomes one QC_CHECK event.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this ingestion run.

    Returns:
        DataFrame with QC_CHECK events in IoT event schema.
    """
    source_path = get_source_path(config, "production", "quality_checks.json")

    df_raw = spark.read.json(source_path)

    df_events = (
        df_raw
        .withColumn("event_id", F.col("check_id"))
        .withColumn("event_type", F.lit("QC_CHECK"))
        .withColumn("event_timestamp", F.col("timestamp"))
        .withColumn("production_line_id", F.lit(None).cast(StringType()))
        .withColumn("sku_id", F.lit(None).cast(StringType()))
        .withColumn("expected_output_cases", F.lit(None).cast(StringType()))
        .withColumn("actual_output_cases", F.lit(None).cast(StringType()))
        .withColumn("qc_status", F.col("result"))
        .withColumn("sensor_temperature", F.lit(None).cast(StringType()))
        .withColumn("sensor_pressure", F.lit(None).cast(StringType()))
        .withColumn("operator_id", F.col("inspector_id"))
    )

    return _to_event_schema(df_events)


def ingest_downtime_events(spark: SparkSession, config: dict, batch_id: str) -> DataFrame:
    """
    Read downtime events JSON and convert to IoT downtime event rows.

    Maps category to DOWNTIME_PLANNED or DOWNTIME_UNPLANNED event type.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this ingestion run.

    Returns:
        DataFrame with DOWNTIME_* events in IoT event schema.
    """
    source_path = get_source_path(config, "production", "downtime_events.json")

    df_raw = spark.read.json(source_path)

    df_events = (
        df_raw
        .withColumn(
            "event_type",
            F.when(F.col("category").isin(list(_PLANNED_CATEGORIES)),
                   F.lit("DOWNTIME_PLANNED"))
            .otherwise(F.lit("DOWNTIME_UNPLANNED"))
        )
        .withColumn("event_id", F.col("event_id"))
        .withColumn("production_line_id", F.col("production_line"))
        .withColumn("event_timestamp", F.col("start_time"))
        .withColumn("downtime_start_ts", F.col("start_time"))
        .withColumn("downtime_end_ts", F.col("end_time"))
        .withColumn("batch_id", F.lit(None).cast(StringType()))
        .withColumn("sku_id", F.lit(None).cast(StringType()))
    )

    return _to_event_schema(df_events)


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the full Bronze Production ingestion pipeline.

    Combines batch, QC check, and downtime events into a single append write
    to brz_freshsip.iot_sensor_events_raw.

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    batch_id = str(uuid.uuid4())
    target_table = config["tables"]["bronze"]["iot_sensor_events"]

    logger.info("Production Bronze pipeline starting | batch_id=%s", batch_id)

    df_batches  = ingest_batches(spark, config, batch_id)
    df_qc       = ingest_quality_checks(spark, config, batch_id)
    df_downtime = ingest_downtime_events(spark, config, batch_id)

    df_all = df_batches.unionByName(df_qc).unionByName(df_downtime)

    total_count = df_all.count()

    # DQ checks — BRZ-PROD-IOT-001/002/003/005
    dq_rules = [
        {"type": "not_null", "columns": ["event_id", "event_type"], "severity": "error"},
    ]
    result = run_quality_checks(df_all, dq_rules, dq_logger=logger, total_count=total_count)
    df_clean = result["clean_df"]

    # Get source_path for metadata (combined)
    source_path = get_source_path(config, "production", "batches.json")
    df_final = _add_bronze_metadata(df_clean, source_path, batch_id)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['bronze']['database']}")
    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    log_pipeline_end(logger, "production_bronze", count)
    logger.info("Production Bronze pipeline complete | records=%d", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze_Production_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
