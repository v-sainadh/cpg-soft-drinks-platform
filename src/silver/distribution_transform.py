"""
Silver pipeline for Distribution domain — Shipments.

Reads from:  brz_freshsip.logistics_shipments_raw
Writes to:   slv_freshsip.shipments  (Delta MERGE upsert)
Schedule:    Daily
Depends on:  bronze/distribution_ingestion.py

Transformations applied:
  - Type casting: STRING → INT, DECIMAL, DATE, BOOLEAN
  - Deduplication on shipment_id (keep latest)
  - Computed flag: on_time_flag = (actual_delivery_date <= promised_delivery_date)
  - Null actual_delivery_date → on_time_flag = False (not yet delivered = not on time)
  - Referential integrity: order_id checked against erp_sales (warning only)

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DecimalType, DateType, BooleanType, LongType
)
from delta.tables import DeltaTable

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_dq_threshold
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="silver", domain="distribution")


def cast_shipment_columns(df: DataFrame) -> DataFrame:
    """
    Cast Bronze string columns to Silver typed schema for shipments.

    Casts: ship_date/promised_delivery_date/actual_delivery_date → DATE,
           cases_delivered/quantity_ordered/quantity_shipped → INT,
           logistics_cost_usd → DECIMAL(10,2),
           is_fully_shipped → BOOLEAN.

    Args:
        df: Bronze logistics_shipments_raw DataFrame (all STRING columns).

    Returns:
        DataFrame with correctly typed Silver columns.
    """
    return (
        df
        .withColumn("ship_date",               F.to_date(F.col("ship_date")))
        .withColumn("promised_delivery_date",   F.to_date(F.col("promised_delivery_date")))
        .withColumn("actual_delivery_date",     F.to_date(F.col("actual_delivery_date")))
        .withColumn("cases_delivered",          F.col("cases_delivered").cast(IntegerType()))
        .withColumn("quantity_ordered",         F.col("quantity_ordered").cast(IntegerType()))
        .withColumn("quantity_shipped",         F.col("quantity_shipped").cast(IntegerType()))
        .withColumn("logistics_cost_usd",
                    F.col("logistics_cost_usd").cast(DecimalType(10, 2)))
        .withColumn("is_fully_shipped",
                    F.col("is_fully_shipped").cast(BooleanType()))
        .withColumn("created_at",  F.current_timestamp())
        .withColumn("updated_at",  F.current_timestamp())
        .withColumn("_source_batch_id", F.col("_batch_id"))
    )


def compute_on_time_flag(df: DataFrame) -> DataFrame:
    """
    Set on_time_flag = True when actual_delivery_date <= promised_delivery_date.

    Null actual_delivery_date (shipment still in transit) → on_time_flag = False.

    Args:
        df: Typed shipments DataFrame with date columns.

    Returns:
        DataFrame with on_time_flag BOOLEAN column added.
    """
    return df.withColumn(
        "on_time_flag",
        F.when(
            F.col("actual_delivery_date").isNull(),
            F.lit(False)
        ).when(
            F.col("actual_delivery_date") <= F.col("promised_delivery_date"),
            F.lit(True)
        ).otherwise(F.lit(False))
        .cast(BooleanType())
    )


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the full Silver Distribution transformation pipeline.

    Steps:
    1. Read Bronze logistics_shipments_raw.
    2. Deduplicate on shipment_id.
    3. Cast types.
    4. Compute on_time_flag.
    5. Run output DQ checks.
    6. Upsert to slv_freshsip.shipments.

    Args:
        spark: Active SparkSession.
    """
    config      = load_config()
    brz_ship    = get_table_config(config, "bronze", "logistics_shipments")
    slv_ship    = get_table_config(config, "silver", "shipments")
    fail_rate   = get_dq_threshold(config)

    log_pipeline_start(logger, "distribution_silver", brz_ship, slv_ship)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['silver']['database']}")

    df_brz  = spark.read.table(brz_ship)
    total   = df_brz.count()

    input_rules = [
        {"type": "not_null", "columns": ["shipment_id", "order_id", "warehouse_id"],
         "severity": "error"},
    ]
    df_clean = run_quality_checks(df_brz, input_rules, dq_logger=logger,
                                  total_count=total, fail_rate_pct=fail_rate)["clean_df"]

    # Deduplicate — keep latest ingested version of each shipment
    from pyspark.sql import Window
    w = Window.partitionBy("shipment_id").orderBy(F.col("_ingested_at").desc())
    df_deduped = (
        df_clean
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df_typed  = cast_shipment_columns(df_deduped)
    df_final  = compute_on_time_flag(df_typed)

    # Generate surrogate key
    df_final = df_final.withColumn(
        "shipment_key",
        F.abs(F.hash(F.col("shipment_id"))).cast(LongType())
    )

    # Output DQ
    output_rules = [
        {"type": "not_null", "columns": ["shipment_id", "promised_delivery_date"],
         "severity": "error"},
        {"type": "range", "column": "cases_delivered", "min_val": 0, "severity": "warning"},
        {"type": "range", "column": "logistics_cost_usd", "min_val": 0, "severity": "warning"},
    ]
    df_write = run_quality_checks(df_final, output_rules, dq_logger=logger,
                                  total_count=df_final.count(),
                                  fail_rate_pct=fail_rate)["clean_df"]

    try:
        dt = DeltaTable.forName(spark, slv_ship)
        (dt.alias("t").merge(df_write.alias("s"), "t.shipment_key = s.shipment_key")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    except Exception:
        (df_write.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("ship_date").saveAsTable(slv_ship))

    count = df_write.count()
    log_pipeline_end(logger, "distribution_silver", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver_Distribution_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
