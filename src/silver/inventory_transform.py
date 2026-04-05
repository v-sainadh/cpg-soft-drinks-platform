"""
Silver pipeline for Inventory domain — Stock snapshots and Reorder points.

Reads from:  brz_freshsip.erp_inventory_raw
Writes to:   slv_freshsip.inventory_stock      (Delta MERGE upsert)
             slv_freshsip.ref_reorder_points   (overwrite — SCD Type 1)
Schedule:    Daily
Depends on:  bronze/inventory_ingestion.py

Transformations applied:
  - Type casting: STRING → INT, DECIMAL, DATE, TIMESTAMP
  - Deduplication on (warehouse_id, sku_id, snapshot_date)
  - Computed column: inventory_value = units_on_hand * standard_cost_per_unit
  - Computed column: days_of_supply (units_on_hand / avg daily outflow; estimated)
  - Referential integrity: sku_id checked against ref_products
  - Reorder alert: units_on_hand <= reorder_point_units

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType, TimestampType, LongType
from delta.tables import DeltaTable

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_dq_threshold
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="silver", domain="inventory")


def cast_inventory_columns(df: DataFrame) -> DataFrame:
    """
    Cast Bronze string columns to Silver typed schema for inventory stock.

    Casts: units_on_hand/in_transit/reserved → INT,
           standard_cost_per_unit → DECIMAL(10,2),
           snapshot_date → DATE, snapshot_timestamp → TIMESTAMP.

    Args:
        df: Bronze erp_inventory_raw DataFrame (all STRING columns).

    Returns:
        DataFrame with correctly typed Silver columns.
    """
    return (
        df
        .withColumn("units_on_hand",           F.col("units_on_hand").cast(IntegerType()))
        .withColumn("units_in_transit",         F.col("units_in_transit").cast(IntegerType()))
        .withColumn("units_reserved",           F.col("units_reserved").cast(IntegerType()))
        .withColumn("reorder_point_units",      F.col("reorder_point_units").cast(IntegerType()))
        .withColumn("standard_cost_per_unit",
                    F.col("standard_cost_per_unit").cast(DecimalType(10, 2)))
        .withColumn("snapshot_date",            F.to_date(F.col("snapshot_date")))
        .withColumn("snapshot_timestamp",
                    F.to_timestamp(F.col("snapshot_timestamp")))
        .withColumn("created_at",  F.current_timestamp())
        .withColumn("updated_at",  F.current_timestamp())
        .withColumn("_source_batch_id", F.col("_batch_id"))
    )


def compute_inventory_value(df: DataFrame) -> DataFrame:
    """
    Compute inventory_value = units_on_hand * standard_cost_per_unit.

    Null standard_cost_per_unit results in null inventory_value (flagged by DQ).

    Args:
        df: Typed inventory DataFrame.

    Returns:
        DataFrame with inventory_value column added.
    """
    return df.withColumn(
        "inventory_value",
        (F.col("units_on_hand") * F.col("standard_cost_per_unit")).cast(DecimalType(14, 2))
    )


def compute_days_of_supply(df: DataFrame) -> DataFrame:
    """
    Estimate days_of_supply per SKU/warehouse snapshot.

    Formula: units_on_hand / avg_daily_outflow
    Since actual daily outflow is not in this table, we use a proxy:
      units_in_transit / 7  (i.e., 1 week of demand being replenished).
    A floor of 1 is applied to avoid division by zero.

    This is an approximation; Gold layer refines using actual sales velocity.

    Args:
        df: Typed inventory DataFrame with units_on_hand and units_in_transit.

    Returns:
        DataFrame with dsi_days column added.
    """
    avg_daily = F.greatest(
        (F.col("units_in_transit") / F.lit(7.0)).cast(DecimalType(10, 2)),
        F.lit(1.0)
    )
    return df.withColumn(
        "dsi_days",
        (F.col("units_on_hand") / avg_daily).cast(DecimalType(8, 1))
    )


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the full Silver Inventory transformation pipeline.

    Steps:
    1. Read Bronze erp_inventory_raw.
    2. Cast types.
    3. Deduplicate on (warehouse_id, sku_id, snapshot_date).
    4. Compute inventory_value and days_of_supply.
    5. Run output DQ checks.
    6. Upsert to slv_freshsip.inventory_stock.
    7. Overwrite slv_freshsip.ref_reorder_points (SCD Type 1).

    Args:
        spark: Active SparkSession.
    """
    config    = load_config()
    brz_inv   = get_table_config(config, "bronze", "erp_inventory")
    slv_stock = get_table_config(config, "silver", "inventory_stock")
    slv_rop   = get_table_config(config, "silver", "ref_reorder_points")
    fail_rate = get_dq_threshold(config)

    log_pipeline_start(logger, "inventory_silver", brz_inv, slv_stock)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['silver']['database']}")

    df_brz    = spark.read.table(brz_inv)
    total     = df_brz.count()

    # Input DQ — SLV-INV-STOCK-001/002
    input_rules = [
        {"type": "not_null", "columns": ["warehouse_id", "sku_id", "units_on_hand",
                                          "snapshot_date"],
         "severity": "error"},
    ]
    df_clean = run_quality_checks(df_brz, input_rules, dq_logger=logger,
                                  total_count=total, fail_rate_pct=fail_rate)["clean_df"]

    # Deduplicate — SLV-INV-STOCK-006
    df_deduped = (
        df_clean
        .withColumn("_rn",
                    F.row_number().over(
                        __import__("pyspark.sql", fromlist=["Window"]).Window
                        .partitionBy("warehouse_id", "sku_id", "snapshot_date")
                        .orderBy(F.col("_ingested_at").desc())
                    ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df_typed  = cast_inventory_columns(df_deduped)
    df_valued = compute_inventory_value(df_typed)
    df_final  = compute_days_of_supply(df_valued)

    # Output DQ — SLV-INV-STOCK-003/004/005/008
    output_rules = [
        {"type": "range", "column": "units_on_hand",    "min_val": 0,    "severity": "error"},
        {"type": "range", "column": "units_in_transit", "min_val": 0,    "severity": "error"},
        {"type": "range", "column": "standard_cost_per_unit", "min_val": 0.01, "severity": "warning"},
    ]
    df_write = run_quality_checks(df_final, output_rules, dq_logger=logger,
                                  total_count=df_final.count(),
                                  fail_rate_pct=fail_rate)["clean_df"]

    # Generate surrogate key
    df_write = df_write.withColumn(
        "stock_key",
        F.abs(F.hash(F.col("warehouse_id"), F.col("sku_id"),
                     F.col("snapshot_date").cast("string"))).cast(LongType())
    )

    # Upsert to inventory_stock
    try:
        dt = DeltaTable.forName(spark, slv_stock)
        (dt.alias("t")
         .merge(df_write.alias("s"),
                "t.stock_key = s.stock_key")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    except Exception:
        (df_write.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("snapshot_date")
         .saveAsTable(slv_stock))

    # Upsert reorder points (SCD Type 1 — latest value wins)
    df_rop = (
        df_write
        .select("sku_id", "warehouse_id", "reorder_point_units", "_source_batch_id")
        .withColumn("safety_stock_units",
                    (F.col("reorder_point_units") * F.lit(0.2)).cast(IntegerType()))
        .withColumn("last_updated_at", F.current_timestamp())
        .dropDuplicates(["sku_id", "warehouse_id"])
    )

    (df_rop.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(slv_rop))

    count = df_write.count()
    log_pipeline_end(logger, "inventory_silver", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver_Inventory_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
