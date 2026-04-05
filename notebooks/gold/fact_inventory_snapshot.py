# Databricks notebook source

"""
Gold pipeline — Fact Inventory Snapshot.

Reads from:  slv_freshsip.inventory_stock
             slv_freshsip.ref_reorder_points
             gld_freshsip.dim_date
Writes to:   gld_freshsip.fact_inventory_snapshot  (overwrite by snapshot_date partition)
Schedule:    Daily
Depends on:  silver/inventory_transform.py, gold/dim_date.py

KPIs enabled: Inventory Turnover Rate, Days Sales of Inventory (DSI), Reorder Alerts

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, IntegerType, BooleanType

import sys
sys.path.append('/Workspace/FreshSip/src')

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="inventory")

# COMMAND ----------


def compute_fact_inventory_snapshot(
    df_stock: DataFrame,
    df_rop: DataFrame,
    df_dim_date: DataFrame,
) -> DataFrame:
    """
    Build the fact_inventory_snapshot table.

    Computed measures:
    - inventory_value     = units_on_hand * standard_cost_per_unit
    - reorder_alert_flag  = units_on_hand <= reorder_point_units
    - dsi_days            = from Silver inventory_stock (pre-computed)

    Args:
        df_stock:    Silver inventory_stock DataFrame.
        df_rop:      Silver ref_reorder_points DataFrame.
        df_dim_date: Gold dim_date DataFrame.

    Returns:
        Fact inventory snapshot DataFrame ready for Gold write.
    """
    df_dates = df_dim_date.select(
        F.col("date_key").cast(IntegerType()),
        F.col("full_date")
    )

    df_prod_keys = df_stock.select(
        F.col("sku_id"),
        F.abs(F.hash(F.col("sku_id"))).cast(LongType()).alias("product_key"),
    ).dropDuplicates(["sku_id"])

    df_wh_keys = df_stock.select(
        F.col("warehouse_id"),
        F.abs(F.hash(F.col("warehouse_id"))).cast(LongType()).alias("warehouse_key"),
    ).dropDuplicates(["warehouse_id"])

    df_with_rop = df_stock.join(
        df_rop.select("sku_id", "warehouse_id",
                      F.col("reorder_point_units").alias("rop")),
        on=["sku_id", "warehouse_id"], how="left"
    )

    df = (
        df_with_rop
        .join(df_dates, df_with_rop["snapshot_date"] == df_dates["full_date"], how="left")
        .join(df_prod_keys, on="sku_id", how="left")
        .join(df_wh_keys, on="warehouse_id", how="left")
        .withColumn("reorder_alert_flag",
                    (F.col("units_on_hand") <= F.coalesce(F.col("rop"), F.lit(0)))
                    .cast(BooleanType()))
        .withColumn("snapshot_key",
                    F.abs(F.hash(F.col("sku_id"), F.col("warehouse_id"),
                                 F.col("snapshot_date").cast("string"))).cast(LongType()))
        .select(
            "snapshot_key", "date_key", "product_key", "warehouse_key",
            "units_on_hand", "inventory_value",
            F.coalesce(F.col("rop"), F.lit(0)).cast(IntegerType()).alias("reorder_point_units"),
            "reorder_alert_flag", "dsi_days",
            "snapshot_date",
        )
    )

    return df

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Gold Fact Inventory Snapshot pipeline.

    Args:
        spark: Active SparkSession.
    """
    config   = load_config()
    slv_stk  = get_table_config(config, "silver", "inventory_stock")
    slv_rop  = get_table_config(config, "silver", "ref_reorder_points")
    gld_dim  = get_table_config(config, "gold",   "dim_date")
    target   = get_table_config(config, "gold",   "fact_inventory")

    log_pipeline_start(logger, "fact_inventory_gold", slv_stk, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_stock    = spark.read.table(slv_stk)
    df_rop      = spark.read.table(slv_rop)
    df_dim_date = spark.read.table(gld_dim)

    df_fact = compute_fact_inventory_snapshot(df_stock, df_rop, df_dim_date)

    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("snapshot_date")
        .saveAsTable(target)
    )

    count = df_fact.count()
    log_pipeline_end(logger, "fact_inventory_gold", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_FactInventory_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
