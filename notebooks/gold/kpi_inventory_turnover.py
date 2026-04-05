# Databricks notebook source

"""
Gold KPI pipeline — Inventory Turnover Rate by Warehouse.

KPI #2 from project-context.md: Inventory Turnover Rate
Grain: report_month × warehouse_id

Formula:
  inventory_turnover_rate = total_cogs / avg_inventory_value
  avg_inventory_value     = AVG(daily inventory_value) over the reporting period
  total_cogs              = SUM of COGS from fact_sales for the period

Reads from:  gld_freshsip.fact_inventory_snapshot  (for avg inventory value)
             gld_freshsip.fact_sales               (for COGS)
             slv_freshsip.ref_warehouses            (for warehouse attributes)
Writes to:   gld_freshsip.kpi_inventory_turnover   (overwrite by report_month partition)
Schedule:    Daily (monthly grain)
Depends on:  gold/fact_inventory_snapshot.py, gold/fact_sales.py

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="kpi_inventory")

# COMMAND ----------


def compute_kpi_inventory_turnover(
    df_inventory: DataFrame,
    df_sales: DataFrame,
) -> DataFrame:
    """
    Compute monthly inventory turnover rate per warehouse.

    Turnover Rate = COGS for month / Average inventory value for month.
    Higher turnover = faster-moving inventory.

    Args:
        df_inventory: Gold fact_inventory_snapshot DataFrame.
        df_sales:     Gold fact_sales DataFrame.

    Returns:
        KPI DataFrame with one row per (report_month, warehouse_key).
    """
    df_inv_monthly = (
        df_inventory
        .withColumn("report_month", F.date_format(F.col("snapshot_date"), "yyyy-MM"))
        .groupBy("warehouse_key", "report_month")
        .agg(
            F.avg("inventory_value").cast(DecimalType(16, 2)).alias("avg_inventory_value"),
            F.sum("units_on_hand").alias("total_units_on_hand"),
        )
    )

    df_cogs_monthly = (
        df_sales
        .withColumn("report_month", F.date_format(F.col("transaction_date"), "yyyy-MM"))
        .groupBy("warehouse_key", "report_month")
        .agg(
            F.sum("cogs").cast(DecimalType(16, 2)).alias("total_cogs"),
        )
    )

    df = (
        df_inv_monthly
        .join(df_cogs_monthly, on=["warehouse_key", "report_month"], how="left")
        .withColumn(
            "inventory_turnover_rate",
            F.when(
                F.col("avg_inventory_value") > 0,
                (F.col("total_cogs") / F.col("avg_inventory_value")).cast(DecimalType(8, 2))
            ).otherwise(F.lit(None).cast(DecimalType(8, 2)))
        )
        .withColumn(
            "days_inventory_outstanding",
            F.when(
                F.col("inventory_turnover_rate") > 0,
                (F.lit(365) / F.col("inventory_turnover_rate")).cast(DecimalType(8, 1))
            ).otherwise(F.lit(None).cast(DecimalType(8, 1)))
        )
    )

    return df

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Inventory Turnover KPI pipeline.

    Args:
        spark: Active SparkSession.
    """
    config     = load_config()
    gld_inv    = get_table_config(config, "gold", "fact_inventory")
    gld_sales  = get_table_config(config, "gold", "fact_sales")
    target     = get_table_config(config, "gold", "kpi_inv_turnover")

    log_pipeline_start(logger, "kpi_inventory_turnover", gld_inv, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_inventory = spark.read.table(gld_inv)
    df_sales     = spark.read.table(gld_sales)

    df_kpi = compute_kpi_inventory_turnover(df_inventory, df_sales)

    (
        df_kpi.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("report_month")
        .saveAsTable(target)
    )

    count = df_kpi.count()
    log_pipeline_end(logger, "kpi_inventory_turnover", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_KPI_InventoryTurnover").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
