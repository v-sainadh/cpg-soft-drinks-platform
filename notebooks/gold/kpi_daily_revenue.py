# Databricks notebook source

"""
Gold KPI pipeline — Daily Revenue by Product Category and Region.

KPI #1 from project-context.md: Daily Revenue
Grain: report_date × product_category × region × channel

Reads from:  gld_freshsip.fact_sales
             slv_freshsip.ref_products  (for product_category)
             slv_freshsip.customers     (for region/channel)
Writes to:   gld_freshsip.kpi_daily_revenue  (overwrite by report_date partition)
Schedule:    Daily
Depends on:  gold/fact_sales.py

Formula:
  total_revenue    = SUM(net_revenue)
  total_cogs       = SUM(cogs)
  total_margin     = SUM(gross_margin_amount)
  margin_pct       = total_margin / total_revenue * 100
  order_count      = COUNT(DISTINCT transaction_key)
  units_sold       = SUM(quantity_sold)

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

import sys
sys.path.append('/Workspace/FreshSip/src')

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="kpi_revenue")

# COMMAND ----------


def compute_kpi_daily_revenue(
    df_fact: DataFrame,
    df_products: DataFrame,
    df_customers: DataFrame,
) -> DataFrame:
    """
    Compute daily revenue KPI aggregated by product_category, region, and channel.

    Args:
        df_fact:     Gold fact_sales DataFrame.
        df_products: Silver ref_products DataFrame (for product_category lookup).
        df_customers: Silver customers DataFrame (for region/channel lookup).

    Returns:
        Aggregated KPI DataFrame, one row per (report_date, product_category, region, channel).
    """
    df_prod = df_products.select(
        F.abs(F.hash(F.col("sku_id"))).alias("product_key"),
        F.col("product_category"),
    ).dropDuplicates(["product_key"])

    df_cust = (
        df_customers
        .filter(F.col("is_current") == True)
        .select(
            F.col("surrogate_key").alias("customer_key"),
            F.col("region"),
            F.col("channel"),
        )
    )

    df = (
        df_fact
        .join(df_prod, on="product_key", how="left")
        .join(df_cust, on="customer_key", how="left")
        .groupBy("transaction_date", "product_category", "region", "channel")
        .agg(
            F.sum("net_revenue").cast(DecimalType(16, 2)).alias("total_revenue"),
            F.sum("cogs").cast(DecimalType(16, 2)).alias("total_cogs"),
            F.sum("gross_margin_amount").cast(DecimalType(16, 2)).alias("total_gross_margin"),
            F.sum("quantity_sold").alias("units_sold"),
            F.countDistinct("transaction_key").alias("transaction_count"),
        )
        .withColumn(
            "gross_margin_pct",
            F.when(F.col("total_revenue") > 0,
                   (F.col("total_gross_margin") / F.col("total_revenue") * 100)
                   .cast(DecimalType(6, 2)))
            .otherwise(F.lit(0).cast(DecimalType(6, 2)))
        )
        .withColumnRenamed("transaction_date", "report_date")
    )

    return df

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Daily Revenue KPI pipeline.

    Args:
        spark: Active SparkSession.
    """
    config   = load_config()
    gld_fact = get_table_config(config, "gold",   "fact_sales")
    slv_prod = get_table_config(config, "silver", "products")
    slv_cust = get_table_config(config, "silver", "customers")
    target   = get_table_config(config, "gold",   "kpi_daily_revenue")

    log_pipeline_start(logger, "kpi_daily_revenue", gld_fact, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_fact     = spark.read.table(gld_fact)
    df_products = spark.read.table(slv_prod)
    df_customers = spark.read.table(slv_cust)

    df_kpi = compute_kpi_daily_revenue(df_fact, df_products, df_customers)

    (
        df_kpi.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("report_date")
        .saveAsTable(target)
    )

    count = df_kpi.count()
    log_pipeline_end(logger, "kpi_daily_revenue", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_KPI_DailyRevenue").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
