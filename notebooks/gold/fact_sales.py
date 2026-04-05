# Databricks notebook source

"""
Gold pipeline — Fact Sales.

Builds the central sales fact table from Silver sales_transactions and enriches
with COGS and gross margin using Silver ref_products costs.

Reads from:  slv_freshsip.sales_transactions
             slv_freshsip.ref_products
             slv_freshsip.customers
             gld_freshsip.dim_date
Writes to:   gld_freshsip.fact_sales  (overwrite by transaction_date partition)
Schedule:    Daily
Depends on:  silver/sales_transform.py, silver/master_data_transform.py, gold/dim_date.py

KPIs enabled: Daily Revenue, Gross Margin by SKU

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, IntegerType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="sales")

# COMMAND ----------


def compute_fact_sales(
    df_txn: DataFrame,
    df_products: DataFrame,
    df_customers: DataFrame,
    df_dim_date: DataFrame,
) -> DataFrame:
    """
    Build the fact_sales table by joining Silver transactions with dimension tables.

    Computed measures:
    - net_revenue        = unit_price * quantity_sold
    - cogs               = standard_cost_per_unit * quantity_sold
    - gross_margin_amount = net_revenue - cogs
    - return_amount      = 0 (returns tracked separately)

    Args:
        df_txn:      Silver sales_transactions DataFrame.
        df_products: Silver ref_products DataFrame.
        df_customers: Silver customers DataFrame (current version only).
        df_dim_date: Gold dim_date DataFrame.

    Returns:
        Fact sales DataFrame ready to write to Gold.
    """
    df_dates = df_dim_date.select(
        F.col("date_key").cast(IntegerType()),
        F.col("full_date")
    )

    df_prod_keys = df_products.select(
        F.col("sku_id"),
        F.col("standard_cost_per_unit"),
        F.abs(F.hash(F.col("sku_id"))).cast(LongType()).alias("product_key"),
    )

    df_cust_keys = (
        df_customers
        .filter(F.col("is_current") == True)
        .select(
            F.col("retailer_id"),
            F.col("surrogate_key").alias("customer_key"),
        )
    )

    df = (
        df_txn
        .join(df_dates, df_txn["transaction_date"] == df_dates["full_date"], how="left")
        .join(df_prod_keys, on="sku_id", how="left")
        .join(df_cust_keys, on="retailer_id", how="left")
        .withColumn("net_revenue",
                    (F.col("unit_price") * F.col("quantity_sold")).cast(DecimalType(14, 2)))
        .withColumn("cogs",
                    (F.coalesce(F.col("standard_cost_per_unit"), F.lit(0))
                     * F.col("quantity_sold")).cast(DecimalType(14, 2)))
        .withColumn("gross_margin_amount",
                    (F.col("net_revenue") - F.col("cogs")).cast(DecimalType(14, 2)))
        .withColumn("return_amount", F.lit(0).cast(DecimalType(10, 2)))
        .select(
            F.col("transaction_key"),
            F.col("date_key"),
            F.col("product_key"),
            F.col("customer_key"),
            F.col("quantity_sold"),
            F.col("unit_price"),
            F.col("net_revenue"),
            F.col("return_amount"),
            F.col("cogs"),
            F.col("gross_margin_amount"),
            F.col("transaction_date"),
        )
    )

    return df

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Gold Fact Sales pipeline.

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    slv_txn  = get_table_config(config, "silver", "sales_transactions")
    slv_prod = get_table_config(config, "silver", "products")
    slv_cust = get_table_config(config, "silver", "customers")
    gld_dim  = get_table_config(config, "gold",   "dim_date")
    target   = get_table_config(config, "gold",   "fact_sales")

    log_pipeline_start(logger, "fact_sales_gold", slv_txn, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_txn      = spark.read.table(slv_txn)
    df_products = spark.read.table(slv_prod)
    df_customers = spark.read.table(slv_cust)
    df_dim_date  = spark.read.table(gld_dim)

    df_fact = compute_fact_sales(df_txn, df_products, df_customers, df_dim_date)

    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("transaction_date")
        .saveAsTable(target)
    )

    count = df_fact.count()
    log_pipeline_end(logger, "fact_sales_gold", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_FactSales_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
