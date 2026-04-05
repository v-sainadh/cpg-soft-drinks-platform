# Databricks notebook source

"""
Gold pipeline — Fact Shipment.

Reads from:  slv_freshsip.shipments
             slv_freshsip.customers
             gld_freshsip.dim_date
Writes to:   gld_freshsip.fact_shipment  (overwrite by ship_date partition)
Schedule:    Daily
Depends on:  silver/distribution_transform.py, gold/dim_date.py

KPIs enabled: On-Time Delivery %, Order Fulfillment Rate

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, IntegerType

import sys
sys.path.append('/Workspace/FreshSip/src')

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="distribution")

# COMMAND ----------


def compute_fact_shipment(
    df_ship: DataFrame,
    df_customers: DataFrame,
    df_dim_date: DataFrame,
) -> DataFrame:
    """
    Build the fact_shipment table by joining Silver shipments with dimension tables.

    Args:
        df_ship:     Silver shipments DataFrame.
        df_customers: Silver customers DataFrame (current records).
        df_dim_date: Gold dim_date DataFrame.

    Returns:
        Fact shipment DataFrame ready for Gold write.
    """
    df_dates = df_dim_date.select(
        F.col("date_key").cast(IntegerType()),
        F.col("full_date")
    )

    df_cust_keys = (
        df_customers
        .filter(F.col("is_current") == True)
        .select(
            F.col("retailer_id"),
            F.col("surrogate_key").alias("customer_key"),
        )
    )

    df_wh_keys = df_ship.select(
        F.col("warehouse_id"),
        F.abs(F.hash(F.col("warehouse_id"))).cast(LongType()).alias("warehouse_key"),
    ).dropDuplicates(["warehouse_id"])

    df = (
        df_ship
        .join(df_dates, df_ship["ship_date"] == df_dates["full_date"], how="left")
        .join(df_cust_keys, on="retailer_id", how="left")
        .join(df_wh_keys, on="warehouse_id", how="left")
        .withColumn("channel_key",
                    F.abs(F.hash(F.col("channel"))).cast(LongType()))
        .withColumn("region_key",
                    F.abs(F.hash(F.col("region"))).cast(LongType()))
        .select(
            "shipment_key",
            "date_key",
            "customer_key",
            "warehouse_key",
            "channel_key",
            "region_key",
            "shipment_id",
            "route_id",
            "promised_delivery_date",
            "actual_delivery_date",
            "on_time_flag",
            "cases_delivered",
            "logistics_cost_usd",
            "is_fully_shipped",
            "ship_date",
        )
    )

    return df

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Gold Fact Shipment pipeline.

    Args:
        spark: Active SparkSession.
    """
    config   = load_config()
    slv_ship = get_table_config(config, "silver", "shipments")
    slv_cust = get_table_config(config, "silver", "customers")
    gld_dim  = get_table_config(config, "gold",   "dim_date")
    target   = get_table_config(config, "gold",   "fact_shipment")

    log_pipeline_start(logger, "fact_shipment_gold", slv_ship, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_ship     = spark.read.table(slv_ship)
    df_customers = spark.read.table(slv_cust)
    df_dim_date  = spark.read.table(gld_dim)

    df_fact = compute_fact_shipment(df_ship, df_customers, df_dim_date)

    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ship_date")
        .saveAsTable(target)
    )

    count = df_fact.count()
    log_pipeline_end(logger, "fact_shipment_gold", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_FactShipment_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
