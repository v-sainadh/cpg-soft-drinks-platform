# Databricks notebook source

# COMMAND ----------
"""
Bronze pipeline for Master Data domain — Products, Customers, Warehouses.

Reads from: data/synthetic/erp/products.csv
            data/synthetic/erp/customers.csv
            data/synthetic/erp/warehouses.csv
Writes to:  brz_freshsip.erp_products_raw
            brz_freshsip.erp_customers_raw
            brz_freshsip.erp_warehouses_raw
Schedule:   Daily
Depends on: None

Author: Data Engineer Agent
"""

import uuid
import datetime
import logging
import sys
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
# Databricks-compatible config — inline to avoid local module import issues
_CONFIG = {
    "layers": {
        "bronze": {"database": "brz_freshsip"},
        "silver": {"database": "slv_freshsip"},
        "gold":   {"database": "gld_freshsip"},
    },
    "sources": {
        "erp":        {"base_path": "/Volumes/workspace/default/freshsip_data/synthetic/erp"},
        "pos":        {"base_path": "/Volumes/workspace/default/freshsip_data/synthetic/pos"},
        "production": {"base_path": "/Volumes/workspace/default/freshsip_data/synthetic/production"},
        "logistics":  {"base_path": "/Volumes/workspace/default/freshsip_data/synthetic/logistics"},
    },
    "tables": {
        "bronze": {
            "erp_products":          "brz_freshsip.erp_products_raw",
            "erp_customers":         "brz_freshsip.erp_customers_raw",
            "erp_warehouses":        "brz_freshsip.erp_warehouses_raw",
            "pos_transactions":      "brz_freshsip.pos_transactions_raw",
            "erp_sales":             "brz_freshsip.erp_sales_raw",
            "erp_inventory":         "brz_freshsip.erp_inventory_raw",
            "iot_sensor_events":     "brz_freshsip.iot_sensor_events_raw",
            "logistics_shipments":   "brz_freshsip.logistics_shipments_raw",
        },
    },
    "thresholds": {"dq_fail_rate_pct": 5.0},
    "date_spine": {"start_year": 2023, "end_year": 2027},
}

# COMMAND ----------
def _add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    """Add five standard Bronze metadata columns. Cast all data columns to STRING."""
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

# COMMAND ----------
def ingest_products(spark: SparkSession, batch_id: str) -> int:
    """Ingest products CSV into brz_freshsip.erp_products_raw."""
    source_path  = _CONFIG["sources"]["erp"]["base_path"] + "/products.csv"
    target_table = _CONFIG["tables"]["bronze"]["erp_products"]

    print(f"PIPELINE START | products_bronze | source={source_path} | target={target_table}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    df_mapped = (
        df_raw
        .withColumnRenamed("category", "product_category")
        .withColumnRenamed("sub_category", "product_subcategory")
        .withColumnRenamed("unit_price", "list_price")
        .withColumnRenamed("cost_price", "standard_cost_per_unit")
        .withColumnRenamed("pack_size", "package_size_ml")
        .withColumnRenamed("status", "is_active")
        .withColumn("record_effective_date",
                    F.coalesce(F.col("launch_date"),
                               F.lit(datetime.date.today().isoformat())))
        .withColumn("brand", F.lit(""))
        .withColumn("packaging_type", F.lit(""))
        .withColumn("price_tier", F.lit(""))
    )

    df_selected = df_mapped.select(
        "sku_id", "product_name", "product_category", "product_subcategory",
        "brand", "packaging_type", "package_size_ml", "standard_cost_per_unit",
        "list_price", "price_tier", "is_active", "record_effective_date",
    )

    df_final = _add_bronze_metadata(df_selected, source_path, batch_id)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS brz_freshsip")
    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    print(f"PIPELINE END | products_bronze | records_written={count}")
    return count

# COMMAND ----------
def ingest_customers(spark: SparkSession, batch_id: str) -> int:
    """Ingest customers CSV into brz_freshsip.erp_customers_raw."""
    source_path  = _CONFIG["sources"]["erp"]["base_path"] + "/customers.csv"
    target_table = _CONFIG["tables"]["bronze"]["erp_customers"]

    print(f"PIPELINE START | customers_bronze | source={source_path} | target={target_table}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    df_mapped = (
        df_raw
        .withColumnRenamed("customer_id", "retailer_id")
        .withColumnRenamed("name", "retailer_name")
        .withColumnRenamed("segment", "retail_segment")
        .withColumnRenamed("acquisition_date", "account_activation_date")
        .withColumnRenamed("status", "account_status")
        .withColumn("record_effective_date", F.col("account_activation_date"))
        .withColumn("city", F.lit(""))
        .withColumn("trade_spend_usd", F.lit("0"))
        .withColumn("broker_commission_usd", F.lit("0"))
        .withColumn("field_sales_cost_usd", F.lit("0"))
    )

    df_selected = df_mapped.select(
        "retailer_id", "retailer_name", "retail_segment", "channel",
        "region", "state" if "state" in df_mapped.columns else F.lit("").alias("state"),
        "city", "credit_terms_days",
        "account_activation_date", "account_status",
        "trade_spend_usd", "broker_commission_usd", "field_sales_cost_usd",
        "record_effective_date",
    )

    df_final = _add_bronze_metadata(df_selected, source_path, batch_id)

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    print(f"PIPELINE END | customers_bronze | records_written={count}")
    return count

# COMMAND ----------
def ingest_warehouses(spark: SparkSession, batch_id: str) -> int:
    """Ingest warehouses CSV into brz_freshsip.erp_warehouses_raw."""
    source_path  = _CONFIG["sources"]["erp"]["base_path"] + "/warehouses.csv"
    target_table = _CONFIG["tables"]["bronze"]["erp_warehouses"]

    print(f"PIPELINE START | warehouses_bronze | source={source_path} | target={target_table}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    df_final = _add_bronze_metadata(df_raw, source_path, batch_id)

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    print(f"PIPELINE END | warehouses_bronze | records_written={count}")
    return count

# COMMAND ----------
# Main execution
batch_id = str(uuid.uuid4())
print(f"Master Data Bronze pipeline starting | batch_id={batch_id}")

prod_count = ingest_products(spark, batch_id)
cust_count = ingest_customers(spark, batch_id)
wh_count   = ingest_warehouses(spark, batch_id)

print(f"Master Data Bronze pipeline complete | products={prod_count} | customers={cust_count} | warehouses={wh_count}")
