# Databricks notebook source

# COMMAND ----------
"""
Bronze pipeline for Sales domain — POS Transactions and ERP Sales Orders.

Reads from: data/synthetic/pos/pos_transactions.json
            data/synthetic/erp/orders.csv
            data/synthetic/erp/order_lines.csv
Writes to:  brz_freshsip.pos_transactions_raw
            brz_freshsip.erp_sales_raw
Schedule:   Daily
Depends on: None

Author: Data Engineer Agent
"""

import uuid
import datetime
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
# Inline config
_CONFIG = {
    "layers": {"bronze": {"database": "brz_freshsip"}},
    "sources": {
        "erp": {"base_path": "/Workspace/Users/naninadh.v@gmail.com/freshsip/data/erp"},
        "pos": {"base_path": "/Workspace/Users/naninadh.v@gmail.com/freshsip/data/pos"},
    },
    "tables": {
        "bronze": {
            "pos_transactions": "brz_freshsip.pos_transactions_raw",
            "erp_sales":        "brz_freshsip.erp_sales_raw",
        }
    },
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
def ingest_pos_transactions(spark: SparkSession, batch_id: str) -> int:
    """Ingest POS transaction JSON files into brz_freshsip.pos_transactions_raw."""
    source_path  = _CONFIG["sources"]["pos"]["base_path"] + "/pos_transactions.json"
    target_table = _CONFIG["tables"]["bronze"]["pos_transactions"]

    print(f"PIPELINE START | pos_transactions_bronze | source={source_path} | target={target_table}")

    df_raw = (
        spark.read
        .option("multiline", "false")
        .json(source_path)
    )

    df_exploded = (
        df_raw
        .withColumn("item", F.explode(F.col("items")))
        .select(
            F.col("transaction_id"),
            F.col("store_id"),
            F.col("customer_id").alias("retailer_id"),
            F.col("timestamp").alias("transaction_timestamp"),
            F.col("payment_method"),
            F.col("total").cast(StringType()).alias("transaction_total"),
            F.col("item.sku_id").cast(StringType()).alias("sku_id"),
            F.col("item.qty").cast(StringType()).alias("quantity"),
            F.col("item.price").cast(StringType()).alias("unit_price"),
            F.col("item.discount").cast(StringType()).alias("discount_pct"),
            F.lit("Retail").alias("channel"),
            F.lit("").alias("region"),
            F.lit("").alias("state"),
        )
    )

    df_final = _add_bronze_metadata(df_exploded, source_path, batch_id)

    spark.sql("CREATE DATABASE IF NOT EXISTS brz_freshsip")
    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    print(f"PIPELINE END | pos_transactions_bronze | records_written={count}")
    return count

# COMMAND ----------
def ingest_erp_sales(spark: SparkSession, batch_id: str) -> int:
    """Ingest ERP orders and order_lines CSVs into brz_freshsip.erp_sales_raw."""
    orders_path  = _CONFIG["sources"]["erp"]["base_path"] + "/orders.csv"
    lines_path   = _CONFIG["sources"]["erp"]["base_path"] + "/order_lines.csv"
    target_table = _CONFIG["tables"]["bronze"]["erp_sales"]

    print(f"PIPELINE START | erp_sales_bronze | source={orders_path} | target={target_table}")

    df_orders = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(orders_path)
        .select(
            F.col("order_id"),
            F.col("customer_id").alias("retailer_id"),
            F.col("order_date"),
            F.col("ship_date"),
            F.col("status").alias("order_status"),
        )
    )

    df_lines = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(lines_path)
        .select(
            F.col("order_line_id"),
            F.col("order_id"),
            F.col("sku_id"),
            F.col("quantity").alias("quantity_ordered"),
            F.col("unit_price").alias("invoice_price"),
            F.col("discount_pct"),
            F.col("line_total"),
        )
    )

    df_joined = df_lines.join(df_orders, on="order_id", how="left")
    df_joined = (
        df_joined
        .withColumn("quantity_shipped", F.col("quantity_ordered"))
        .withColumn("channel", F.lit(""))
        .withColumn("region", F.lit(""))
        .withColumn("state", F.lit(""))
    )

    df_final = _add_bronze_metadata(df_joined, orders_path, batch_id)

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    print(f"PIPELINE END | erp_sales_bronze | records_written={count}")
    return count

# COMMAND ----------
# Main execution
batch_id = str(uuid.uuid4())
print(f"Sales Bronze pipeline starting | batch_id={batch_id}")

pos_count = ingest_pos_transactions(spark, batch_id)
erp_count = ingest_erp_sales(spark, batch_id)

print(f"Sales Bronze pipeline complete | pos_records={pos_count} | erp_records={erp_count}")
