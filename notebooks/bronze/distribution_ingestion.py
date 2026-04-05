# Databricks notebook source

# COMMAND ----------
"""
Bronze pipeline for Distribution domain — Logistics Shipments.

Reads from: data/synthetic/logistics/shipments.csv
Writes to:  brz_freshsip.logistics_shipments_raw  (append-only)
Schedule:   Daily
Depends on: None

Author: Data Engineer Agent
"""

import uuid
import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
_CONFIG = {
    "sources": {
        "logistics": {"base_path": "/Workspace/Users/naninadh.v@gmail.com/freshsip/data/logistics"},
    },
    "tables": {
        "bronze": {
            "logistics_shipments": "brz_freshsip.logistics_shipments_raw",
        }
    },
}

# COMMAND ----------
def _add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    """Add five standard Bronze metadata columns."""
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
def ingest_shipments(spark: SparkSession, batch_id: str) -> int:
    """Ingest logistics shipments CSV into brz_freshsip.logistics_shipments_raw."""
    source_path  = _CONFIG["sources"]["logistics"]["base_path"] + "/shipments.csv"
    target_table = _CONFIG["tables"]["bronze"]["logistics_shipments"]

    print(f"PIPELINE START | distribution_bronze | source={source_path} | target={target_table}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    df_mapped = (
        df_raw
        .withColumnRenamed("carrier", "carrier_id")
        .withColumnRenamed("estimated_delivery", "promised_delivery_date")
        .withColumnRenamed("actual_delivery", "actual_delivery_date")
        .withColumnRenamed("cases_shipped", "cases_delivered")
        .withColumnRenamed("freight_cost", "logistics_cost_usd")
        .withColumn("retailer_id", F.lit(""))
        .withColumn("route_id", F.lit(""))
        .withColumn("channel", F.lit(""))
        .withColumn("region", F.lit(""))
        .withColumn("state", F.lit(""))
        .withColumn("is_fully_shipped", F.lit("true"))
        .withColumn("quantity_ordered", F.lit("0"))
        .withColumn("quantity_shipped", F.col("cases_delivered"))
    )

    df_selected = df_mapped.select(
        "shipment_id", "order_id", "retailer_id", "warehouse_id",
        "route_id", "carrier_id", "ship_date", "promised_delivery_date",
        "actual_delivery_date", "cases_delivered", "logistics_cost_usd",
        "channel", "region", "state", "is_fully_shipped",
        "quantity_ordered", "quantity_shipped",
    )

    df_final = _add_bronze_metadata(df_selected, source_path, batch_id)

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
    print(f"PIPELINE END | distribution_bronze | records_written={count}")
    return count

# COMMAND ----------
# Main execution
batch_id = str(uuid.uuid4())
print(f"Distribution Bronze pipeline starting | batch_id={batch_id}")
count = ingest_shipments(spark, batch_id)
print(f"Distribution Bronze pipeline complete | records={count}")
