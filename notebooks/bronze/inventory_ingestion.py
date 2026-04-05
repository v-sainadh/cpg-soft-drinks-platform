# Databricks notebook source

# COMMAND ----------
"""
Bronze pipeline for Inventory domain — Daily inventory snapshots.

Reads from: data/synthetic/erp/inventory_daily.csv
Writes to:  brz_freshsip.erp_inventory_raw  (append-only)
Schedule:   Daily (EOD snapshot)
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
        "erp": {"base_path": "/Workspace/Users/naninadh.v@gmail.com/freshsip/data/erp"},
    },
    "tables": {
        "bronze": {
            "erp_inventory": "brz_freshsip.erp_inventory_raw",
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
def ingest_inventory(spark: SparkSession, batch_id: str) -> int:
    """Ingest daily inventory snapshot CSV into brz_freshsip.erp_inventory_raw."""
    source_path  = _CONFIG["sources"]["erp"]["base_path"] + "/inventory_daily.csv"
    target_table = _CONFIG["tables"]["bronze"]["erp_inventory"]

    print(f"PIPELINE START | inventory_bronze | source={source_path} | target={target_table}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    df_mapped = (
        df_raw
        .withColumnRenamed("quantity_on_hand", "units_on_hand")
        .withColumnRenamed("quantity_on_order", "units_in_transit")
        .withColumnRenamed("reorder_point", "reorder_point_units")
        .withColumn("units_reserved", F.lit("0"))
        .withColumn("snapshot_timestamp",
                    F.concat(F.col("snapshot_date"), F.lit("T00:00:00")))
        .withColumn("standard_cost_per_unit", F.lit(None).cast(StringType()))
    )

    df_selected = df_mapped.select(
        "warehouse_id", "sku_id", "units_on_hand", "units_in_transit",
        "units_reserved", "snapshot_date", "snapshot_timestamp",
        "reorder_point_units", "standard_cost_per_unit",
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
    print(f"PIPELINE END | inventory_bronze | records_written={count}")
    return count

# COMMAND ----------
# Main execution
batch_id = str(uuid.uuid4())
print(f"Inventory Bronze pipeline starting | batch_id={batch_id}")
count = ingest_inventory(spark, batch_id)
print(f"Inventory Bronze pipeline complete | records={count}")
