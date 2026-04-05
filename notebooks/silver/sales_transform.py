# Databricks notebook source

# COMMAND ----------
"""
Silver pipeline for Sales domain — Transactions.

Reads from:  brz_freshsip.pos_transactions_raw
Writes to:   slv_freshsip.sales_transactions  (Delta MERGE upsert)
Schedule:    Daily
Depends on:  bronze/sales_ingestion, bronze/master_data_ingestion

Author: Data Engineer Agent
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType, TimestampType, LongType
from delta.tables import DeltaTable

# COMMAND ----------
def cast_and_validate_sales_transactions(df: DataFrame) -> DataFrame:
    """Cast Bronze string columns to Silver typed columns for sales transactions."""
    return (
        df
        .withColumn("quantity_sold",         F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",            F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("transaction_timestamp", F.to_timestamp(F.col("transaction_timestamp")))
        .withColumn("transaction_date",      F.to_date(F.col("transaction_timestamp")))
        .withColumn("net_line_amount",
                    (F.col("unit_price") * F.col("quantity_sold")).cast(DecimalType(12, 2)))
        .withColumn("transaction_key",
                    F.abs(F.hash(F.col("transaction_id"), F.col("sku_id"))).cast(LongType()))
        .withColumn("created_at",            F.current_timestamp())
        .withColumn("updated_at",            F.current_timestamp())
        .withColumn("_source_batch_id",      F.col("_batch_id"))
    )

# COMMAND ----------
def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """Remove duplicate transaction records, keeping the most recent by _ingested_at."""
    from pyspark.sql import Window
    w = Window.partitionBy("transaction_id", "sku_id").orderBy(F.col("_ingested_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

# COMMAND ----------
def _upsert_to_silver(spark, df, target_table, merge_key):
    """Upsert a DataFrame into a Silver Delta table using MERGE."""
    try:
        delta_tbl = DeltaTable.forName(spark, target_table)
        (
            delta_tbl.alias("t")
            .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
            .whenMatchedUpdate(set={"updated_at": "s.updated_at",
                                    "net_line_amount": "s.net_line_amount",
                                    "unit_price": "s.unit_price",
                                    "quantity_sold": "s.quantity_sold"})
            .whenNotMatchedInsertAll()
            .execute()
        )
    except Exception:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("transaction_date")
            .saveAsTable(target_table)
        )

# COMMAND ----------
# Main execution
brz_pos = "brz_freshsip.pos_transactions_raw"
slv_txn = "slv_freshsip.sales_transactions"

print(f"PIPELINE START | sales_silver | source={brz_pos} | target={slv_txn}")
spark.sql("CREATE DATABASE IF NOT EXISTS slv_freshsip")

df_brz   = spark.read.table(brz_pos)
df_typed = cast_and_validate_sales_transactions(df_brz)
df_dedup = deduplicate_transactions(df_typed)
df_final = df_dedup.withColumn(
    "net_line_amount",
    (F.col("unit_price") * F.col("quantity_sold")).cast(DecimalType(12, 2))
)

_upsert_to_silver(spark, df_final, slv_txn, "transaction_key")

count = df_final.count()
print(f"PIPELINE END | sales_silver | records_written={count}")
