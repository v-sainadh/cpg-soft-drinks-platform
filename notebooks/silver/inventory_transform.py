# Databricks notebook source

# COMMAND ----------
"""
Silver pipeline for Inventory domain — Stock snapshots and Reorder points.

Reads from:  brz_freshsip.erp_inventory_raw
Writes to:   slv_freshsip.inventory_stock      (Delta MERGE upsert)
             slv_freshsip.ref_reorder_points   (overwrite)
Schedule:    Daily
Depends on:  bronze/inventory_ingestion, bronze/master_data_ingestion

Author: Data Engineer Agent
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType, TimestampType, LongType
from delta.tables import DeltaTable

# COMMAND ----------
def cast_inventory_columns(df: DataFrame) -> DataFrame:
    """Cast Bronze string columns to Silver typed schema for inventory stock."""
    return (
        df
        .withColumn("units_on_hand",          F.col("units_on_hand").cast(IntegerType()))
        .withColumn("units_in_transit",        F.col("units_in_transit").cast(IntegerType()))
        .withColumn("units_reserved",          F.col("units_reserved").cast(IntegerType()))
        .withColumn("reorder_point_units",     F.col("reorder_point_units").cast(IntegerType()))
        .withColumn("standard_cost_per_unit",  F.col("standard_cost_per_unit").cast(DecimalType(10, 2)))
        .withColumn("snapshot_date",           F.to_date(F.col("snapshot_date")))
        .withColumn("snapshot_timestamp",      F.to_timestamp(F.col("snapshot_timestamp")))
        .withColumn("created_at",              F.current_timestamp())
        .withColumn("updated_at",              F.current_timestamp())
        .withColumn("_source_batch_id",        F.col("_batch_id"))
    )

# COMMAND ----------
def compute_inventory_value(df: DataFrame) -> DataFrame:
    """Compute inventory_value = units_on_hand * standard_cost_per_unit."""
    return df.withColumn(
        "inventory_value",
        (F.col("units_on_hand") * F.col("standard_cost_per_unit")).cast(DecimalType(14, 2))
    )

# COMMAND ----------
def compute_days_of_supply(df: DataFrame) -> DataFrame:
    """Estimate days_of_supply per SKU/warehouse snapshot."""
    avg_daily = F.greatest(
        (F.col("units_in_transit") / F.lit(7.0)).cast(DecimalType(10, 2)),
        F.lit(1.0)
    )
    return df.withColumn(
        "dsi_days",
        (F.col("units_on_hand") / avg_daily).cast(DecimalType(8, 1))
    )

# COMMAND ----------
# Main execution
brz_inv   = "brz_freshsip.erp_inventory_raw"
slv_stock = "slv_freshsip.inventory_stock"
slv_rop   = "slv_freshsip.ref_reorder_points"

print(f"PIPELINE START | inventory_silver | source={brz_inv} | target={slv_stock}")
spark.sql("CREATE DATABASE IF NOT EXISTS slv_freshsip")

df_brz = spark.read.table(brz_inv)

# Deduplicate
from pyspark.sql import Window
df_deduped = (
    df_brz
    .withColumn("_rn",
                F.row_number().over(
                    Window.partitionBy("warehouse_id", "sku_id", "snapshot_date")
                    .orderBy(F.col("_ingested_at").desc())
                ))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

df_typed  = cast_inventory_columns(df_deduped)
df_valued = compute_inventory_value(df_typed)
df_final  = compute_days_of_supply(df_valued)

# Surrogate key
df_final = df_final.withColumn(
    "stock_key",
    F.abs(F.hash(F.col("warehouse_id"), F.col("sku_id"),
                 F.col("snapshot_date").cast("string"))).cast(LongType())
)

# Upsert to inventory_stock
try:
    dt = DeltaTable.forName(spark, slv_stock)
    (dt.alias("t").merge(df_final.alias("s"), "t.stock_key = s.stock_key")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
except Exception:
    (df_final.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("snapshot_date")
     .saveAsTable(slv_stock))

# Reorder points (SCD Type 1)
df_rop = (
    df_final
    .select("sku_id", "warehouse_id", "reorder_point_units", "_source_batch_id")
    .withColumn("safety_stock_units",
                (F.col("reorder_point_units") * F.lit(0.2)).cast(IntegerType()))
    .withColumn("last_updated_at", F.current_timestamp())
    .dropDuplicates(["sku_id", "warehouse_id"])
)
(df_rop.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(slv_rop))

count = df_final.count()
print(f"PIPELINE END | inventory_silver | records_written={count}")
