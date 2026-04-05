# Databricks notebook source

# COMMAND ----------
"""
Silver pipeline for Master Data domain — Products, Customers, Warehouses.

Reads from:  brz_freshsip.erp_products_raw
             brz_freshsip.erp_customers_raw
             brz_freshsip.erp_warehouses_raw
Writes to:   slv_freshsip.ref_products   (SCD Type 1)
             slv_freshsip.customers      (SCD Type 2)
             slv_freshsip.ref_warehouses (SCD Type 1)
Schedule:    Daily
Depends on:  bronze/master_data_ingestion

Author: Data Engineer Agent
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType, BooleanType, LongType
from delta.tables import DeltaTable

# COMMAND ----------
_SCD_END_DATE = "9999-12-31"

# COMMAND ----------
def apply_scd_type2(spark, df_incoming, target_table, natural_key, tracked_cols,
                    valid_from_col="record_effective_date"):
    """Apply SCD Type 2 logic to a dimensional table."""
    from pyspark.sql import Window

    w = Window.partitionBy(natural_key).orderBy(F.col(valid_from_col).desc())
    df_latest = (
        df_incoming
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    hash_expr = F.md5(
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols])
    )
    df_latest = df_latest.withColumn("_row_hash", hash_expr)
    df_latest = df_latest.withColumn(
        "surrogate_key",
        F.abs(F.hash(F.col(natural_key), F.col(valid_from_col).cast("string"))).cast(LongType())
    )
    df_latest = (
        df_latest
        .withColumn("valid_from", F.to_date(F.col(valid_from_col)))
        .withColumn("valid_to",   F.to_date(F.lit(_SCD_END_DATE)))
        .withColumn("is_current", F.lit(True).cast(BooleanType()))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    try:
        dt = DeltaTable.forName(spark, target_table)
        (
            dt.alias("t")
            .merge(df_latest.alias("s"),
                   f"t.{natural_key} = s.{natural_key} AND t.is_current = true")
            .whenMatchedUpdate(
                condition="t._row_hash != s._row_hash",
                set={"valid_to": "CURRENT_DATE()", "is_current": "false", "updated_at": "s.updated_at"}
            )
            .execute()
        )

        df_target_current = (
            spark.read.table(target_table)
            .filter(F.col("is_current") == True)
            .select(natural_key, "_row_hash")
        )
        df_to_insert = (
            df_latest
            .join(df_target_current.withColumnRenamed("_row_hash", "_existing_hash"),
                  on=natural_key, how="left")
            .filter(F.col("_existing_hash").isNull() | (F.col("_row_hash") != F.col("_existing_hash")))
            .drop("_row_hash", "_existing_hash")
        )
        if df_to_insert.count() > 0:
            (df_to_insert.write.format("delta").mode("append")
             .option("mergeSchema", "true").saveAsTable(target_table))
    except Exception:
        df_out = df_latest.drop("_row_hash")
        (df_out.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(target_table))

# COMMAND ----------
def transform_products(df: DataFrame) -> DataFrame:
    """Cast and clean product master data for ref_products Silver table."""
    from pyspark.sql import Window
    return (
        df
        .withColumn("package_size_ml",          F.col("package_size_ml").cast(IntegerType()))
        .withColumn("standard_cost_per_unit",    F.col("standard_cost_per_unit").cast(DecimalType(10, 2)))
        .withColumn("list_price",                F.col("list_price").cast(DecimalType(10, 2)))
        .withColumn("is_active",
                    F.col("is_active").isin("true", "True", "TRUE", "1", "active").cast(BooleanType()))
        .withColumn("record_effective_date",     F.to_date(F.col("record_effective_date")))
        .withColumn("created_at",               F.current_timestamp())
        .withColumn("updated_at",               F.current_timestamp())
        .withColumn("_source_batch_id",         F.col("_batch_id"))
        .withColumn("_rn",
                    F.row_number().over(
                        Window.partitionBy("sku_id").orderBy(F.col("record_effective_date").desc())
                    ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

# COMMAND ----------
def transform_customers(df: DataFrame) -> DataFrame:
    """Cast and clean customer master data for SCD Type 2 processing."""
    return (
        df
        .withColumn("credit_terms_days",        F.col("credit_terms_days").cast(IntegerType()))
        .withColumn("account_activation_date",  F.to_date(F.col("account_activation_date")))
        .withColumn("record_effective_date",    F.to_date(F.col("record_effective_date")))
        .withColumn("trade_spend_usd",          F.col("trade_spend_usd").cast(DecimalType(12, 2)))
        .withColumn("broker_commission_usd",    F.col("broker_commission_usd").cast(DecimalType(12, 2)))
        .withColumn("field_sales_cost_usd",     F.col("field_sales_cost_usd").cast(DecimalType(12, 2)))
        .withColumn("created_at",               F.current_timestamp())
        .withColumn("updated_at",               F.current_timestamp())
        .withColumn("_source_batch_id",         F.col("_batch_id"))
    )

# COMMAND ----------
def transform_warehouses(df: DataFrame) -> DataFrame:
    """Cast and clean warehouse master data for ref_warehouses Silver table."""
    return (
        df
        .withColumn("created_at",       F.current_timestamp())
        .withColumn("updated_at",       F.current_timestamp())
        .withColumn("_source_batch_id", F.col("_batch_id"))
        .dropDuplicates(["warehouse_id"])
    )

# COMMAND ----------
# Main execution
spark.sql("CREATE DATABASE IF NOT EXISTS slv_freshsip")

# Products (SCD Type 1)
brz_prod = "brz_freshsip.erp_products_raw"
slv_prod = "slv_freshsip.ref_products"

print(f"PIPELINE START | products_silver | source={brz_prod} | target={slv_prod}")
df_prod = spark.read.table(brz_prod)
df_prod_typed = transform_products(df_prod)
(df_prod_typed.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(slv_prod))
prod_count = df_prod_typed.count()
print(f"PIPELINE END | products_silver | records_written={prod_count}")

# COMMAND ----------
# Customers (SCD Type 2)
brz_cust = "brz_freshsip.erp_customers_raw"
slv_cust = "slv_freshsip.customers"

print(f"PIPELINE START | customers_silver | source={brz_cust} | target={slv_cust}")
df_cust = spark.read.table(brz_cust)
df_cust_typed = transform_customers(df_cust)
apply_scd_type2(
    spark, df_cust_typed, slv_cust,
    natural_key="retailer_id",
    tracked_cols=["retailer_name", "retail_segment", "channel", "region",
                  "account_status", "credit_terms_days"],
)
cust_count = df_cust_typed.count()
print(f"PIPELINE END | customers_silver | records_written={cust_count}")

# COMMAND ----------
# Warehouses (SCD Type 1)
brz_wh = "brz_freshsip.erp_warehouses_raw"
slv_wh = "slv_freshsip.ref_warehouses"

print(f"PIPELINE START | warehouses_silver | source={brz_wh} | target={slv_wh}")
df_wh = spark.read.table(brz_wh)
df_wh_typed = transform_warehouses(df_wh)
(df_wh_typed.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(slv_wh))
wh_count = df_wh_typed.count()
print(f"PIPELINE END | warehouses_silver | records_written={wh_count}")

print(f"Master Data Silver complete | products={prod_count} | customers={cust_count} | warehouses={wh_count}")
