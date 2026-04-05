"""
Silver pipeline for Master Data domain — Products, Customers, Warehouses.

Reads from:  brz_freshsip.erp_products_raw
             brz_freshsip.erp_customers_raw
             brz_freshsip.erp_warehouses_raw
Writes to:   slv_freshsip.ref_products   (SCD Type 1 — overwrite)
             slv_freshsip.customers      (SCD Type 2 — history preserved)
             slv_freshsip.ref_warehouses (SCD Type 1 — overwrite)
Schedule:    Daily
Depends on:  bronze/master_data_ingestion.py

SCD Strategy:
  - Products (ref_products):   SCD Type 1 — reference table, current values only
  - Customers:                  SCD Type 2 — full history with valid_from/valid_to/is_current
  - Warehouses (ref_warehouses): SCD Type 1 — reference table, current values only

Author: Data Engineer Agent
"""

import logging
import hashlib

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, DecimalType, DateType, BooleanType, LongType
)
from delta.tables import DeltaTable

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_dq_threshold
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="silver", domain="master_data")

# SCD Type 2 high-watermark date — used as valid_to for closed records
_SCD_END_DATE = "9999-12-31"


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------

def apply_scd_type2(
    spark: SparkSession,
    df_incoming: DataFrame,
    target_table: str,
    natural_key: str,
    tracked_cols: list,
    valid_from_col: str = "record_effective_date",
) -> None:
    """
    Apply SCD Type 2 logic to a dimensional table.

    For each incoming record:
    - If the natural_key does not exist → INSERT as new current record
    - If it exists AND tracked attributes changed → close old record (valid_to = today),
      INSERT new record with is_current=True
    - If it exists AND nothing changed → no action (MERGE condition not met)

    Args:
        spark: Active SparkSession.
        df_incoming: Incoming DataFrame with new/changed records.
        target_table: Fully-qualified Silver table name.
        natural_key: Business key column name (e.g., 'retailer_id').
        tracked_cols: List of attribute columns that trigger a new version when changed.
        valid_from_col: Column in incoming data to use as valid_from date.
    """
    from pyspark.sql import Window

    # Deduplicate incoming by natural_key (keep latest record_effective_date)
    w = Window.partitionBy(natural_key).orderBy(F.col(valid_from_col).desc())
    df_latest = (
        df_incoming
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Compute a hash of tracked columns to detect changes
    hash_expr = F.md5(
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols])
    )
    df_latest = df_latest.withColumn("_row_hash", hash_expr)

    # Derive surrogate_key
    df_latest = df_latest.withColumn(
        "surrogate_key",
        F.abs(F.hash(F.col(natural_key),
                     F.col(valid_from_col).cast("string"))).cast(LongType())
    )
    df_latest = (
        df_latest
        .withColumn("valid_from",
                    F.to_date(F.col(valid_from_col)))
        .withColumn("valid_to",  F.to_date(F.lit(_SCD_END_DATE)))
        .withColumn("is_current", F.lit(True).cast(BooleanType()))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    try:
        dt = DeltaTable.forName(spark, target_table)

        # Step 1: Close existing current records whose tracked attributes changed.
        # Records with matching hash are left untouched (no new version needed).
        (
            dt.alias("t")
            .merge(
                df_latest.alias("s"),
                f"t.{natural_key} = s.{natural_key} AND t.is_current = true"
            )
            .whenMatchedUpdate(
                condition="t._row_hash != s._row_hash",
                set={
                    "valid_to":   "CURRENT_DATE()",
                    "is_current": "false",
                    "updated_at": "s.updated_at",
                }
            )
            .execute()
        )

        # Step 2: Insert ONLY changed or net-new records.
        # Anti-join: exclude incoming records whose natural_key already has a
        # current row in the target WITH the same hash (i.e., unchanged records).
        df_target_current = (
            spark.read.table(target_table)
            .filter(F.col("is_current") == True)
            .select(natural_key, "_row_hash")
        )
        df_to_insert = (
            df_latest
            .join(
                df_target_current.withColumnRenamed("_row_hash", "_existing_hash"),
                on=natural_key,
                how="left"
            )
            # Insert if: no existing current row (net-new) OR hash differs (changed)
            .filter(
                F.col("_existing_hash").isNull() |
                (F.col("_row_hash") != F.col("_existing_hash"))
            )
            .drop("_row_hash", "_existing_hash")
        )

        if df_to_insert.count() > 0:
            (
                df_to_insert.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target_table)
            )
    except Exception:
        # Initial load — table does not yet exist
        df_out = df_latest.drop("_row_hash")
        (
            df_out.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )


# ---------------------------------------------------------------------------
# Products (SCD Type 1 — ref_products)
# ---------------------------------------------------------------------------

def transform_products(df: DataFrame) -> DataFrame:
    """
    Cast and clean product master data for ref_products Silver table.

    Args:
        df: Bronze erp_products_raw DataFrame.

    Returns:
        Typed DataFrame ready for SCD Type 1 write.
    """
    return (
        df
        .withColumn("package_size_ml",
                    F.col("package_size_ml").cast(IntegerType()))
        .withColumn("standard_cost_per_unit",
                    F.col("standard_cost_per_unit").cast(DecimalType(10, 2)))
        .withColumn("list_price",
                    F.col("list_price").cast(DecimalType(10, 2)))
        .withColumn("is_active",
                    F.col("is_active").isin("true", "True", "TRUE", "1").cast(BooleanType()))
        .withColumn("record_effective_date",
                    F.to_date(F.col("record_effective_date")))
        .withColumn("created_at",  F.current_timestamp())
        .withColumn("updated_at",  F.current_timestamp())
        .withColumn("_source_batch_id", F.col("_batch_id"))
        # Keep latest record per sku_id
        .withColumn("_rn",
                    F.row_number().over(
                        __import__("pyspark.sql", fromlist=["Window"]).Window
                        .partitionBy("sku_id")
                        .orderBy(F.col("record_effective_date").desc())
                    ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Customers (SCD Type 2)
# ---------------------------------------------------------------------------

def transform_customers(df: DataFrame) -> DataFrame:
    """
    Cast and clean customer master data in preparation for SCD Type 2 processing.

    Args:
        df: Bronze erp_customers_raw DataFrame.

    Returns:
        Typed DataFrame for apply_scd_type2().
    """
    return (
        df
        .withColumn("credit_terms_days",
                    F.col("credit_terms_days").cast(IntegerType()))
        .withColumn("account_activation_date",
                    F.to_date(F.col("account_activation_date")))
        .withColumn("record_effective_date",
                    F.to_date(F.col("record_effective_date")))
        .withColumn("trade_spend_usd",
                    F.col("trade_spend_usd").cast(DecimalType(12, 2)))
        .withColumn("broker_commission_usd",
                    F.col("broker_commission_usd").cast(DecimalType(12, 2)))
        .withColumn("field_sales_cost_usd",
                    F.col("field_sales_cost_usd").cast(DecimalType(12, 2)))
        .withColumn("created_at",  F.current_timestamp())
        .withColumn("updated_at",  F.current_timestamp())
        .withColumn("_source_batch_id", F.col("_batch_id"))
    )


# ---------------------------------------------------------------------------
# Warehouses (SCD Type 1 — ref_warehouses)
# ---------------------------------------------------------------------------

def transform_warehouses(df: DataFrame) -> DataFrame:
    """
    Cast and clean warehouse master data for ref_warehouses Silver table.

    Args:
        df: Bronze erp_warehouses_raw DataFrame.

    Returns:
        Typed DataFrame ready for SCD Type 1 write.
    """
    return (
        df
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("_source_batch_id", F.col("_batch_id"))
        .dropDuplicates(["warehouse_id"])
    )


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the full Silver Master Data transformation pipeline.

    Steps:
    1. Process and write ref_products (SCD Type 1).
    2. Process and write customers (SCD Type 2).
    3. Process and write ref_warehouses (SCD Type 1).

    Args:
        spark: Active SparkSession.
    """
    config       = load_config()
    fail_rate    = get_dq_threshold(config)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['silver']['database']}")

    # --- Products (SCD Type 1) ---
    brz_prod  = get_table_config(config, "bronze", "erp_products")
    slv_prod  = get_table_config(config, "silver", "products")

    log_pipeline_start(logger, "products_silver", brz_prod, slv_prod)
    df_prod = spark.read.table(brz_prod)
    total_prod = df_prod.count()

    dq_rules = [{"type": "not_null", "columns": ["sku_id", "product_category"],
                 "severity": "error"}]
    df_prod_clean = run_quality_checks(df_prod, dq_rules, dq_logger=logger,
                                       total_count=total_prod,
                                       fail_rate_pct=fail_rate)["clean_df"]
    df_prod_typed = transform_products(df_prod_clean)
    (df_prod_typed.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(slv_prod))

    prod_count = df_prod_typed.count()
    log_pipeline_end(logger, "products_silver", prod_count)

    # --- Customers (SCD Type 2) ---
    brz_cust  = get_table_config(config, "bronze", "erp_customers")
    slv_cust  = get_table_config(config, "silver", "customers")

    log_pipeline_start(logger, "customers_silver", brz_cust, slv_cust)
    df_cust = spark.read.table(brz_cust)
    total_cust = df_cust.count()

    dq_rules_cust = [{"type": "not_null", "columns": ["retailer_id", "record_effective_date"],
                      "severity": "error"}]
    df_cust_clean = run_quality_checks(df_cust, dq_rules_cust, dq_logger=logger,
                                       total_count=total_cust,
                                       fail_rate_pct=fail_rate)["clean_df"]
    df_cust_typed = transform_customers(df_cust_clean)

    apply_scd_type2(
        spark, df_cust_typed, slv_cust,
        natural_key="retailer_id",
        tracked_cols=["retailer_name", "retail_segment", "channel", "region",
                      "state", "account_status", "credit_terms_days"],
    )

    cust_count = df_cust_typed.count()
    log_pipeline_end(logger, "customers_silver", cust_count)

    # --- Warehouses (SCD Type 1) ---
    brz_wh  = get_table_config(config, "bronze", "erp_warehouses")
    slv_wh  = get_table_config(config, "silver", "warehouses")

    log_pipeline_start(logger, "warehouses_silver", brz_wh, slv_wh)
    df_wh = spark.read.table(brz_wh)
    total_wh = df_wh.count()

    dq_rules_wh = [{"type": "not_null", "columns": ["warehouse_id"], "severity": "error"}]
    df_wh_clean = run_quality_checks(df_wh, dq_rules_wh, dq_logger=logger,
                                     total_count=total_wh, fail_rate_pct=fail_rate)["clean_df"]
    df_wh_typed = transform_warehouses(df_wh_clean)
    (df_wh_typed.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(slv_wh))

    wh_count = df_wh_typed.count()
    log_pipeline_end(logger, "warehouses_silver", wh_count)

    logger.info(
        "Master Data Silver complete | products=%d | customers=%d | warehouses=%d",
        prod_count, cust_count, wh_count,
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver_MasterData_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
