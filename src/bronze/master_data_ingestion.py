"""
Bronze pipeline for Master Data domain — Products, Customers, Warehouses.

Reads from: data/synthetic/erp/products.csv
            data/synthetic/erp/customers.csv
            data/synthetic/erp/warehouses.csv
Writes to:  brz_freshsip.erp_products_raw    (append-only, SCD Type 2 applied in Silver)
            brz_freshsip.erp_customers_raw   (append-only, SCD Type 2 applied in Silver)
            brz_freshsip.erp_warehouses_raw  (append-only)
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

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_source_path
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="bronze", domain="master_data")


def _add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    """
    Add five standard Bronze metadata columns. Cast all data columns to STRING.

    Args:
        df: Input DataFrame.
        source_path: Source file path for lineage.
        batch_id: UUID string for this ingestion run.

    Returns:
        DataFrame with STRING data columns and 5 metadata columns.
    """
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


def ingest_products(spark: SparkSession, config: dict, batch_id: str) -> int:
    """
    Ingest products CSV into brz_freshsip.erp_products_raw.

    Maps synthetic field names to Bronze schema:
      - category       → product_category
      - sub_category   → product_subcategory
      - unit_price     → list_price
      - cost_price     → standard_cost_per_unit
      - pack_size      → package_size_ml
      - status         → is_active
      - launch_date    → record_effective_date

    Fields not in source (brand, packaging_type, price_tier) default to empty string.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this ingestion run.

    Returns:
        Number of records written.
    """
    source_path  = get_source_path(config, "erp", "products.csv")
    target_table = get_table_config(config, "bronze", "erp_products")

    log_pipeline_start(logger, "products_bronze", source_path, target_table)

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
        # Defaults for columns not in source
        .withColumn("brand", F.lit(""))
        .withColumn("packaging_type", F.lit(""))
        .withColumn("price_tier", F.lit(""))
    )

    df_selected = df_mapped.select(
        "sku_id", "product_name", "product_category", "product_subcategory",
        "brand", "packaging_type", "package_size_ml", "standard_cost_per_unit",
        "list_price", "price_tier", "is_active", "record_effective_date",
    )

    total_count = df_selected.count()

    # DQ checks — BRZ-PROD-ERP-001/002/003/004
    dq_rules = [
        {"type": "not_null", "columns": ["sku_id", "product_category"], "severity": "error"},
        {"type": "not_null", "columns": ["standard_cost_per_unit"], "severity": "error"},
    ]
    result = run_quality_checks(df_selected, dq_rules, dq_logger=logger,
                                total_count=total_count)
    df_clean = result["clean_df"]

    df_final = _add_bronze_metadata(df_clean, source_path, batch_id)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['bronze']['database']}")
    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    log_pipeline_end(logger, "products_bronze", count)
    return count


def ingest_customers(spark: SparkSession, config: dict, batch_id: str) -> int:
    """
    Ingest customers CSV into brz_freshsip.erp_customers_raw.

    Maps synthetic field names to Bronze schema:
      - customer_id       → retailer_id
      - name              → retailer_name
      - segment           → retail_segment
      - acquisition_date  → account_activation_date (and record_effective_date)
      - status            → account_status

    Spend fields (trade_spend_usd etc.) default to "0" as not present in source.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this ingestion run.

    Returns:
        Number of records written.
    """
    source_path  = get_source_path(config, "erp", "customers.csv")
    target_table = get_table_config(config, "bronze", "erp_customers")

    log_pipeline_start(logger, "customers_bronze", source_path, target_table)

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
        "region", "state", "city", "credit_terms_days",
        "account_activation_date", "account_status",
        "trade_spend_usd", "broker_commission_usd", "field_sales_cost_usd",
        "record_effective_date",
    )

    total_count = df_selected.count()

    # DQ checks — BRZ-CUST-ERP-001/005
    dq_rules = [
        {"type": "not_null", "columns": ["retailer_id"], "severity": "error"},
        {"type": "not_null", "columns": ["retailer_name", "retail_segment"], "severity": "warning"},
    ]
    result = run_quality_checks(df_selected, dq_rules, dq_logger=logger,
                                total_count=total_count)
    df_clean = result["clean_df"]

    df_final = _add_bronze_metadata(df_clean, source_path, batch_id)

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    log_pipeline_end(logger, "customers_bronze", count)
    return count


def ingest_warehouses(spark: SparkSession, config: dict, batch_id: str) -> int:
    """
    Ingest warehouses CSV into brz_freshsip.erp_warehouses_raw.

    Writes warehouse reference data as-is with metadata columns.
    This is a reference table that rarely changes; SCD Type 1 in Silver.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this ingestion run.

    Returns:
        Number of records written.
    """
    source_path  = get_source_path(config, "erp", "warehouses.csv")
    target_table = get_table_config(config, "bronze", "erp_warehouses")

    log_pipeline_start(logger, "warehouses_bronze", source_path, target_table)

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    total_count = df_raw.count()

    dq_rules = [
        {"type": "not_null", "columns": ["warehouse_id"], "severity": "error"},
    ]
    result = run_quality_checks(df_raw, dq_rules, dq_logger=logger, total_count=total_count)
    df_clean = result["clean_df"]

    df_final = _add_bronze_metadata(df_clean, source_path, batch_id)

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    log_pipeline_end(logger, "warehouses_bronze", count)
    return count


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Bronze Master Data ingestion pipeline.

    Ingests products, customers, and warehouses in sequence.

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    batch_id = str(uuid.uuid4())
    logger.info("Master Data Bronze pipeline starting | batch_id=%s", batch_id)

    prod_count = ingest_products(spark, config, batch_id)
    cust_count = ingest_customers(spark, config, batch_id)
    wh_count   = ingest_warehouses(spark, config, batch_id)

    logger.info(
        "Master Data Bronze pipeline complete | products=%d | customers=%d | warehouses=%d",
        prod_count, cust_count, wh_count,
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze_MasterData_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
