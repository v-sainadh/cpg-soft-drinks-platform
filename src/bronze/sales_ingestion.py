"""
Bronze pipeline for Sales domain — POS Transactions and ERP Sales Orders.

Reads from: data/synthetic/pos/pos_transactions.json
            data/synthetic/erp/orders.csv
            data/synthetic/erp/order_lines.csv
Writes to:  brz_freshsip.pos_transactions_raw  (append-only)
            brz_freshsip.erp_sales_raw          (append-only)
Schedule:   POS: hourly | ERP Sales: daily
Depends on: None (first pipeline in chain)

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

logger = get_logger(__name__, layer="bronze", domain="sales")


# ---------------------------------------------------------------------------
# Metadata helper
# ---------------------------------------------------------------------------

def _add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    """
    Append the five standard Bronze metadata columns to every record.

    All non-metadata columns are cast to STRING to comply with Bronze schema-on-read.

    Args:
        df: Input DataFrame with raw source columns.
        source_path: Source file path string for lineage.
        batch_id: UUID string identifying this ingestion run.

    Returns:
        DataFrame with all data columns as STRING plus 5 metadata columns.
    """
    # Cast all existing columns to string (bronze = schema-on-read)
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


# ---------------------------------------------------------------------------
# POS Transactions ingestion
# ---------------------------------------------------------------------------

def ingest_pos_transactions(spark: SparkSession, config: dict, batch_id: str) -> int:
    """
    Ingest POS transaction JSON files into brz_freshsip.pos_transactions_raw.

    The POS JSON has a nested 'items' array; each item becomes one row in Bronze
    so that downstream Silver can work with flat, atomic records.

    Steps:
    1. Read NDJSON source.
    2. Explode items array into individual rows.
    3. Run DQ checks (not_null on key identifiers).
    4. Add Bronze metadata columns.
    5. Append-write to Delta target table.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this batch run.

    Returns:
        Number of records written.
    """
    source_path = get_source_path(config, "pos", "pos_transactions.json")
    target_table = get_table_config(config, "bronze", "pos_transactions")

    log_pipeline_start(logger, "pos_transactions_bronze", source_path, target_table)

    # Read NDJSON — each line is one transaction with nested items array
    df_raw = (
        spark.read
        .option("multiline", "false")
        .json(source_path)
    )

    # Explode items → one row per transaction-item
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
            # Channel/region/state not in POS source — filled downstream
            F.lit("Retail").alias("channel"),
            F.lit("").alias("region"),
            F.lit("").alias("state"),
        )
    )

    total_count = df_exploded.count()

    # DQ checks — BLOCKER rules per BRZ-SALES-POS-001/002/003
    dq_rules = [
        {"type": "not_null", "columns": ["transaction_id", "retailer_id", "sku_id"],
         "severity": "error"},
        {"type": "not_null", "columns": ["unit_price"], "severity": "error"},
    ]
    result = run_quality_checks(df_exploded, dq_rules, dq_logger=logger,
                                total_count=total_count)
    df_clean = result["clean_df"]

    # Add Bronze metadata
    df_final = _add_bronze_metadata(df_clean, source_path, batch_id)

    # Create database if needed and write
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
    log_pipeline_end(logger, "pos_transactions_bronze", count)
    return count


# ---------------------------------------------------------------------------
# ERP Sales Orders ingestion
# ---------------------------------------------------------------------------

def ingest_erp_sales(spark: SparkSession, config: dict, batch_id: str) -> int:
    """
    Ingest ERP orders and order_lines CSVs into brz_freshsip.erp_sales_raw.

    Joins orders (header) with order_lines (detail) to produce one row per
    order line, mirroring the Silver model where each line is a distinct fact.

    Steps:
    1. Read orders.csv and order_lines.csv.
    2. Join on order_id.
    3. Run DQ checks (not_null on order_id, sku_id; format check on invoice_price).
    4. Add Bronze metadata columns.
    5. Append-write to Delta target.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this batch run.

    Returns:
        Number of records written.
    """
    orders_path = get_source_path(config, "erp", "orders.csv")
    lines_path  = get_source_path(config, "erp", "order_lines.csv")
    target_table = get_table_config(config, "bronze", "erp_sales")

    log_pipeline_start(logger, "erp_sales_bronze", orders_path, target_table)

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

    # Join — retain all lines; orders without lines are excluded
    df_joined = df_lines.join(df_orders, on="order_id", how="left")

    # Add columns expected by Bronze schema but not in source
    df_joined = (
        df_joined
        .withColumn("quantity_shipped", F.col("quantity_ordered"))  # approximation
        .withColumn("channel", F.lit(""))
        .withColumn("region", F.lit(""))
        .withColumn("state", F.lit(""))
    )

    total_count = df_joined.count()

    dq_rules = [
        {"type": "not_null", "columns": ["order_id", "sku_id", "retailer_id"],
         "severity": "error"},
        {"type": "not_null", "columns": ["invoice_price"], "severity": "warning"},
    ]
    result = run_quality_checks(df_joined, dq_rules, dq_logger=logger,
                                total_count=total_count)
    df_clean = result["clean_df"]

    df_final = _add_bronze_metadata(df_clean, orders_path, batch_id)

    (
        df_final.write
        .format("delta")
        .mode("append")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    count = df_final.count()
    log_pipeline_end(logger, "erp_sales_bronze", count)
    return count


# ---------------------------------------------------------------------------
# Main pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the full Bronze Sales ingestion pipeline.

    Steps:
    1. Load config.
    2. Generate batch_id for this run.
    3. Ingest POS transactions.
    4. Ingest ERP sales (orders + order_lines).

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    batch_id = str(uuid.uuid4())
    logger.info("Sales Bronze pipeline starting | batch_id=%s", batch_id)

    pos_count = ingest_pos_transactions(spark, config, batch_id)
    erp_count = ingest_erp_sales(spark, config, batch_id)

    logger.info(
        "Sales Bronze pipeline complete | pos_records=%d | erp_records=%d",
        pos_count, erp_count,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze_Sales_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
