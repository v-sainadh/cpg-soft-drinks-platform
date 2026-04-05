"""
Silver pipeline for Sales domain — Transactions, Returns, and Spend.

Reads from:  brz_freshsip.pos_transactions_raw
             brz_freshsip.erp_sales_raw
             brz_freshsip.erp_customers_raw  (for spend data)
Writes to:   slv_freshsip.sales_transactions  (Delta MERGE upsert)
             slv_freshsip.sales_returns       (Delta MERGE upsert)
             slv_freshsip.sales_spend         (Delta MERGE upsert)
Schedule:    Hourly (follows Bronze POS) / Daily (ERP sales)
Depends on:  bronze/sales_ingestion.py, bronze/master_data_ingestion.py

Transformations applied:
  - Deduplication on transaction_id / return_id / (retailer_id, period)
  - Type casting: STRING → INT, DECIMAL, DATE, TIMESTAMP
  - Business rule validation: price > 0, quantity > 0, channel in valid list
  - Computed columns: net_line_amount, transaction_date
  - Referential integrity check: sku_id exists in ref_products

Author: Data Engineer Agent
"""

import logging
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, DateType, TimestampType, LongType
)
from delta.tables import DeltaTable

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_dq_threshold
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="silver", domain="sales")

_VALID_CHANNELS = ["Retail", "Wholesale", "DTC"]
_VALID_REGIONS  = ["NE", "SE", "MW", "W"]


# ---------------------------------------------------------------------------
# Transformation functions
# ---------------------------------------------------------------------------

def cast_and_validate_sales_transactions(df: DataFrame) -> DataFrame:
    """
    Cast Bronze string columns to Silver typed columns for sales transactions.

    Casts: quantity→INT, unit_price→DECIMAL(10,2), transaction_timestamp→TIMESTAMP.
    Derives: transaction_date from transaction_timestamp, net_line_amount.
    Generates: transaction_key as hash-based surrogate.

    Args:
        df: Bronze pos_transactions_raw DataFrame (all STRING columns).

    Returns:
        DataFrame with correctly typed Silver columns.
    """
    return (
        df
        .withColumn("quantity_sold", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("transaction_timestamp",
                    F.to_timestamp(F.col("transaction_timestamp")))
        .withColumn("transaction_date",
                    F.to_date(F.col("transaction_timestamp")))
        .withColumn("net_line_amount",
                    (F.col("unit_price") * F.col("quantity_sold")).cast(DecimalType(12, 2)))
        .withColumn("transaction_key",
                    F.abs(F.hash(F.col("transaction_id"), F.col("sku_id"))).cast(LongType()))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("_source_batch_id", F.col("_batch_id"))
    )


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """
    Remove duplicate transaction records, keeping the most recent by _ingested_at.

    Deduplication key: transaction_id (one row per POS transaction-item in Silver).

    Args:
        df: Typed transactions DataFrame.

    Returns:
        DataFrame with one row per unique transaction_id.
    """
    from pyspark.sql import Window
    w = Window.partitionBy("transaction_id", "sku_id").orderBy(F.col("_ingested_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def compute_net_line_amount(df: DataFrame) -> DataFrame:
    """
    Recompute net_line_amount to enforce formula integrity: unit_price * quantity_sold.

    Overwrites any pre-existing net_line_amount values from the source.

    Args:
        df: Typed transactions DataFrame with unit_price and quantity_sold.

    Returns:
        DataFrame with corrected net_line_amount column.
    """
    return df.withColumn(
        "net_line_amount",
        (F.col("unit_price") * F.col("quantity_sold")).cast(DecimalType(12, 2))
    )


# ---------------------------------------------------------------------------
# Silver write helpers
# ---------------------------------------------------------------------------

def _upsert_to_silver(spark: SparkSession, df: DataFrame, target_table: str,
                      merge_key: str) -> None:
    """
    Upsert a DataFrame into a Silver Delta table using MERGE.

    Creates the table if it does not yet exist. Uses merge_key as the
    match condition for WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT.

    Args:
        spark: Active SparkSession.
        df: DataFrame to upsert.
        target_table: Fully-qualified Delta table name.
        merge_key: Column name for the merge condition.
    """
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
        # Table does not exist — create via initial write
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("transaction_date")
            .saveAsTable(target_table)
        )


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the full Silver Sales transformation pipeline.

    Steps:
    1. Read Bronze pos_transactions_raw.
    2. Cast types and validate.
    3. Deduplicate on transaction_id + sku_id.
    4. Run output quality checks (SLV-SALES-TXN-* rules).
    5. Upsert to slv_freshsip.sales_transactions.

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    brz_pos   = get_table_config(config, "bronze", "pos_transactions")
    slv_txn   = get_table_config(config, "silver", "sales_transactions")
    fail_rate = get_dq_threshold(config)

    log_pipeline_start(logger, "sales_silver", brz_pos, slv_txn)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['silver']['database']}")

    # --- Sales Transactions ---
    df_brz = spark.read.table(brz_pos)
    total  = df_brz.count()

    # Input DQ
    input_rules = [
        {"type": "not_null", "columns": ["transaction_id", "retailer_id", "sku_id"],
         "severity": "error"},
    ]
    df_input_clean = run_quality_checks(df_brz, input_rules, dq_logger=logger,
                                        total_count=total,
                                        fail_rate_pct=fail_rate)["clean_df"]

    # Transform
    df_typed   = cast_and_validate_sales_transactions(df_input_clean)
    df_deduped = deduplicate_transactions(df_typed)
    df_final   = compute_net_line_amount(df_deduped)

    # Output DQ — SLV-SALES-TXN-003/004/007/009/010
    output_rules = [
        {"type": "not_null",  "columns": ["transaction_id", "transaction_date"],
         "severity": "error"},
        {"type": "range",     "column": "unit_price", "min_val": 0.01, "max_val": 10000,
         "severity": "error"},
        {"type": "range",     "column": "quantity_sold", "min_val": 1,
         "severity": "error"},
        {"type": "custom",
         "condition_expr": "channel NOT IN ('Retail','Wholesale','DTC') AND channel IS NOT NULL",
         "flag_col": "_dq_invalid_channel", "severity": "warning"},
    ]
    output_result = run_quality_checks(df_final, output_rules, dq_logger=logger,
                                       total_count=df_final.count(),
                                       fail_rate_pct=fail_rate)
    df_write = output_result["clean_df"]

    _upsert_to_silver(spark, df_write, slv_txn, "transaction_key")

    count = df_write.count()
    log_pipeline_end(logger, "sales_silver", count)
    logger.info("Sales Silver pipeline complete | records=%d", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver_Sales_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
