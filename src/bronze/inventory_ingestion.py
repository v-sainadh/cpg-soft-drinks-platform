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
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_source_path
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="bronze", domain="inventory")


def _add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    """
    Add the five standard Bronze metadata columns. Cast all data columns to STRING.

    Args:
        df: Input DataFrame.
        source_path: File path string for lineage.
        batch_id: UUID string for this batch run.

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


def ingest_inventory(spark: SparkSession, config: dict, batch_id: str) -> int:
    """
    Ingest daily inventory snapshot CSV into brz_freshsip.erp_inventory_raw.

    Maps synthetic data fields to the Bronze schema columns:
      - quantity_on_hand   → units_on_hand
      - quantity_on_order  → units_in_transit
      - reorder_point      → reorder_point_units
      - units_reserved     → 0 (not in source; defaulted)
      - standard_cost_per_unit → null (not in source)
      - snapshot_timestamp → snapshot_date + 'T00:00:00'

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this batch run.

    Returns:
        Number of records written.
    """
    source_path = get_source_path(config, "erp", "inventory_daily.csv")
    target_table = get_table_config(config, "bronze", "erp_inventory")

    log_pipeline_start(logger, "inventory_bronze", source_path, target_table)

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    # Rename / derive columns to match Bronze schema
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

    # Select only columns present in Bronze schema
    df_selected = df_mapped.select(
        "warehouse_id",
        "sku_id",
        "units_on_hand",
        "units_in_transit",
        "units_reserved",
        "snapshot_date",
        "snapshot_timestamp",
        "reorder_point_units",
        "standard_cost_per_unit",
    )

    total_count = df_selected.count()

    # DQ checks — BRZ-INV-SNAP-001/002/003/004
    dq_rules = [
        {"type": "not_null", "columns": ["warehouse_id", "sku_id", "units_on_hand", "snapshot_date"],
         "severity": "error"},
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
    log_pipeline_end(logger, "inventory_bronze", count)
    return count


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Bronze Inventory ingestion pipeline.

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    batch_id = str(uuid.uuid4())
    logger.info("Inventory Bronze pipeline starting | batch_id=%s", batch_id)

    count = ingest_inventory(spark, config, batch_id)
    logger.info("Inventory Bronze pipeline complete | records=%d", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze_Inventory_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
