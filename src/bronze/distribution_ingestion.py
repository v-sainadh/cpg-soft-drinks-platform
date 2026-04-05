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
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config, get_source_path
from src.utils.quality_checks import run_quality_checks

logger = get_logger(__name__, layer="bronze", domain="distribution")


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


def ingest_shipments(spark: SparkSession, config: dict, batch_id: str) -> int:
    """
    Ingest logistics shipments CSV into brz_freshsip.logistics_shipments_raw.

    Maps source field names to Bronze schema column names:
      - carrier             → carrier_id
      - estimated_delivery  → promised_delivery_date
      - actual_delivery     → actual_delivery_date
      - cases_shipped       → cases_delivered
      - freight_cost        → logistics_cost_usd

    Fields absent from the source (retailer_id, route_id, channel, region, state,
    quantity_ordered) are defaulted to empty string or 0.

    Args:
        spark: Active SparkSession.
        config: Loaded pipeline config dict.
        batch_id: UUID string for this ingestion run.

    Returns:
        Number of records written.
    """
    source_path  = get_source_path(config, "logistics", "shipments.csv")
    target_table = get_table_config(config, "bronze", "logistics_shipments")

    log_pipeline_start(logger, "distribution_bronze", source_path, target_table)

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
        # Fields not in source — provide defaults
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

    total_count = df_selected.count()

    # DQ checks — BRZ-DIST-SHIP-001/002
    dq_rules = [
        {"type": "not_null", "columns": ["shipment_id", "order_id", "warehouse_id"],
         "severity": "error"},
        {"type": "not_null", "columns": ["promised_delivery_date"], "severity": "error"},
        {"type": "not_null", "columns": ["logistics_cost_usd"], "severity": "warning"},
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
    log_pipeline_end(logger, "distribution_bronze", count)
    return count


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Bronze Distribution ingestion pipeline.

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    batch_id = str(uuid.uuid4())
    logger.info("Distribution Bronze pipeline starting | batch_id=%s", batch_id)

    count = ingest_shipments(spark, config, batch_id)
    logger.info("Distribution Bronze pipeline complete | records=%d", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze_Distribution_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
