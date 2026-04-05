# Databricks notebook source

"""
Gold pipeline — Fact Production Batch.

Reads from:  slv_freshsip.production_batches
             slv_freshsip.production_events
             gld_freshsip.dim_date
Writes to:   gld_freshsip.fact_production_batch  (overwrite by batch_date partition)
Schedule:    Daily
Depends on:  silver/production_transform.py, gold/dim_date.py

KPIs enabled: Production Yield Rate, QC Pass Rate, Downtime Hours per Batch

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, IntegerType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="production")

# COMMAND ----------


def compute_fact_production_batch(
    df_batches: DataFrame,
    df_events: DataFrame,
    df_dim_date: DataFrame,
) -> DataFrame:
    """
    Build the fact_production_batch table.

    Aggregates total downtime hours per batch from production_events,
    then joins with production_batches for the final fact record.

    Args:
        df_batches:  Silver production_batches DataFrame.
        df_events:   Silver production_events DataFrame (for downtime aggregation).
        df_dim_date: Gold dim_date DataFrame.

    Returns:
        Fact production batch DataFrame ready for Gold write.
    """
    df_dates = df_dim_date.select(
        F.col("date_key").cast(IntegerType()),
        F.col("full_date")
    )

    df_downtime = (
        df_events
        .filter(F.col("event_type").isin(["DOWNTIME_UNPLANNED", "DOWNTIME_PLANNED"]))
        .groupBy("batch_id")
        .agg(
            F.sum("downtime_hours").cast(DecimalType(8, 2)).alias("total_downtime_hours")
        )
    )

    df_prod_keys = df_batches.select(
        F.col("sku_id"),
        F.abs(F.hash(F.col("sku_id"))).cast(LongType()).alias("product_key"),
    ).dropDuplicates(["sku_id"])

    df = (
        df_batches
        .join(df_downtime, on="batch_id", how="left")
        .join(df_dates, df_batches["batch_date"] == df_dates["full_date"], how="left")
        .join(df_prod_keys, on="sku_id", how="left")
        .withColumn(
            "downtime_hours",
            F.coalesce(F.col("total_downtime_hours"), F.lit(0.0)).cast(DecimalType(8, 2))
        )
        .withColumn("warehouse_key", F.lit(None).cast(LongType()))
        .select(
            "batch_key",
            "date_key",
            "product_key",
            "warehouse_key",
            "batch_id",
            "production_line_id",
            F.col("expected_output_cases").cast(IntegerType()).alias("expected_output_units"),
            F.col("actual_output_cases").cast(IntegerType()).alias("actual_output_units"),
            "yield_rate_pct",
            "qc_pass_flag",
            "qc_status",
            "downtime_hours",
            "batch_date",
        )
    )

    return df

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Gold Fact Production Batch pipeline.

    Args:
        spark: Active SparkSession.
    """
    config   = load_config()
    slv_bat  = get_table_config(config, "silver", "production_batches")
    slv_evt  = get_table_config(config, "silver", "production_events")
    gld_dim  = get_table_config(config, "gold",   "dim_date")
    target   = get_table_config(config, "gold",   "fact_production")

    log_pipeline_start(logger, "fact_production_gold", slv_bat, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_batches  = spark.read.table(slv_bat)
    df_events   = spark.read.table(slv_evt)
    df_dim_date = spark.read.table(gld_dim)

    df_fact = compute_fact_production_batch(df_batches, df_events, df_dim_date)

    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("batch_date")
        .saveAsTable(target)
    )

    count = df_fact.count()
    log_pipeline_end(logger, "fact_production_gold", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_FactProduction_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
