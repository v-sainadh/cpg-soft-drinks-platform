# Databricks notebook source

"""
Gold KPI pipeline — Production Yield Rate by Batch, Line, and Period.

KPI #3 from project-context.md: Production Yield Rate
Grain: batch_date × production_line_id  (also aggregated by week/month)

Formula:
  yield_rate_pct       = actual_output_units / expected_output_units * 100  (per batch)
  avg_yield_rate       = AVG(yield_rate_pct) per line per period
  qc_pass_rate         = SUM(qc_pass_flag) / COUNT(*) * 100

Reads from:  gld_freshsip.fact_production_batch
Writes to:   gld_freshsip.kpi_production_yield  (overwrite by report_date partition)
Schedule:    Daily
Depends on:  gold/fact_production_batch.py

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="kpi_production")

# COMMAND ----------


def compute_kpi_production_yield(df_fact: DataFrame) -> DataFrame:
    """
    Compute production yield rate aggregated by production_line and report_date.

    Args:
        df_fact: Gold fact_production_batch DataFrame.

    Returns:
        KPI DataFrame with one row per (report_date, production_line_id).
    """
    return (
        df_fact
        .groupBy("batch_date", "production_line_id")
        .agg(
            F.count("*").alias("batch_count"),
            F.avg("yield_rate_pct").cast(DecimalType(6, 2)).alias("avg_yield_rate_pct"),
            F.min("yield_rate_pct").cast(DecimalType(6, 2)).alias("min_yield_rate_pct"),
            F.max("yield_rate_pct").cast(DecimalType(6, 2)).alias("max_yield_rate_pct"),
            F.sum("expected_output_units").alias("total_expected_units"),
            F.sum("actual_output_units").alias("total_actual_units"),
            F.sum("downtime_hours").cast(DecimalType(10, 2)).alias("total_downtime_hours"),
            (F.sum(F.col("qc_pass_flag").cast("int"))
             / F.count("*") * 100).cast(DecimalType(6, 2)).alias("qc_pass_rate_pct"),
        )
        .withColumn(
            "overall_yield_pct",
            F.when(F.col("total_expected_units") > 0,
                   (F.col("total_actual_units").cast(DecimalType(14, 2))
                    / F.col("total_expected_units").cast(DecimalType(14, 2))
                    * 100).cast(DecimalType(6, 2)))
            .otherwise(F.lit(None).cast(DecimalType(6, 2)))
        )
        .withColumnRenamed("batch_date", "report_date")
    )

# COMMAND ----------


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Production Yield KPI pipeline.

    Args:
        spark: Active SparkSession.
    """
    config   = load_config()
    gld_prod = get_table_config(config, "gold", "fact_production")
    target   = get_table_config(config, "gold", "kpi_prod_yield")

    log_pipeline_start(logger, "kpi_production_yield", gld_prod, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_fact = spark.read.table(gld_prod)
    df_kpi  = compute_kpi_production_yield(df_fact)

    (
        df_kpi.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("report_date")
        .saveAsTable(target)
    )

    count = df_kpi.count()
    log_pipeline_end(logger, "kpi_production_yield", count)

# COMMAND ----------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_KPI_ProductionYield").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
