"""
Gold KPI pipeline — Order Fulfillment Rate and On-Time Delivery %.

KPI #4 from project-context.md: Order Fulfillment Rate
KPI #8 from project-context.md: On-Time Delivery Percentage
Grain: report_date × channel × region

Formulas:
  on_time_delivery_pct  = SUM(on_time_flag) / COUNT(*) * 100
  fulfillment_rate_pct  = SUM(is_fully_shipped) / COUNT(*) * 100
  avg_logistics_cost    = AVG(logistics_cost_usd) per shipment

Reads from:  gld_freshsip.fact_shipment
             slv_freshsip.customers  (for channel/region by retailer)
Writes to:   gld_freshsip.kpi_fulfillment_rate  (overwrite by report_date partition)
Schedule:    Daily
Depends on:  gold/fact_shipment.py

Author: Data Engineer Agent
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="kpi_fulfillment")


def compute_kpi_fulfillment_rate(
    df_ship: DataFrame,
    df_customers: DataFrame,
) -> DataFrame:
    """
    Compute fulfillment and on-time delivery KPIs grouped by channel and region.

    Args:
        df_ship:      Gold fact_shipment DataFrame.
        df_customers: Silver customers DataFrame (current records).

    Returns:
        KPI DataFrame with one row per (report_date, channel, region).
    """
    df_cust = (
        df_customers
        .filter(F.col("is_current") == True)
        .select(
            F.col("surrogate_key").alias("customer_key"),
            F.col("channel"),
            F.col("region"),
        )
    )

    return (
        df_ship
        .join(df_cust, on="customer_key", how="left")
        .groupBy("ship_date", "channel", "region")
        .agg(
            F.count("*").alias("total_shipments"),
            F.sum(F.col("on_time_flag").cast("int")).alias("on_time_count"),
            F.sum(F.col("is_fully_shipped").cast("int")).alias("fully_shipped_count"),
            F.sum("cases_delivered").alias("total_cases_delivered"),
            F.sum("logistics_cost_usd").cast(DecimalType(14, 2)).alias("total_logistics_cost"),
            F.avg("logistics_cost_usd").cast(DecimalType(10, 2)).alias("avg_logistics_cost_per_shipment"),
        )
        .withColumn(
            "on_time_delivery_pct",
            F.when(F.col("total_shipments") > 0,
                   (F.col("on_time_count").cast(DecimalType(10, 2))
                    / F.col("total_shipments") * 100).cast(DecimalType(6, 2)))
            .otherwise(F.lit(0).cast(DecimalType(6, 2)))
        )
        .withColumn(
            "fulfillment_rate_pct",
            F.when(F.col("total_shipments") > 0,
                   (F.col("fully_shipped_count").cast(DecimalType(10, 2))
                    / F.col("total_shipments") * 100).cast(DecimalType(6, 2)))
            .otherwise(F.lit(0).cast(DecimalType(6, 2)))
        )
        .withColumnRenamed("ship_date", "report_date")
    )


def run_pipeline(spark: SparkSession) -> None:
    """
    Orchestrate the Fulfillment Rate KPI pipeline.

    Args:
        spark: Active SparkSession.
    """
    config   = load_config()
    gld_ship = get_table_config(config, "gold",   "fact_shipment")
    slv_cust = get_table_config(config, "silver", "customers")
    target   = get_table_config(config, "gold",   "kpi_fulfillment")

    log_pipeline_start(logger, "kpi_fulfillment_rate", gld_ship, target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df_ship     = spark.read.table(gld_ship)
    df_customers = spark.read.table(slv_cust)

    df_kpi = compute_kpi_fulfillment_rate(df_ship, df_customers)

    (
        df_kpi.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("report_date")
        .saveAsTable(target)
    )

    count = df_kpi.count()
    log_pipeline_end(logger, "kpi_fulfillment_rate", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_KPI_FulfillmentRate").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
