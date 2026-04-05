"""
Gold pipeline — Date Dimension (dim_date).

Generates a complete date dimension for all years in the configured range.
Seeded once; append new years as needed.

Reads from:  (generated — no Silver source)
Writes to:   gld_freshsip.dim_date  (append-only, partitioned by year)
Schedule:    Run once; re-run yearly to extend the range.
Depends on:  None

Columns: date_key (YYYYMMDD integer), full_date, day_name, day_of_week,
         day_of_month, week_of_year, month_number, month_name, quarter,
         year, is_weekend, is_holiday (US federal), fiscal_period,
         fiscal_quarter, fiscal_year

Author: Data Engineer Agent
"""

import logging
import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, DateType, BooleanType, StructType, StructField
)

from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from src.utils.config_loader import load_config, get_table_config

logger = get_logger(__name__, layer="gold", domain="dimensions")

# US Federal holidays (month, day) — observed dates approximated to weekday
_US_FEDERAL_HOLIDAYS = {
    (1, 1), (7, 4), (11, 11), (12, 25),  # Fixed-date holidays
}


def generate_date_dimension(spark: SparkSession, start_year: int, end_year: int) -> DataFrame:
    """
    Generate a complete date dimension DataFrame for all dates in [start_year, end_year].

    Date key format: YYYYMMDD as INTEGER (e.g., 20250401 for 2025-04-01).
    Fiscal year assumed to start October 1 (FreshSip fiscal calendar).

    Args:
        spark: Active SparkSession.
        start_year: First year to include (inclusive).
        end_year: Last year to include (inclusive).

    Returns:
        DataFrame with one row per calendar date and all dimension attributes.
    """
    start_dt   = datetime.date(start_year, 1, 1)
    end_dt     = datetime.date(end_year, 12, 31)
    _EPOCH     = datetime.date(1970, 1, 1)

    # Use timezone-safe day arithmetic (avoids strftime("%s") which is
    # non-standard and produces wrong results on UTC Databricks clusters)
    start_epoch = (start_dt - _EPOCH).days
    end_epoch   = (end_dt   - _EPOCH).days

    # Use Spark sequence to generate all dates via Unix day offset
    df = spark.range(start_epoch, end_epoch + 1).select(
        F.to_date(
            (F.col("id") * 86400).cast("timestamp")
        ).alias("full_date")
    )

    # Day attributes
    df = (
        df
        .withColumn("date_key",
                    (F.year("full_date") * 10000
                     + F.month("full_date") * 100
                     + F.dayofmonth("full_date")).cast(IntegerType()))
        .withColumn("day_name",       F.date_format("full_date", "EEEE"))
        .withColumn("day_of_week",    F.dayofweek("full_date").cast(IntegerType()))
        .withColumn("day_of_month",   F.dayofmonth("full_date").cast(IntegerType()))
        .withColumn("week_of_year",   F.weekofyear("full_date").cast(IntegerType()))
        .withColumn("month_number",   F.month("full_date").cast(IntegerType()))
        .withColumn("month_name",     F.date_format("full_date", "MMMM"))
        .withColumn("quarter",
                    F.ceil(F.month("full_date") / 3).cast(IntegerType()))
        .withColumn("year",           F.year("full_date").cast(IntegerType()))
        .withColumn("is_weekend",
                    F.dayofweek("full_date").isin([1, 7]).cast(BooleanType()))
    )

    # US federal holiday flag (fixed-date holidays only)
    holiday_condition = F.lit(False)
    for (m, d) in _US_FEDERAL_HOLIDAYS:
        holiday_condition = holiday_condition | (
            (F.month("full_date") == m) & (F.dayofmonth("full_date") == d)
        )
    df = df.withColumn("is_holiday", holiday_condition.cast(BooleanType()))

    # Fiscal calendar (FreshSip: Oct–Sep fiscal year)
    # FY starts October 1. Oct-Dec = Q1, Jan-Mar = Q2, Apr-Jun = Q3, Jul-Sep = Q4
    df = df.withColumn(
        "fiscal_year",
        F.when(F.month("full_date") >= 10, F.year("full_date") + 1)
        .otherwise(F.year("full_date")).cast(IntegerType())
    ).withColumn(
        "fiscal_quarter",
        F.when(F.month("full_date").isin([10, 11, 12]), F.lit("FQ1"))
        .when(F.month("full_date").isin([1, 2, 3]),     F.lit("FQ2"))
        .when(F.month("full_date").isin([4, 5, 6]),     F.lit("FQ3"))
        .otherwise(F.lit("FQ4"))
    ).withColumn(
        "fiscal_period",
        F.concat(F.col("fiscal_year").cast(StringType()),
                 F.lit("-"), F.col("fiscal_quarter"))
    )

    return df


def run_pipeline(spark: SparkSession) -> None:
    """
    Generate and write the date dimension to gld_freshsip.dim_date.

    Overwrites the entire table — safe since this is a generated reference table
    with no incremental state.

    Args:
        spark: Active SparkSession.
    """
    config = load_config()
    target = get_table_config(config, "gold", "dim_date")
    start_year = config.get("date_spine", {}).get("start_year", 2023)
    end_year   = config.get("date_spine", {}).get("end_year", 2027)

    log_pipeline_start(logger, "dim_date_gold", "generated", target)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['layers']['gold']['database']}")

    df = generate_date_dimension(spark, start_year, end_year)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("year")
        .saveAsTable(target)
    )

    count = df.count()
    log_pipeline_end(logger, "dim_date_gold", count)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold_DimDate_Pipeline").getOrCreate()
    try:
        run_pipeline(spark)
    except Exception as exc:
        logging.getLogger(__name__).error("Pipeline failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()
