# Databricks notebook source

# COMMAND ----------
"""
Bronze pipeline for Production domain — Batches, QC checks, and Downtime events.

Reads from: data/synthetic/production/batches.json
            data/synthetic/production/quality_checks.json
            data/synthetic/production/downtime_events.json
Writes to:  brz_freshsip.iot_sensor_events_raw  (append-only)
Schedule:   Daily
Depends on: None

Author: Data Engineer Agent
"""

import uuid
import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
_CONFIG = {
    "sources": {
        "production": {"base_path": "/Volumes/workspace/default/freshsip_data/synthetic/production"},
    },
    "tables": {
        "bronze": {
            "iot_sensor_events": "brz_freshsip.iot_sensor_events_raw",
        }
    },
}

_PLANNED_CATEGORIES = {"PLANNED_MAINTENANCE", "CHANGEOVER"}

# COMMAND ----------
def _add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str) -> DataFrame:
    """Add five standard Bronze metadata columns."""
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

# COMMAND ----------
def _to_event_schema(df: DataFrame) -> DataFrame:
    """Ensure a DataFrame has exactly the Bronze iot_sensor_events_raw schema columns."""
    expected_cols = [
        "event_id", "batch_id", "production_line_id", "event_type",
        "event_timestamp", "sku_id", "expected_output_cases",
        "actual_output_cases", "qc_status", "downtime_start_ts",
        "downtime_end_ts", "sensor_temperature", "sensor_pressure", "operator_id",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(StringType()))
    return df.select(expected_cols)

# COMMAND ----------
def ingest_batches(spark: SparkSession) -> DataFrame:
    """Read production batches JSON and convert to IoT event rows."""
    source_path = _CONFIG["sources"]["production"]["base_path"] + "/batches.json"
    df_raw = spark.read.json(source_path)

    df_start = (
        df_raw
        .withColumn("event_id", F.concat(F.col("batch_id"), F.lit("_START")))
        .withColumn("event_type", F.lit("BATCH_START"))
        .withColumn("event_timestamp", F.col("start_time"))
        .withColumn("production_line_id", F.col("production_line"))
        .withColumn("expected_output_cases", F.col("target_quantity").cast(StringType()))
        .withColumn("actual_output_cases", F.lit(None).cast(StringType()))
        .withColumn("qc_status", F.lit(None).cast(StringType()))
    )

    df_end = (
        df_raw
        .withColumn("event_id", F.concat(F.col("batch_id"), F.lit("_END")))
        .withColumn("event_type", F.lit("BATCH_END"))
        .withColumn("event_timestamp", F.col("end_time"))
        .withColumn("production_line_id", F.col("production_line"))
        .withColumn("expected_output_cases", F.col("target_quantity").cast(StringType()))
        .withColumn("actual_output_cases", F.col("actual_quantity").cast(StringType()))
        .withColumn("qc_status", F.col("status"))
    )

    df_events = df_start.unionByName(df_end, allowMissingColumns=True)
    return _to_event_schema(df_events)

# COMMAND ----------
def ingest_quality_checks(spark: SparkSession) -> DataFrame:
    """Read production QC check JSON and convert to IoT QC_CHECK event rows."""
    source_path = _CONFIG["sources"]["production"]["base_path"] + "/quality_checks.json"
    df_raw = spark.read.json(source_path)

    df_events = (
        df_raw
        .withColumn("event_id", F.col("check_id"))
        .withColumn("event_type", F.lit("QC_CHECK"))
        .withColumn("event_timestamp", F.col("timestamp"))
        .withColumn("production_line_id", F.lit(None).cast(StringType()))
        .withColumn("sku_id", F.lit(None).cast(StringType()))
        .withColumn("expected_output_cases", F.lit(None).cast(StringType()))
        .withColumn("actual_output_cases", F.lit(None).cast(StringType()))
        .withColumn("qc_status", F.col("result"))
        .withColumn("sensor_temperature", F.lit(None).cast(StringType()))
        .withColumn("sensor_pressure", F.lit(None).cast(StringType()))
        .withColumn("operator_id", F.col("inspector_id"))
    )

    return _to_event_schema(df_events)

# COMMAND ----------
def ingest_downtime_events(spark: SparkSession) -> DataFrame:
    """Read downtime events JSON and convert to IoT downtime event rows."""
    source_path = _CONFIG["sources"]["production"]["base_path"] + "/downtime_events.json"
    df_raw = spark.read.json(source_path)

    df_events = (
        df_raw
        .withColumn(
            "event_type",
            F.when(F.col("category").isin(list(_PLANNED_CATEGORIES)),
                   F.lit("DOWNTIME_PLANNED"))
            .otherwise(F.lit("DOWNTIME_UNPLANNED"))
        )
        .withColumn("event_id", F.col("event_id"))
        .withColumn("production_line_id", F.col("production_line"))
        .withColumn("event_timestamp", F.col("start_time"))
        .withColumn("downtime_start_ts", F.col("start_time"))
        .withColumn("downtime_end_ts", F.col("end_time"))
        .withColumn("batch_id", F.lit(None).cast(StringType()))
        .withColumn("sku_id", F.lit(None).cast(StringType()))
    )

    return _to_event_schema(df_events)

# COMMAND ----------
# Main execution
batch_id = str(uuid.uuid4())
target_table = _CONFIG["tables"]["bronze"]["iot_sensor_events"]

print(f"Production Bronze pipeline starting | batch_id={batch_id}")

df_batches  = ingest_batches(spark)
df_qc       = ingest_quality_checks(spark)
df_downtime = ingest_downtime_events(spark)

df_all = df_batches.unionByName(df_qc).unionByName(df_downtime)

source_path = _CONFIG["sources"]["production"]["base_path"] + "/batches.json"
df_final = _add_bronze_metadata(df_all, source_path, batch_id)

spark.sql("CREATE DATABASE IF NOT EXISTS brz_freshsip")
(
    df_final.write
    .format("delta")
    .mode("append")
    .partitionBy("ingestion_date")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

count = df_final.count()
print(f"Production Bronze pipeline complete | records={count}")
