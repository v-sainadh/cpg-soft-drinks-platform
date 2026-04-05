# Databricks notebook source

# COMMAND ----------
"""
Shared Bronze layer utilities for FreshSip Beverages CPG Data Platform.

Centralises the Bronze metadata helper so all Bronze ingestion pipelines
import from a single source.
"""

import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# COMMAND ----------
def add_bronze_metadata(df: DataFrame, source_path: str, batch_id: str,
                        pipeline_run_id: str = None) -> DataFrame:
    """
    Append the five standard Bronze metadata columns to a DataFrame.

    All non-metadata columns are cast to STRING to comply with Bronze
    schema-on-read. Metadata columns retain their native types
    (TIMESTAMP for _ingested_at, DATE for ingestion_date).

    Args:
        df: Input DataFrame with raw source columns.
        source_path: Source file path string for lineage tracking.
        batch_id: UUID string identifying this ingestion batch run.
        pipeline_run_id: Optional Databricks job run ID.

    Returns:
        DataFrame with all data columns as STRING plus 5 metadata columns:
        _ingested_at, _source_file, _batch_id, _pipeline_run_id, ingestion_date.
    """
    run_id = pipeline_run_id or f"manual_{datetime.date.today().isoformat()}"

    # Cast every pre-existing column to STRING (Bronze = schema-on-read)
    string_cols = [F.col(c).cast(StringType()).alias(c) for c in df.columns]
    df = df.select(string_cols)

    return (
        df
        .withColumn("_ingested_at",     F.current_timestamp())
        .withColumn("_source_file",     F.lit(source_path))
        .withColumn("_batch_id",        F.lit(batch_id))
        .withColumn("_pipeline_run_id", F.lit(run_id))
        .withColumn("ingestion_date",   F.to_date(F.col("_ingested_at")))
    )
