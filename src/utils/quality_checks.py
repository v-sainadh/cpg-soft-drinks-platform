"""
Reusable Data Quality (DQ) framework for FreshSip Beverages CPG Data Platform.

All pipelines use run_quality_checks() to enforce rules defined in:
  _bmad-output/architecture/data-quality-rules.md

Severity model:
  - 'error'   (BLOCKER): Records failing the rule are removed from the clean DF
                          and appended to the quarantine DF. If > dq_fail_rate_pct %
                          of the batch fails, ValueError is raised to halt the pipeline.
  - 'warning' (WARN):    Records are flagged (a boolean column added) but stay in clean DF.
                          A warning is logged. Pipeline continues.

Usage:
    from src.utils.quality_checks import run_quality_checks

    rules = [
        {"type": "not_null",  "columns": ["transaction_id", "sku_id"], "severity": "error"},
        {"type": "unique",    "columns": ["transaction_id"],           "severity": "error"},
        {"type": "range",     "column":  "unit_price", "min_val": 0,   "severity": "warning"},
        {"type": "referential", "fk_col": "sku_id",
         "ref_values": known_skus, "severity": "warning"},
    ]
    result = run_quality_checks(df, rules, logger, total_count=df.count())
    clean_df      = result["clean_df"]
    quarantine_df = result["quarantine_df"]
    report        = result["report"]
"""

import logging
from typing import Any, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

logger = logging.getLogger(__name__)

# Default threshold — overridden by pipeline_config.yaml via run_quality_checks()
_DEFAULT_FAIL_RATE_PCT = 5.0


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------


def check_not_null(
    df: DataFrame,
    columns: list,
    severity: str = "error",
    flag_col: str = "_dq_null_flag",
) -> tuple:
    """
    Check that none of the specified columns contain NULL values.

    For 'error' severity: removes failing records and returns them as quarantine_df.
    For 'warning' severity: adds a boolean flag column; all records stay in clean_df.

    Args:
        df: Input DataFrame.
        columns: List of column names to check for NULLs.
        severity: 'error' to quarantine | 'warning' to flag.
        flag_col: Name of the boolean flag column added in warning mode.

    Returns:
        Tuple (clean_df, failed_df, failed_count).
        In warning mode failed_df is an empty DF with the same schema.
    """
    # Build condition: any column is null
    null_condition = F.lit(False)
    for col in columns:
        null_condition = null_condition | F.col(col).isNull()

    failed_count = df.filter(null_condition).count()

    if severity == "error":
        clean_df = df.filter(~null_condition).withColumn(
            "_dq_failure_reason", F.lit(None).cast("string")
        )
        failed_df = df.filter(null_condition).withColumn(
            "_dq_failure_reason",
            F.lit(f"null_in_columns:{','.join(columns)}"),
        )
        return clean_df, failed_df, failed_count
    else:
        # warning: flag but keep all records
        clean_df = df.withColumn(flag_col, null_condition.cast(BooleanType()))
        empty_failed = df.filter(F.lit(False))
        return clean_df, empty_failed, failed_count


def check_unique(
    df: DataFrame,
    columns: list,
    severity: str = "error",
) -> tuple:
    """
    Enforce uniqueness on a set of columns by deduplicating.

    Keeps the first occurrence per key; all subsequent duplicates go to failed_df.

    Args:
        df: Input DataFrame.
        columns: List of column names forming the unique key.
        severity: 'error' to quarantine duplicates | 'warning' to flag.

    Returns:
        Tuple (clean_df, failed_df, dup_count).
    """
    # Add row number within each key group; keep row 1, quarantine rest
    from pyspark.sql import Window

    window = Window.partitionBy(*columns).orderBy(F.monotonically_increasing_id())
    df_ranked = df.withColumn("_rn", F.row_number().over(window))

    clean_df = df_ranked.filter(F.col("_rn") == 1).drop("_rn")
    failed_df = df_ranked.filter(F.col("_rn") > 1).drop("_rn")

    if severity == "error":
        failed_df = failed_df.withColumn(
            "_dq_failure_reason",
            F.lit(f"duplicate_key:{','.join(columns)}"),
        )
        dup_count = failed_df.count()
        return clean_df, failed_df, dup_count
    else:
        # warning mode: return all records as clean, flag dupes separately
        df_with_flag = df_ranked.withColumn(
            "_dq_dup_flag", (F.col("_rn") > 1).cast(BooleanType())
        ).drop("_rn")
        dup_count = df_with_flag.filter(F.col("_dq_dup_flag")).count()
        empty_failed = df.filter(F.lit(False))
        return df_with_flag, empty_failed, dup_count


def check_range(
    df: DataFrame,
    column: str,
    min_val: Optional[Any] = None,
    max_val: Optional[Any] = None,
    severity: str = "warning",
    flag_col: Optional[str] = None,
) -> tuple:
    """
    Check that numeric values in 'column' fall within [min_val, max_val].

    For 'error': removes out-of-range records.
    For 'warning': adds a boolean flag column.

    Args:
        df: Input DataFrame.
        column: Column name to check.
        min_val: Minimum valid value (inclusive). None = no lower bound.
        max_val: Maximum valid value (inclusive). None = no upper bound.
        severity: 'error' to quarantine | 'warning' to flag.
        flag_col: Flag column name in warning mode (default: _dq_range_<column>).

    Returns:
        Tuple (clean_df, failed_df, failed_count).
    """
    if flag_col is None:
        flag_col = f"_dq_range_{column}"

    # Build out-of-range condition
    oor = F.lit(False)
    if min_val is not None:
        oor = oor | (F.col(column) < F.lit(min_val))
    if max_val is not None:
        oor = oor | (F.col(column) > F.lit(max_val))
    # Treat NULLs as failing range check
    oor = oor | F.col(column).isNull()

    failed_count = df.filter(oor).count()

    if severity == "error":
        clean_df = df.filter(~oor)
        failed_df = df.filter(oor).withColumn(
            "_dq_failure_reason",
            F.lit(f"out_of_range:{column}[{min_val},{max_val}]"),
        )
        return clean_df, failed_df, failed_count
    else:
        clean_df = df.withColumn(flag_col, oor.cast(BooleanType()))
        empty_failed = df.filter(F.lit(False))
        return clean_df, empty_failed, failed_count


def check_referential_integrity(
    df: DataFrame,
    fk_col: str,
    ref_values: Union[list, set],
    severity: str = "warning",
    flag_col: Optional[str] = None,
) -> tuple:
    """
    Check that all values in fk_col exist in the ref_values set.

    Args:
        df: Input DataFrame.
        fk_col: Foreign key column to validate.
        ref_values: Collection of valid values (list or set).
        severity: 'error' to quarantine orphans | 'warning' to flag them.
        flag_col: Flag column name in warning mode.

    Returns:
        Tuple (clean_df, failed_df, orphan_count).
    """
    if flag_col is None:
        flag_col = f"_dq_ref_{fk_col}"

    ref_set = set(ref_values)
    is_orphan = ~F.col(fk_col).isin(list(ref_set))

    orphan_count = df.filter(is_orphan).count()

    if severity == "error":
        clean_df = df.filter(~is_orphan)
        failed_df = df.filter(is_orphan).withColumn(
            "_dq_failure_reason",
            F.lit(f"referential_integrity:{fk_col}"),
        )
        return clean_df, failed_df, orphan_count
    else:
        clean_df = df.withColumn(flag_col, is_orphan.cast(BooleanType()))
        empty_failed = df.filter(F.lit(False))
        return clean_df, empty_failed, orphan_count


def check_custom(
    df: DataFrame,
    condition_expr: str,
    flag_col: str,
    severity: str = "warning",
) -> tuple:
    """
    Apply an arbitrary SQL expression as a DQ check.

    The condition_expr should evaluate to TRUE for FAILING records.
    Example: "unit_price < 0 OR unit_price IS NULL"

    Args:
        df: Input DataFrame.
        condition_expr: SQL expression string; evaluates to True for failures.
        flag_col: Column name for the boolean failure flag.
        severity: 'error' to quarantine | 'warning' to flag.

    Returns:
        Tuple (clean_df, failed_df, failed_count).
    """
    fails = F.expr(condition_expr)
    failed_count = df.filter(fails).count()

    if severity == "error":
        clean_df = df.filter(~fails)
        failed_df = df.filter(fails).withColumn(
            "_dq_failure_reason",
            F.lit(f"custom_check_failed:{flag_col}"),
        )
        return clean_df, failed_df, failed_count
    else:
        clean_df = df.withColumn(flag_col, fails.cast(BooleanType()))
        empty_failed = df.filter(F.lit(False))
        return clean_df, empty_failed, failed_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_quality_checks(
    df: DataFrame,
    rules: list,
    dq_logger=None,
    total_count: Optional[int] = None,
    fail_rate_pct: float = _DEFAULT_FAIL_RATE_PCT,
    quarantine_table: Optional[str] = None,
    spark=None,
) -> dict:
    """
    Run a list of DQ rules against a DataFrame.

    Rules are applied sequentially. After all BLOCKER ('error') checks, if the
    cumulative failure rate exceeds fail_rate_pct, a ValueError is raised to halt
    the pipeline. WARNING-severity rules log but do not filter.

    Args:
        df: Input DataFrame.
        rules: List of rule dicts. Each dict must have 'type' and 'severity'.
               See module docstring for full schema.
        dq_logger: Optional logger; falls back to module logger.
        total_count: Total records before any filtering (for failure rate calculation).
                     If None, calculated from df at start.
        fail_rate_pct: Threshold (%) above which the pipeline halts.
        quarantine_table: Optional fully-qualified table name to write quarantine records.
        spark: Optional SparkSession (needed if quarantine_table is provided).

    Returns:
        Dict with keys:
          - 'clean_df':      DataFrame with passing records.
          - 'quarantine_df': DataFrame of all quarantined (BLOCKER-failed) records.
          - 'report':        List of dicts, one per rule: {rule_type, severity, failed_count}.

    Raises:
        ValueError: If cumulative BLOCKER failure rate exceeds fail_rate_pct.
    """
    log = dq_logger or logger

    if total_count is None:
        total_count = df.count()

    log.info("DQ START | rules=%d | total_records=%d", len(rules), total_count)

    clean_df = df
    quarantine_parts = []
    report = []
    cumulative_blocker_failures = 0

    for rule in rules:
        rule_type = rule.get("type")
        severity = rule.get("severity", "warning")

        if rule_type == "not_null":
            clean_df, failed_df, n = check_not_null(
                clean_df, rule["columns"], severity=severity
            )

        elif rule_type == "unique":
            clean_df, failed_df, n = check_unique(
                clean_df, rule["columns"], severity=severity
            )

        elif rule_type == "range":
            clean_df, failed_df, n = check_range(
                clean_df,
                rule["column"],
                min_val=rule.get("min_val"),
                max_val=rule.get("max_val"),
                severity=severity,
                flag_col=rule.get("flag_col"),
            )

        elif rule_type == "referential":
            clean_df, failed_df, n = check_referential_integrity(
                clean_df,
                rule["fk_col"],
                rule["ref_values"],
                severity=severity,
                flag_col=rule.get("flag_col"),
            )

        elif rule_type == "custom":
            clean_df, failed_df, n = check_custom(
                clean_df,
                rule["condition_expr"],
                rule["flag_col"],
                severity=severity,
            )

        else:
            log.warning("Unknown DQ rule type '%s' — skipping.", rule_type)
            report.append({"rule_type": rule_type, "severity": severity, "failed_count": 0, "status": "skipped"})
            continue

        # Track blocker failures for halt-threshold check
        if severity == "error" and n > 0:
            cumulative_blocker_failures += n
            quarantine_parts.append(failed_df)
            log.warning("DQ BLOCKER | rule=%s | failed=%d", rule_type, n)
        elif severity == "warning" and n > 0:
            log.warning("DQ WARNING | rule=%s | flagged=%d", rule_type, n)

        report.append({"rule_type": rule_type, "severity": severity, "failed_count": n, "status": "applied"})

    # Build quarantine DF
    if quarantine_parts:
        from functools import reduce
        quarantine_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), quarantine_parts)
    else:
        quarantine_df = clean_df.filter(F.lit(False))

    # Write quarantine records to table if requested
    if quarantine_table and spark and cumulative_blocker_failures > 0:
        try:
            (quarantine_df
             .write.format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .saveAsTable(quarantine_table))
            log.info("Quarantine records written to %s", quarantine_table)
        except Exception as exc:
            log.warning("Could not write quarantine records: %s", exc)

    # Halt check
    if total_count > 0:
        fail_rate = (cumulative_blocker_failures / total_count) * 100
        log.info(
            "DQ END | blocker_failures=%d | total=%d | fail_rate=%.2f%%",
            cumulative_blocker_failures, total_count, fail_rate,
        )
        if fail_rate > fail_rate_pct:
            raise ValueError(
                f"DQ halt: {fail_rate:.2f}% of records failed BLOCKER checks "
                f"(threshold={fail_rate_pct}%). Pipeline aborted."
            )

    return {
        "clean_df": clean_df,
        "quarantine_df": quarantine_df,
        "report": report,
    }
