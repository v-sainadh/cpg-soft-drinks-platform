"""
Unit tests for src/utils/quality_checks.py

Tests all DQ check functions: check_not_null, check_unique, check_range,
check_referential_integrity, check_custom, and run_quality_checks orchestrator.
"""

import datetime
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, LongType
)
from decimal import Decimal

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

from src.utils.quality_checks import (
    check_not_null,
    check_unique,
    check_range,
    check_referential_integrity,
    check_custom,
    run_quality_checks,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_df(spark, rows, cols):
    """Create a simple DataFrame from (rows, cols) for quick test setup."""
    return spark.createDataFrame(rows, cols)


# ---------------------------------------------------------------------------
# check_not_null
# ---------------------------------------------------------------------------

class TestCheckNotNull:
    def test_happy_path_no_nulls(self, spark):
        """All non-null rows pass through; quarantine is empty."""
        df = _simple_df(spark,
                        [("A",), ("B",), ("C",)],
                        ["id"])
        clean, failed, n = check_not_null(df, ["id"], severity="error")
        assert clean.count() == 3
        assert failed.count() == 0
        assert n == 0

    def test_nulls_quarantined_in_error_mode(self, spark):
        """Null values in key column quarantined; clean DF excludes them."""
        df = _simple_df(spark,
                        [("A",), (None,), ("C",), (None,)],
                        ["id"])
        clean, failed, n = check_not_null(df, ["id"], severity="error")
        assert clean.count() == 2
        assert failed.count() == 2
        assert n == 2
        assert failed.filter(F.col("id").isNotNull()).count() == 0

    def test_multiple_columns_any_null_fails(self, spark):
        """Null in ANY of the listed columns causes quarantine."""
        df = _simple_df(spark,
                        [("A", "X"), ("B", None), (None, "Y"), ("D", "Z")],
                        ["id", "name"])
        clean, failed, n = check_not_null(df, ["id", "name"], severity="error")
        assert clean.count() == 2
        assert n == 2

    def test_warning_mode_passes_all_records_through(self, spark):
        """In warning mode, all records stay in clean_df; nulls are flagged."""
        df = _simple_df(spark,
                        [("A",), (None,), ("C",)],
                        ["id"])
        clean, failed, n = check_not_null(df, ["id"], severity="warning",
                                          flag_col="_dq_null_flag")
        # All records in clean
        assert clean.count() == 3
        # Quarantine is empty in warning mode
        assert failed.count() == 0
        assert n == 1  # 1 null counted
        assert "_dq_null_flag" in clean.columns

    def test_empty_dataframe(self, spark):
        """Empty DataFrame returns empty clean and empty quarantine."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame([], schema)
        clean, failed, n = check_not_null(df, ["id"], severity="error")
        assert clean.count() == 0
        assert failed.count() == 0
        assert n == 0


# ---------------------------------------------------------------------------
# check_unique
# ---------------------------------------------------------------------------

class TestCheckUnique:
    def test_all_unique_passthrough(self, spark):
        """No duplicates → all rows pass, quarantine empty."""
        df = _simple_df(spark,
                        [("A",), ("B",), ("C",)],
                        ["id"])
        clean, failed, n = check_unique(df, ["id"], severity="error")
        assert clean.count() == 3
        assert n == 0

    def test_single_column_deduplication(self, spark):
        """One row per unique id retained; duplicate rows quarantined."""
        df = _simple_df(spark,
                        [("A",), ("A",), ("B",), ("C",), ("C",)],
                        ["id"])
        clean, failed, n = check_unique(df, ["id"], severity="error")
        assert clean.count() == 3
        assert n == 2

    def test_composite_key_deduplication(self, spark):
        """Composite key (id, name): duplicates on both columns quarantined."""
        df = _simple_df(spark,
                        [("A", "X"), ("A", "X"), ("A", "Y"), ("B", "X")],
                        ["id", "name"])
        clean, failed, n = check_unique(df, ["id", "name"], severity="error")
        assert clean.count() == 3
        assert n == 1

    def test_all_duplicates_reduces_to_one(self, spark):
        """DataFrame with all same key → one row retained."""
        df = _simple_df(spark,
                        [("A",), ("A",), ("A",)],
                        ["id"])
        clean, failed, n = check_unique(df, ["id"], severity="error")
        assert clean.count() == 1
        assert n == 2


# ---------------------------------------------------------------------------
# check_range
# ---------------------------------------------------------------------------

class TestCheckRange:
    def test_all_in_range(self, spark):
        """Values within [0, 100] → no failures."""
        df = _simple_df(spark, [(50,), (0,), (100,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_range(df, "val", min_val=0, max_val=100, severity="error")
        assert clean.count() == 3
        assert n == 0

    def test_values_below_min_quarantined(self, spark):
        """Values below min_val rejected in error mode."""
        df = _simple_df(spark, [(-1,), (0,), (50,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_range(df, "val", min_val=0, severity="error")
        assert clean.count() == 2
        assert n == 1

    def test_values_above_max_quarantined(self, spark):
        """Values above max_val rejected in error mode."""
        df = _simple_df(spark, [(50,), (101,), (200,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_range(df, "val", max_val=100, severity="error")
        assert clean.count() == 1
        assert n == 2

    def test_null_values_treated_as_failures(self, spark):
        """NULL values fail range check (treated as out-of-range)."""
        df = _simple_df(spark, [(50,), (None,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_range(df, "val", min_val=0, severity="error")
        assert n == 1

    def test_no_min_only_max_works(self, spark):
        """max_val-only open range: values <= max pass."""
        df = _simple_df(spark, [(50,), (150,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_range(df, "val", max_val=100, severity="error")
        assert clean.count() == 1

    def test_warning_mode_flags_not_filters(self, spark):
        """In warning mode, out-of-range values flagged but not removed."""
        df = _simple_df(spark, [(-5,), (50,), (200,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_range(df, "val", min_val=0, max_val=100,
                                       severity="warning", flag_col="_flag")
        assert clean.count() == 3  # all retained
        assert "_flag" in clean.columns
        assert clean.filter(F.col("_flag") == True).count() == 2


# ---------------------------------------------------------------------------
# check_referential_integrity
# ---------------------------------------------------------------------------

class TestCheckReferentialIntegrity:
    def test_all_refs_valid(self, spark):
        """All FK values exist in ref set → no failures."""
        df = _simple_df(spark, [("A",), ("B",)], ["fk"])
        clean, failed, n = check_referential_integrity(df, "fk", ["A", "B", "C"],
                                                       severity="error")
        assert clean.count() == 2
        assert n == 0

    def test_orphan_rejected_in_error_mode(self, spark):
        """FK value not in ref set → quarantined."""
        df = _simple_df(spark, [("A",), ("Z",)], ["fk"])
        clean, failed, n = check_referential_integrity(df, "fk", ["A", "B"],
                                                       severity="error")
        assert clean.count() == 1
        assert n == 1

    def test_warning_mode_flags_orphans(self, spark):
        """In warning mode, orphans flagged but all records kept."""
        df = _simple_df(spark, [("A",), ("Z",)], ["fk"])
        clean, failed, n = check_referential_integrity(df, "fk", ["A"],
                                                       severity="warning",
                                                       flag_col="_dq_ref_fk")
        assert clean.count() == 2
        assert n == 1
        assert clean.filter(F.col("_dq_ref_fk") == True).count() == 1


# ---------------------------------------------------------------------------
# check_custom
# ---------------------------------------------------------------------------

class TestCheckCustom:
    def test_custom_condition_error_mode(self, spark):
        """Custom SQL condition 'val < 0' quarantines negative values."""
        df = _simple_df(spark, [(5,), (-3,), (0,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_custom(df, "val < 0", "_neg_flag", severity="error")
        assert clean.count() == 2
        assert n == 1

    def test_custom_condition_warning_mode(self, spark):
        """Custom condition in warning mode flags but does not remove."""
        df = _simple_df(spark, [(5,), (-3,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_custom(df, "val < 0", "_neg_flag", severity="warning")
        assert clean.count() == 2
        assert n == 1
        assert "_neg_flag" in clean.columns

    def test_no_failures_on_clean_data(self, spark):
        """Clean data produces zero failures."""
        df = _simple_df(spark, [(5,), (10,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        clean, failed, n = check_custom(df, "val < 0", "_neg_flag", severity="error")
        assert clean.count() == 2
        assert n == 0


# ---------------------------------------------------------------------------
# run_quality_checks
# ---------------------------------------------------------------------------

class TestRunQualityChecks:
    def test_clean_data_all_rules_pass(self, spark):
        """Clean data passes all rules with zero failures in report."""
        df = _simple_df(spark, [("TXN-001", "SKU-001", 3, 5.0),
                                 ("TXN-002", "SKU-002", 1, 2.5)],
                        ["txn_id", "sku_id", "qty", "price"])
        df = df.withColumn("qty",   F.col("qty").cast(IntegerType()))
        df = df.withColumn("price", F.col("price").cast(DecimalType(6, 2)))

        rules = [
            {"type": "not_null",  "columns": ["txn_id", "sku_id"], "severity": "error"},
            {"type": "range",     "column":  "qty",   "min_val": 1, "severity": "error"},
            {"type": "range",     "column":  "price", "min_val": 0, "severity": "warning"},
        ]
        result = run_quality_checks(df, rules, total_count=2)
        assert result["clean_df"].count() == 2
        assert result["quarantine_df"].count() == 0
        assert all(r["failed_count"] == 0 for r in result["report"])

    def test_blocker_violations_filtered(self, spark):
        """BLOCKER violations remove records from clean_df."""
        df = _simple_df(spark, [("TXN-001",), (None,), ("TXN-003",)], ["txn_id"])
        rules = [{"type": "not_null", "columns": ["txn_id"], "severity": "error"}]
        result = run_quality_checks(df, rules, total_count=3)
        assert result["clean_df"].count() == 2
        assert result["quarantine_df"].count() == 1

    def test_empty_rules_list_passthrough(self, spark):
        """No rules → entire DataFrame passes through unchanged."""
        df = _simple_df(spark, [("A",), ("B",)], ["id"])
        result = run_quality_checks(df, [], total_count=2)
        assert result["clean_df"].count() == 2

    def test_high_failure_rate_raises_value_error(self, spark):
        """Failure rate > threshold raises ValueError to halt pipeline."""
        df = _simple_df(spark, [(None,), (None,), (None,), ("A",)], ["id"])
        rules = [{"type": "not_null", "columns": ["id"], "severity": "error"}]
        with pytest.raises(ValueError, match="DQ halt"):
            run_quality_checks(df, rules, total_count=4, fail_rate_pct=50.0)

    def test_warning_rules_do_not_filter(self, spark):
        """WARNING rules do not reduce clean_df count."""
        df = _simple_df(spark, [(-1,), (5,)], ["val"])
        df = df.withColumn("val", F.col("val").cast(IntegerType()))
        rules = [{"type": "range", "column": "val", "min_val": 0, "severity": "warning"}]
        result = run_quality_checks(df, rules, total_count=2)
        assert result["clean_df"].count() == 2
        assert result["quarantine_df"].count() == 0

    def test_report_contains_entry_per_rule(self, spark):
        """Report list has one entry per applied rule."""
        df = _simple_df(spark, [("A",), ("B",)], ["id"])
        rules = [
            {"type": "not_null", "columns": ["id"], "severity": "error"},
            {"type": "unique",   "columns": ["id"], "severity": "warning"},
        ]
        result = run_quality_checks(df, rules, total_count=2)
        assert len(result["report"]) == 2
