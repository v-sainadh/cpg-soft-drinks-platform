"""
Unit tests for Gold aggregation and KPI pipeline logic.

Tests aggregation functions and KPI computations in isolation using
in-memory DataFrames — no Delta reads/writes.
"""

import datetime
import pytest
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
    DateType, TimestampType, BooleanType, LongType
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

from src.gold.dim_date import generate_date_dimension
from src.gold.fact_sales import compute_fact_sales
from src.gold.fact_inventory_snapshot import compute_fact_inventory_snapshot
from src.gold.fact_production_batch import compute_fact_production_batch
from src.gold.fact_shipment import compute_fact_shipment
from src.gold.kpi_daily_revenue import compute_kpi_daily_revenue
from src.gold.kpi_inventory_turnover import compute_kpi_inventory_turnover
from src.gold.kpi_production_yield import compute_kpi_production_yield
from src.gold.kpi_fulfillment_rate import compute_kpi_fulfillment_rate


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------

class TestDimDate:
    def test_generates_correct_row_count_for_one_year(self, spark):
        """2025 (non-leap year) has 365 rows in the date dimension."""
        df = generate_date_dimension(spark, 2025, 2025)
        assert df.count() == 365

    def test_date_key_format_is_yyyymmdd_integer(self, spark):
        """date_key for 2025-01-01 should be 20250101."""
        df = generate_date_dimension(spark, 2025, 2025)
        row = df.filter(F.col("full_date") == "2025-01-01").first()
        assert row is not None
        assert row["date_key"] == 20250101

    def test_weekend_flag_correct_for_saturday(self, spark):
        """2025-01-04 is a Saturday → is_weekend = True."""
        df = generate_date_dimension(spark, 2025, 2025)
        row = df.filter(F.col("full_date") == "2025-01-04").first()
        assert row["is_weekend"] == True

    def test_weekday_flag_false_for_monday(self, spark):
        """2025-01-06 is a Monday → is_weekend = False."""
        df = generate_date_dimension(spark, 2025, 2025)
        row = df.filter(F.col("full_date") == "2025-01-06").first()
        assert row["is_weekend"] == False

    def test_no_duplicate_date_keys(self, spark):
        """date_key is unique across all rows."""
        df = generate_date_dimension(spark, 2025, 2025)
        total = df.count()
        distinct = df.select("date_key").distinct().count()
        assert total == distinct

    def test_fiscal_quarter_october_is_fq1(self, spark):
        """October (FY starts Oct 1) should be fiscal quarter FQ1."""
        df = generate_date_dimension(spark, 2025, 2025)
        row = df.filter(F.col("full_date") == "2025-10-01").first()
        assert row["fiscal_quarter"] == "FQ1"


# ---------------------------------------------------------------------------
# fact_sales
# ---------------------------------------------------------------------------

class TestFactSales:
    def _make_txn_df(self, spark):
        schema = StructType([
            StructField("transaction_key",  LongType(),       False),
            StructField("transaction_id",   StringType(),     True),
            StructField("retailer_id",      StringType(),     True),
            StructField("sku_id",           StringType(),     True),
            StructField("quantity_sold",    IntegerType(),    True),
            StructField("unit_price",       DecimalType(10,2),True),
            StructField("net_line_amount",  DecimalType(12,2),True),
            StructField("transaction_date", DateType(),       True),
        ])
        return spark.createDataFrame([
            (1001, "TXN-001", "CUST-001", "SKU-001",
             10, Decimal("3.50"), Decimal("35.00"), datetime.date(2025,10,1)),
        ], schema=schema)

    def _make_product_df(self, spark):
        schema = StructType([
            StructField("sku_id",                 StringType(),      True),
            StructField("standard_cost_per_unit", DecimalType(10,2), True),
        ])
        return spark.createDataFrame(
            [("SKU-001", Decimal("1.50"))], schema=schema
        )

    def _make_customer_df(self, spark):
        schema = StructType([
            StructField("surrogate_key", LongType(),   False),
            StructField("retailer_id",   StringType(), True),
            StructField("is_current",    BooleanType(),True),
        ])
        return spark.createDataFrame([(401, "CUST-001", True)], schema=schema)

    def _make_dim_date(self, spark):
        return generate_date_dimension(spark, 2025, 2025)

    def test_net_revenue_computed(self, spark):
        """net_revenue = unit_price × quantity_sold."""
        df_txn  = self._make_txn_df(spark)
        df_prod = self._make_product_df(spark)
        df_cust = self._make_customer_df(spark)
        df_date = self._make_dim_date(spark)
        result  = compute_fact_sales(df_txn, df_prod, df_cust, df_date)
        row = result.first()
        assert float(row["net_revenue"]) == pytest.approx(35.00, rel=0.01)

    def test_cogs_computed(self, spark):
        """cogs = standard_cost_per_unit × quantity_sold."""
        df_txn  = self._make_txn_df(spark)
        df_prod = self._make_product_df(spark)
        df_cust = self._make_customer_df(spark)
        df_date = self._make_dim_date(spark)
        result  = compute_fact_sales(df_txn, df_prod, df_cust, df_date)
        row = result.first()
        # 10 units × 1.50 = 15.00
        assert float(row["cogs"]) == pytest.approx(15.00, rel=0.01)

    def test_gross_margin_computed(self, spark):
        """gross_margin = net_revenue - cogs."""
        df_txn  = self._make_txn_df(spark)
        df_prod = self._make_product_df(spark)
        df_cust = self._make_customer_df(spark)
        df_date = self._make_dim_date(spark)
        result  = compute_fact_sales(df_txn, df_prod, df_cust, df_date)
        row = result.first()
        assert float(row["gross_margin_amount"]) == pytest.approx(20.00, rel=0.01)

    def test_return_amount_defaults_to_zero(self, spark):
        """return_amount column defaulted to 0 when no return data joined."""
        df_txn  = self._make_txn_df(spark)
        df_prod = self._make_product_df(spark)
        df_cust = self._make_customer_df(spark)
        df_date = self._make_dim_date(spark)
        result  = compute_fact_sales(df_txn, df_prod, df_cust, df_date)
        assert float(result.first()["return_amount"]) == 0.0


# ---------------------------------------------------------------------------
# fact_inventory_snapshot
# ---------------------------------------------------------------------------

class TestFactInventorySnapshot:
    def _make_stock_df(self, spark):
        schema = StructType([
            StructField("warehouse_id",           StringType(),      True),
            StructField("sku_id",                 StringType(),      True),
            StructField("units_on_hand",          IntegerType(),     True),
            StructField("inventory_value",        DecimalType(14,2), True),
            StructField("dsi_days",               DecimalType(8,1),  True),
            StructField("snapshot_date",          DateType(),        True),
        ])
        return spark.createDataFrame([
            ("WH-001","SKU-001", 5000, Decimal("7500.00"), Decimal("150.0"),
             datetime.date(2025,10,1)),
            ("WH-001","SKU-002",  300, Decimal("600.00"),  Decimal("10.0"),
             datetime.date(2025,10,1)),
        ], schema=schema)

    def _make_rop_df(self, spark):
        schema = StructType([
            StructField("sku_id",             StringType(), True),
            StructField("warehouse_id",       StringType(), True),
            StructField("reorder_point_units",IntegerType(),True),
        ])
        return spark.createDataFrame([
            ("SKU-001","WH-001", 600),
            ("SKU-002","WH-001", 400),  # SKU-002 at 300 < 400 → alert
        ], schema=schema)

    def test_reorder_alert_flag_true_when_below_rop(self, spark):
        """units_on_hand <= reorder_point_units → reorder_alert_flag = True."""
        df_stock = self._make_stock_df(spark)
        df_rop   = self._make_rop_df(spark)
        df_date  = generate_date_dimension(spark, 2025, 2025)
        result   = compute_fact_inventory_snapshot(df_stock, df_rop, df_date)
        row = result.filter(F.col("product_key") ==
                            F.abs(F.hash(F.lit("SKU-002")))).first()
        assert row["reorder_alert_flag"] == True

    def test_reorder_alert_flag_false_when_above_rop(self, spark):
        """units_on_hand > reorder_point_units → reorder_alert_flag = False."""
        df_stock = self._make_stock_df(spark)
        df_rop   = self._make_rop_df(spark)
        df_date  = generate_date_dimension(spark, 2025, 2025)
        result   = compute_fact_inventory_snapshot(df_stock, df_rop, df_date)
        row = result.filter(F.col("product_key") ==
                            F.abs(F.hash(F.lit("SKU-001")))).first()
        assert row["reorder_alert_flag"] == False

    def test_inventory_value_preserved(self, spark):
        """inventory_value from Silver preserved in Gold fact table."""
        df_stock = self._make_stock_df(spark)
        df_rop   = self._make_rop_df(spark)
        df_date  = generate_date_dimension(spark, 2025, 2025)
        result   = compute_fact_inventory_snapshot(df_stock, df_rop, df_date)
        row = result.filter(F.col("product_key") ==
                            F.abs(F.hash(F.lit("SKU-001")))).first()
        assert float(row["inventory_value"]) == pytest.approx(7500.00, rel=0.01)


# ---------------------------------------------------------------------------
# fact_production_batch
# ---------------------------------------------------------------------------

class TestFactProductionBatch:
    def _make_batches_df(self, spark):
        schema = StructType([
            StructField("batch_key",             LongType(),       False),
            StructField("batch_id",              StringType(),     True),
            StructField("production_line_id",    StringType(),     True),
            StructField("sku_id",                StringType(),     True),
            StructField("expected_output_cases", IntegerType(),    True),
            StructField("actual_output_cases",   IntegerType(),    True),
            StructField("yield_rate_pct",        DecimalType(5,2), True),
            StructField("qc_pass_flag",          BooleanType(),    True),
            StructField("qc_status",             StringType(),     True),
            StructField("batch_date",            DateType(),       True),
        ])
        return spark.createDataFrame([
            (201,"BATCH-001","LINE-A","SKU-001",3000,2900,
             Decimal("96.67"),True,"PASS",datetime.date(2025,10,1)),
        ], schema=schema)

    def _make_events_df(self, spark):
        schema = StructType([
            StructField("batch_id",        StringType(),     True),
            StructField("event_type",      StringType(),     True),
            StructField("downtime_hours",  DecimalType(6,2), True),
        ])
        return spark.createDataFrame([
            ("BATCH-001","DOWNTIME_UNPLANNED",Decimal("2.50")),
            ("BATCH-001","DOWNTIME_PLANNED",  Decimal("1.00")),
        ], schema=schema)

    def test_downtime_aggregated_per_batch(self, spark):
        """Total downtime = sum of all downtime events for that batch."""
        df_batch  = self._make_batches_df(spark)
        df_events = self._make_events_df(spark)
        df_date   = generate_date_dimension(spark, 2025, 2025)
        result    = compute_fact_production_batch(df_batch, df_events, df_date)
        row = result.first()
        assert float(row["downtime_hours"]) == pytest.approx(3.50, rel=0.01)

    def test_yield_preserved_from_silver(self, spark):
        """yield_rate_pct from Silver is preserved in Gold fact."""
        df_batch  = self._make_batches_df(spark)
        df_events = self._make_events_df(spark)
        df_date   = generate_date_dimension(spark, 2025, 2025)
        result    = compute_fact_production_batch(df_batch, df_events, df_date)
        row = result.first()
        assert float(row["yield_rate_pct"]) == pytest.approx(96.67, rel=0.01)

    def test_qc_pass_flag_preserved(self, spark):
        """qc_pass_flag from Silver preserved in Gold fact."""
        df_batch  = self._make_batches_df(spark)
        df_events = self._make_events_df(spark)
        df_date   = generate_date_dimension(spark, 2025, 2025)
        result    = compute_fact_production_batch(df_batch, df_events, df_date)
        assert result.first()["qc_pass_flag"] == True


# ---------------------------------------------------------------------------
# KPI: Daily Revenue
# ---------------------------------------------------------------------------

class TestKPIDailyRevenue:
    def test_revenue_aggregated_by_category_and_region(self, spark, sample_silver_transactions_df,
                                                         sample_silver_products_df,
                                                         sample_silver_customers_df):
        """Revenue aggregates correctly by product_category and region."""
        result = compute_kpi_daily_revenue(
            sample_silver_transactions_df,
            sample_silver_products_df,
            sample_silver_customers_df,
        )
        assert result.count() > 0
        assert "total_revenue" in result.columns
        assert "product_category" in result.columns

    def test_total_revenue_is_non_negative(self, spark, sample_silver_transactions_df,
                                            sample_silver_products_df,
                                            sample_silver_customers_df):
        """Total revenue across all rows is > 0."""
        result = compute_kpi_daily_revenue(
            sample_silver_transactions_df,
            sample_silver_products_df,
            sample_silver_customers_df,
        )
        total = result.agg(F.sum("total_revenue")).first()[0]
        assert total is not None and float(total) > 0

    def test_gross_margin_pct_within_bounds(self, spark, sample_silver_transactions_df,
                                             sample_silver_products_df,
                                             sample_silver_customers_df):
        """gross_margin_pct should be between 0% and 100%."""
        result = compute_kpi_daily_revenue(
            sample_silver_transactions_df,
            sample_silver_products_df,
            sample_silver_customers_df,
        )
        out_of_bounds = result.filter(
            (F.col("gross_margin_pct") < 0) | (F.col("gross_margin_pct") > 100)
        ).count()
        assert out_of_bounds == 0


# ---------------------------------------------------------------------------
# KPI: Production Yield
# ---------------------------------------------------------------------------

class TestKPIProductionYield:
    def _make_fact_prod_df(self, spark):
        schema = StructType([
            StructField("batch_date",          DateType(),       True),
            StructField("production_line_id",  StringType(),     True),
            StructField("yield_rate_pct",      DecimalType(5,2), True),
            StructField("qc_pass_flag",        BooleanType(),    True),
            StructField("downtime_hours",      DecimalType(8,2), True),
            StructField("expected_output_units",IntegerType(),   True),
            StructField("actual_output_units", IntegerType(),    True),
        ])
        return spark.createDataFrame([
            (datetime.date(2025,10,1),"LINE-A",Decimal("96.50"),True, Decimal("0.50"),3000,2895),
            (datetime.date(2025,10,1),"LINE-A",Decimal("95.00"),True, Decimal("1.00"),2000,1900),
            (datetime.date(2025,10,1),"LINE-B",Decimal("85.00"),False,Decimal("3.00"),2000,1700),
        ], schema=schema)

    def test_avg_yield_by_line_and_date(self, spark):
        """Average yield computed correctly per production_line/date."""
        df   = self._make_fact_prod_df(spark)
        result = compute_kpi_production_yield(df)
        row = result.filter(F.col("production_line_id") == "LINE-A").first()
        assert float(row["avg_yield_rate_pct"]) == pytest.approx(95.75, rel=0.01)

    def test_qc_pass_rate_computed(self, spark):
        """qc_pass_rate_pct = (pass count / total) × 100."""
        df   = self._make_fact_prod_df(spark)
        result = compute_kpi_production_yield(df)
        row = result.filter(F.col("production_line_id") == "LINE-A").first()
        # Both LINE-A batches pass → 100%
        assert float(row["qc_pass_rate_pct"]) == pytest.approx(100.0, rel=0.01)

    def test_downtime_aggregated(self, spark):
        """Total downtime summed per line per date."""
        df   = self._make_fact_prod_df(spark)
        result = compute_kpi_production_yield(df)
        row = result.filter(F.col("production_line_id") == "LINE-A").first()
        assert float(row["total_downtime_hours"]) == pytest.approx(1.50, rel=0.01)


# ---------------------------------------------------------------------------
# KPI: Fulfillment Rate
# ---------------------------------------------------------------------------

class TestKPIFulfillmentRate:
    def test_on_time_rate_computed(self, spark, sample_silver_shipments_df,
                                    sample_silver_customers_df):
        """on_time_delivery_pct computed from on_time_flag."""
        result = compute_kpi_fulfillment_rate(
            sample_silver_shipments_df,
            sample_silver_customers_df,
        )
        assert result.count() > 0
        assert "on_time_delivery_pct" in result.columns

    def test_fulfillment_rate_computed(self, spark, sample_silver_shipments_df,
                                        sample_silver_customers_df):
        """fulfillment_rate_pct computed from is_fully_shipped."""
        result = compute_kpi_fulfillment_rate(
            sample_silver_shipments_df,
            sample_silver_customers_df,
        )
        assert "fulfillment_rate_pct" in result.columns

    def test_pct_values_within_0_to_100(self, spark, sample_silver_shipments_df,
                                         sample_silver_customers_df):
        """All percentage values within [0, 100]."""
        result = compute_kpi_fulfillment_rate(
            sample_silver_shipments_df,
            sample_silver_customers_df,
        )
        invalid = result.filter(
            (F.col("on_time_delivery_pct") < 0) |
            (F.col("on_time_delivery_pct") > 100) |
            (F.col("fulfillment_rate_pct") < 0) |
            (F.col("fulfillment_rate_pct") > 100)
        ).count()
        assert invalid == 0
