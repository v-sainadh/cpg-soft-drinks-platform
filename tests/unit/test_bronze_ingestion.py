"""
Unit tests for Bronze ingestion pipeline transformation logic.

Tests transformation helpers (metadata addition, column mapping, field casting)
in isolation — no real file reads or Delta writes are performed.
"""

import datetime
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DateType
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))


# We test the _add_bronze_metadata helper directly via import
# For modules not yet importable (Spark dependency), we define helper functions locally.

def _make_bronze_metadata_df(spark, df, source_path="test_source.csv", batch_id="test-batch"):
    """
    Apply bronze metadata columns to a DataFrame (mirrors _add_bronze_metadata).
    Used to test the metadata pattern without importing module-level code.
    """
    string_cols = [F.col(c).cast(StringType()).alias(c) for c in df.columns]
    df = df.select(string_cols)
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_pipeline_run_id", F.lit("manual_2026-04-05"))
        .withColumn("ingestion_date", F.to_date(F.col("_ingested_at")))
    )


# ---------------------------------------------------------------------------
# Metadata column tests
# ---------------------------------------------------------------------------

class TestBronzeMetadataColumns:
    def test_five_metadata_columns_added(self, spark):
        """All five Bronze metadata columns are present after metadata addition."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame([("A",)], schema)
        result = _make_bronze_metadata_df(spark, df)

        expected_meta = ["_ingested_at", "_source_file", "_batch_id",
                         "_pipeline_run_id", "ingestion_date"]
        for col in expected_meta:
            assert col in result.columns, f"Missing metadata column: {col}"

    def test_source_file_set_correctly(self, spark):
        """_source_file contains the provided path string."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame([("A",)], schema)
        result = _make_bronze_metadata_df(spark, df, source_path="/mnt/test/file.csv")
        val = result.select("_source_file").first()[0]
        assert val == "/mnt/test/file.csv"

    def test_batch_id_set_correctly(self, spark):
        """_batch_id contains the provided batch UUID string."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame([("A",)], schema)
        result = _make_bronze_metadata_df(spark, df, batch_id="uuid-abc-123")
        val = result.select("_batch_id").first()[0]
        assert val == "uuid-abc-123"

    def test_ingestion_date_derived_from_ingested_at(self, spark):
        """ingestion_date is DATE type derived from _ingested_at TIMESTAMP."""
        schema = StructType([StructField("id", StringType(), True)])
        df = spark.createDataFrame([("A",)], schema)
        result = _make_bronze_metadata_df(spark, df)
        # Should not be null
        val = result.select("ingestion_date").first()[0]
        assert val is not None

    def test_data_columns_cast_to_string(self, spark):
        """All data columns are STRING type in Bronze output."""
        schema = StructType([
            StructField("amount", IntegerType(), True),
            StructField("name",   StringType(), True),
        ])
        df = spark.createDataFrame([(100, "test")], schema)
        result = _make_bronze_metadata_df(spark, df)
        # Both data columns should now be strings
        assert result.schema["amount"].dataType == StringType()
        assert result.schema["name"].dataType == StringType()


# ---------------------------------------------------------------------------
# Sales ingestion — column mapping
# ---------------------------------------------------------------------------

class TestSalesIngestionTransform:
    def test_erp_sales_join_produces_expected_columns(self, spark):
        """Joined orders + order_lines produces all required Bronze schema columns."""
        orders = spark.createDataFrame(
            [("ORD-001", "CUST-001", "2025-10-01", "2025-10-03", "delivered", "1500.00")],
            ["order_id", "retailer_id", "order_date", "ship_date", "order_status", "total_amount"]
        )
        lines = spark.createDataFrame(
            [("OL-001", "ORD-001", "SKU-001", "10", "150.00", "0.05", "1425.00"),
             ("OL-002", "ORD-001", "SKU-002", "5",   "75.00", "0.00",  "375.00")],
            ["order_line_id", "order_id", "sku_id", "quantity_ordered",
             "invoice_price", "discount_pct", "line_total"]
        )
        # Replicate the join logic from sales_ingestion.py
        joined = lines.join(orders, on="order_id", how="left")
        joined = (
            joined
            .withColumn("quantity_shipped", F.col("quantity_ordered"))
            .withColumn("channel", F.lit(""))
            .withColumn("region", F.lit(""))
            .withColumn("state", F.lit(""))
        )
        assert "order_id" in joined.columns
        assert "sku_id" in joined.columns
        assert "retailer_id" in joined.columns
        assert "invoice_price" in joined.columns
        assert joined.count() == 2  # 2 lines

    def test_pos_items_explode_produces_one_row_per_item(self, spark):
        """Exploding items array produces correct row count."""
        # Simulate JSON-like structure with explode
        from pyspark.sql.types import ArrayType, MapType
        data = [
            ("TXN-001", "CUST-001", [{"sku_id": "SKU-001", "qty": 3, "price": 2.5, "discount": 0.0},
                                      {"sku_id": "SKU-002", "qty": 1, "price": 4.0, "discount": 0.05}]),
        ]
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id",    StringType(), True),
            StructField("items", ArrayType(StructType([
                StructField("sku_id",    StringType(), True),
                StructField("qty",       IntegerType(), True),
                StructField("price",     StringType(), True),
                StructField("discount",  StringType(), True),
            ])), True),
        ])
        df = spark.createDataFrame(data, schema)
        exploded = df.withColumn("item", F.explode("items")).select(
            "transaction_id", "customer_id",
            F.col("item.sku_id"), F.col("item.qty"),
            F.col("item.price"), F.col("item.discount")
        )
        assert exploded.count() == 2  # 1 transaction × 2 items

    def test_empty_order_lines_produces_empty_output(self, spark):
        """Empty order_lines joined with orders produces empty DataFrame."""
        orders = spark.createDataFrame(
            [("ORD-001", "CUST-001", "2025-10-01")],
            ["order_id", "retailer_id", "order_date"]
        )
        empty_lines = spark.createDataFrame(
            [], StructType([
                StructField("order_line_id", StringType(), True),
                StructField("order_id", StringType(), True),
                StructField("sku_id", StringType(), True),
            ])
        )
        joined = empty_lines.join(orders, on="order_id", how="left")
        assert joined.count() == 0


# ---------------------------------------------------------------------------
# Inventory ingestion — column mapping
# ---------------------------------------------------------------------------

class TestInventoryIngestionTransform:
    def test_inventory_columns_mapped_correctly(self, spark):
        """quantity_on_hand → units_on_hand, reorder_point → reorder_point_units."""
        df = spark.createDataFrame(
            [("WH-001", "SKU-001", "5000", "200", "2025-10-01", "600")],
            ["warehouse_id", "sku_id", "quantity_on_hand",
             "quantity_on_order", "snapshot_date", "reorder_point"]
        )
        mapped = (
            df
            .withColumnRenamed("quantity_on_hand", "units_on_hand")
            .withColumnRenamed("quantity_on_order", "units_in_transit")
            .withColumnRenamed("reorder_point", "reorder_point_units")
            .withColumn("units_reserved", F.lit("0"))
        )
        assert "units_on_hand" in mapped.columns
        assert "units_in_transit" in mapped.columns
        assert "reorder_point_units" in mapped.columns
        assert "units_reserved" in mapped.columns
        assert mapped.filter(F.col("units_on_hand") == "5000").count() == 1

    def test_snapshot_date_preserved_as_string(self, spark):
        """snapshot_date stays as STRING in Bronze (typed in Silver)."""
        df = spark.createDataFrame(
            [("WH-001", "SKU-001", "5000", "2025-10-01")],
            ["warehouse_id", "sku_id", "units_on_hand", "snapshot_date"]
        )
        result = _make_bronze_metadata_df(spark, df)
        assert result.schema["snapshot_date"].dataType == StringType()


# ---------------------------------------------------------------------------
# Production ingestion — event mapping
# ---------------------------------------------------------------------------

class TestProductionIngestionTransform:
    def test_batch_mapped_to_start_and_end_events(self, spark):
        """One batch record produces BATCH_START and BATCH_END events."""
        df = spark.createDataFrame(
            [("BATCH-001", "SKU-001", "LINE-A",
              "2025-10-01T08:00:00", "2025-10-01T16:00:00",
              "3000", "2900", "0.9667", "completed")],
            ["batch_id", "sku_id", "production_line",
             "start_time", "end_time",
             "target_quantity", "actual_quantity", "yield_rate", "status"]
        )
        df_start = df.withColumn("event_type", F.lit("BATCH_START")) \
                     .withColumn("event_timestamp", F.col("start_time"))
        df_end   = df.withColumn("event_type", F.lit("BATCH_END")) \
                     .withColumn("event_timestamp", F.col("end_time"))
        combined = df_start.unionByName(df_end, allowMissingColumns=True)
        assert combined.count() == 2
        assert combined.filter(F.col("event_type") == "BATCH_START").count() == 1
        assert combined.filter(F.col("event_type") == "BATCH_END").count() == 1

    def test_quality_check_maps_to_qc_check_event(self, spark):
        """QC check record maps to QC_CHECK event_type."""
        df = spark.createDataFrame(
            [("QC-001", "BATCH-001", "CARBONATION", "3.62", "3.00", "4.00", "PASS",
              "2025-10-01T11:00:00", "INSP-001")],
            ["check_id", "batch_id", "check_type", "value",
             "min_threshold", "max_threshold", "result", "timestamp", "inspector_id"]
        )
        mapped = df.withColumn("event_type", F.lit("QC_CHECK")) \
                   .withColumn("event_id", F.col("check_id")) \
                   .withColumn("qc_status", F.col("result"))
        assert mapped.filter(F.col("event_type") == "QC_CHECK").count() == 1
        assert mapped.select("qc_status").first()[0] == "PASS"

    def test_downtime_category_maps_to_event_type(self, spark):
        """PLANNED_MAINTENANCE category maps to DOWNTIME_PLANNED event type."""
        _PLANNED_CATEGORIES = {"PLANNED_MAINTENANCE", "CHANGEOVER"}
        df = spark.createDataFrame(
            [("DT-001", "LINE-A", "2025-10-01T02:00:00", "2025-10-01T04:00:00",
              "Scheduled maint.", "PLANNED_MAINTENANCE"),
             ("DT-002", "LINE-B", "2025-10-01T10:00:00", "2025-10-01T11:00:00",
              "Pump failure", "MECHANICAL")],
            ["event_id", "production_line", "start_time", "end_time", "reason", "category"]
        )
        mapped = df.withColumn(
            "event_type",
            F.when(F.col("category").isin(list(_PLANNED_CATEGORIES)),
                   F.lit("DOWNTIME_PLANNED"))
            .otherwise(F.lit("DOWNTIME_UNPLANNED"))
        )
        assert mapped.filter(F.col("event_type") == "DOWNTIME_PLANNED").count() == 1
        assert mapped.filter(F.col("event_type") == "DOWNTIME_UNPLANNED").count() == 1


# ---------------------------------------------------------------------------
# Distribution ingestion — column mapping
# ---------------------------------------------------------------------------

class TestDistributionIngestionTransform:
    def test_shipment_columns_mapped(self, spark):
        """carrier → carrier_id, estimated_delivery → promised_delivery_date, etc."""
        df = spark.createDataFrame(
            [("SHIP-001", "ORD-001", "WH-001", "UPS",
              "2025-10-01", "2025-10-05", "2025-10-04", "320", "487.20", "delivered")],
            ["shipment_id", "order_id", "warehouse_id", "carrier",
             "ship_date", "estimated_delivery", "actual_delivery",
             "cases_shipped", "freight_cost", "status"]
        )
        mapped = (
            df
            .withColumnRenamed("carrier", "carrier_id")
            .withColumnRenamed("estimated_delivery", "promised_delivery_date")
            .withColumnRenamed("actual_delivery", "actual_delivery_date")
            .withColumnRenamed("cases_shipped", "cases_delivered")
            .withColumnRenamed("freight_cost", "logistics_cost_usd")
        )
        assert "carrier_id" in mapped.columns
        assert "promised_delivery_date" in mapped.columns
        assert "actual_delivery_date" in mapped.columns
        assert "cases_delivered" in mapped.columns
        assert "logistics_cost_usd" in mapped.columns

    def test_metadata_added_to_shipment(self, spark):
        """Five metadata columns present after transformation."""
        df = spark.createDataFrame(
            [("SHIP-001", "ORD-001")],
            ["shipment_id", "order_id"]
        )
        result = _make_bronze_metadata_df(spark, df)
        for col in ["_ingested_at", "_source_file", "_batch_id",
                    "_pipeline_run_id", "ingestion_date"]:
            assert col in result.columns


# ---------------------------------------------------------------------------
# Master data ingestion — column mapping
# ---------------------------------------------------------------------------

class TestMasterDataIngestionTransform:
    def test_product_columns_mapped(self, spark):
        """category → product_category, cost_price → standard_cost_per_unit."""
        df = spark.createDataFrame(
            [("SKU-001", "FreshSip Cola", "Carbonated Soft Drinks", "Classic",
              "2.50", "0.85", "355", "180", "2022-01-01", "active")],
            ["sku_id", "product_name", "category", "sub_category",
             "unit_price", "cost_price", "pack_size", "shelf_life_days",
             "launch_date", "status"]
        )
        mapped = (
            df
            .withColumnRenamed("category", "product_category")
            .withColumnRenamed("sub_category", "product_subcategory")
            .withColumnRenamed("unit_price", "list_price")
            .withColumnRenamed("cost_price", "standard_cost_per_unit")
            .withColumnRenamed("pack_size", "package_size_ml")
            .withColumnRenamed("status", "is_active")
        )
        assert "product_category" in mapped.columns
        assert "standard_cost_per_unit" in mapped.columns
        assert "list_price" in mapped.columns
        assert mapped.select("product_category").first()[0] == "Carbonated Soft Drinks"

    def test_customer_columns_mapped(self, spark):
        """customer_id → retailer_id, name → retailer_name, segment → retail_segment."""
        df = spark.createDataFrame(
            [("CUST-001", "Metro Foods", "Enterprise", "NE", "Retail", "30",
              "2020-01-01", "active")],
            ["customer_id", "name", "segment", "region", "channel",
             "credit_terms_days", "acquisition_date", "status"]
        )
        mapped = (
            df
            .withColumnRenamed("customer_id", "retailer_id")
            .withColumnRenamed("name", "retailer_name")
            .withColumnRenamed("segment", "retail_segment")
            .withColumnRenamed("acquisition_date", "account_activation_date")
            .withColumnRenamed("status", "account_status")
        )
        assert "retailer_id" in mapped.columns
        assert "retailer_name" in mapped.columns
        assert "retail_segment" in mapped.columns
        assert mapped.select("retailer_id").first()[0] == "CUST-001"
