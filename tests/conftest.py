"""
Shared pytest fixtures for FreshSip Beverages CPG Data Platform unit tests.

Provides a session-scoped SparkSession and sample DataFrames for each domain,
using inline Python data (no real Delta tables touched).
"""

import datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType, DateType,
    TimestampType, BooleanType, LongType,
)


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for all unit tests. Runs in local mode."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("FreshSip_Unit_Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Bronze sample DataFrames
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_bronze_pos_df(spark):
    """Sample Bronze pos_transactions_raw rows (all STRING + metadata columns)."""
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("retailer_id",    StringType(), True),
        StructField("sku_id",         StringType(), True),
        StructField("quantity",       StringType(), True),
        StructField("unit_price",     StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("channel",        StringType(), True),
        StructField("region",         StringType(), True),
        StructField("state",          StringType(), True),
        StructField("_ingested_at",   TimestampType(), False),
        StructField("_source_file",   StringType(), False),
        StructField("_batch_id",      StringType(), False),
        StructField("_pipeline_run_id", StringType(), False),
        StructField("ingestion_date", DateType(), False),
    ])
    today = datetime.date.today()
    now   = datetime.datetime.now()
    data  = [
        ("POS-001", "CUST-001", "SKU-001", "3",  "3.50", "2025-10-01T10:00:00",
         "Retail", "NE", "NY", now, "/mnt/pos/2025.json", "batch-1", "run-1", today),
        ("POS-002", "CUST-002", "SKU-002", "5",  "2.99", "2025-10-01T11:00:00",
         "Wholesale", "SE", "FL", now, "/mnt/pos/2025.json", "batch-1", "run-1", today),
        ("POS-003", "CUST-003", "SKU-003", "2",  "4.25", "2025-10-02T09:00:00",
         "DTC", "MW", "IL", now, "/mnt/pos/2025.json", "batch-1", "run-1", today),
        # Null transaction_id — should fail DQ
        (None,      "CUST-004", "SKU-004", "1",  "1.99", "2025-10-02T12:00:00",
         "Retail", "W", "CA", now, "/mnt/pos/2025.json", "batch-1", "run-1", today),
        # Null sku_id — should fail DQ
        ("POS-005", "CUST-005", None,       "4",  "3.00", "2025-10-03T08:00:00",
         "Retail", "NE", "MA", now, "/mnt/pos/2025.json", "batch-1", "run-1", today),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_bronze_inventory_df(spark):
    """Sample Bronze erp_inventory_raw rows."""
    schema = StructType([
        StructField("warehouse_id",         StringType(), True),
        StructField("sku_id",               StringType(), True),
        StructField("units_on_hand",        StringType(), True),
        StructField("units_in_transit",     StringType(), True),
        StructField("units_reserved",       StringType(), True),
        StructField("snapshot_date",        StringType(), True),
        StructField("reorder_point_units",  StringType(), True),
        StructField("standard_cost_per_unit", StringType(), True),
        StructField("_ingested_at",         TimestampType(), False),
        StructField("_source_file",         StringType(), False),
        StructField("_batch_id",            StringType(), False),
        StructField("_pipeline_run_id",     StringType(), False),
        StructField("ingestion_date",       DateType(), False),
    ])
    today = datetime.date.today()
    now   = datetime.datetime.now()
    data  = [
        ("WH-001", "SKU-001", "5000", "200", "0", "2025-10-01", "600", "1.50",
         now, "/mnt/inv.csv", "batch-1", "run-1", today),
        ("WH-001", "SKU-002", "2000", "100", "0", "2025-10-01", "400", "2.00",
         now, "/mnt/inv.csv", "batch-1", "run-1", today),
        ("WH-002", "SKU-001", "300",  "500", "0", "2025-10-01", "600", "1.50",
         now, "/mnt/inv.csv", "batch-1", "run-1", today),
        # Null warehouse — should fail DQ
        (None,      "SKU-003", "1000", "0",  "0", "2025-10-01", "200", "3.00",
         now, "/mnt/inv.csv", "batch-1", "run-1", today),
    ]
    return spark.createDataFrame(data, schema=schema)


# ---------------------------------------------------------------------------
# Silver sample DataFrames
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_silver_transactions_df(spark):
    """Sample Silver sales_transactions rows (typed)."""
    schema = StructType([
        StructField("transaction_key",     LongType(),        False),
        StructField("transaction_id",      StringType(),      False),
        StructField("retailer_id",         StringType(),      True),
        StructField("sku_id",              StringType(),      True),
        StructField("unit_price",          DecimalType(10,2), True),
        StructField("quantity_sold",       IntegerType(),     True),
        StructField("net_line_amount",     DecimalType(12,2), True),
        StructField("transaction_date",    DateType(),        True),
        StructField("channel",             StringType(),      True),
        StructField("region",              StringType(),      True),
    ])
    data = [
        (1001, "POS-001", "CUST-001", "SKU-001",
         3.50, 3, 10.50, datetime.date(2025, 10, 1), "Retail", "NE"),
        (1002, "POS-002", "CUST-002", "SKU-002",
         2.99, 5, 14.95, datetime.date(2025, 10, 1), "Wholesale", "SE"),
        (1003, "POS-003", "CUST-003", "SKU-003",
         4.25, 2,  8.50, datetime.date(2025, 10, 2), "DTC", "MW"),
    ]
    from decimal import Decimal
    typed = [(r[0], r[1], r[2], r[3],
              Decimal(str(r[4])), r[5], Decimal(str(r[6])),
              r[7], r[8], r[9]) for r in data]
    return spark.createDataFrame(typed, schema=schema)


@pytest.fixture
def sample_silver_inventory_df(spark):
    """Sample Silver inventory_stock rows (typed)."""
    schema = StructType([
        StructField("stock_key",              LongType(),        False),
        StructField("warehouse_id",           StringType(),      False),
        StructField("sku_id",                 StringType(),      False),
        StructField("units_on_hand",          IntegerType(),     True),
        StructField("units_in_transit",       IntegerType(),     True),
        StructField("units_reserved",         IntegerType(),     True),
        StructField("snapshot_date",          DateType(),        True),
        StructField("standard_cost_per_unit", DecimalType(10,2), True),
        StructField("inventory_value",        DecimalType(14,2), True),
        StructField("dsi_days",               DecimalType(8,1),  True),
    ])
    from decimal import Decimal
    data = [
        (101, "WH-001", "SKU-001", 5000, 200, 0,
         datetime.date(2025, 10, 1), Decimal("1.50"), Decimal("7500.00"), Decimal("175.0")),
        (102, "WH-001", "SKU-002", 2000, 100, 0,
         datetime.date(2025, 10, 1), Decimal("2.00"), Decimal("4000.00"), Decimal("140.0")),
        (103, "WH-002", "SKU-001",  300, 500, 0,
         datetime.date(2025, 10, 1), Decimal("1.50"), Decimal("450.00"),  Decimal("3.0")),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_silver_batches_df(spark):
    """Sample Silver production_batches rows (typed)."""
    schema = StructType([
        StructField("batch_key",            LongType(),       False),
        StructField("batch_id",             StringType(),     False),
        StructField("production_line_id",   StringType(),     True),
        StructField("sku_id",               StringType(),     True),
        StructField("expected_output_cases",IntegerType(),    True),
        StructField("actual_output_cases",  IntegerType(),    True),
        StructField("yield_rate_pct",       DecimalType(5,2), True),
        StructField("qc_status",            StringType(),     True),
        StructField("qc_pass_flag",         BooleanType(),    True),
        StructField("batch_date",           DateType(),       True),
    ])
    from decimal import Decimal
    data = [
        (201, "BATCH-001", "LINE-A", "SKU-001", 3000, 2900, Decimal("96.67"), "PASS", True,
         datetime.date(2025, 10, 1)),
        (202, "BATCH-002", "LINE-B", "SKU-002", 2000, 1700, Decimal("85.00"), "FAIL", False,
         datetime.date(2025, 10, 1)),
        (203, "BATCH-003", "LINE-A", "SKU-003", 4000, 3900, Decimal("97.50"), "PASS", True,
         datetime.date(2025, 10, 2)),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_silver_shipments_df(spark):
    """Sample Silver shipments rows (typed)."""
    schema = StructType([
        StructField("shipment_key",           LongType(),       False),
        StructField("shipment_id",            StringType(),     False),
        StructField("order_id",               StringType(),     True),
        StructField("retailer_id",            StringType(),     True),
        StructField("warehouse_id",           StringType(),     True),
        StructField("channel",                StringType(),     True),
        StructField("region",                 StringType(),     True),
        StructField("ship_date",              DateType(),       True),
        StructField("promised_delivery_date", DateType(),       True),
        StructField("actual_delivery_date",   DateType(),       True),
        StructField("on_time_flag",           BooleanType(),    True),
        StructField("cases_delivered",        IntegerType(),    True),
        StructField("logistics_cost_usd",     DecimalType(10,2),True),
        StructField("is_fully_shipped",       BooleanType(),    True),
    ])
    from decimal import Decimal
    data = [
        (301, "SHIP-001", "ORD-001", "CUST-001", "WH-001", "Retail", "NE",
         datetime.date(2025, 10, 1), datetime.date(2025, 10, 5),
         datetime.date(2025, 10, 4), True, 200, Decimal("320.00"), True),
        (302, "SHIP-002", "ORD-002", "CUST-002", "WH-002", "Wholesale", "SE",
         datetime.date(2025, 10, 2), datetime.date(2025, 10, 6),
         datetime.date(2025, 10, 8), False, 150, Decimal("280.00"), True),
        # In-transit — no actual_delivery → on_time_flag = False
        (303, "SHIP-003", "ORD-003", "CUST-003", "WH-001", "DTC", "MW",
         datetime.date(2025, 10, 3), datetime.date(2025, 10, 7),
         None, False, 100, Decimal("190.00"), False),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_silver_products_df(spark):
    """Sample Silver ref_products rows (typed)."""
    from decimal import Decimal
    data = [
        ("SKU-001", "FreshSip Cola 355ml", "Carbonated Soft Drinks", "Classic",
         "", "Can", 355, Decimal("0.85"), Decimal("2.50"), "Standard", True),
        ("SKU-002", "AquaFlow Lemon 500ml", "Flavored Water", "Citrus",
         "", "Bottle", 500, Decimal("0.60"), Decimal("1.99"), "Economy", True),
        ("SKU-003", "VoltBoost Energy 250ml", "Energy Drinks", "Classic",
         "", "Can", 250, Decimal("1.20"), Decimal("3.99"), "Premium", True),
    ]
    schema = StructType([
        StructField("sku_id",                StringType(),      False),
        StructField("product_name",          StringType(),      True),
        StructField("product_category",      StringType(),      True),
        StructField("product_subcategory",   StringType(),      True),
        StructField("brand",                 StringType(),      True),
        StructField("packaging_type",        StringType(),      True),
        StructField("package_size_ml",       IntegerType(),     True),
        StructField("standard_cost_per_unit",DecimalType(10,2), True),
        StructField("list_price",            DecimalType(10,2), True),
        StructField("price_tier",            StringType(),      True),
        StructField("is_active",             BooleanType(),     True),
    ])
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def sample_silver_customers_df(spark):
    """Sample Silver customers rows (SCD Type 2, current records)."""
    from decimal import Decimal
    schema = StructType([
        StructField("surrogate_key",          LongType(),       False),
        StructField("retailer_id",            StringType(),     False),
        StructField("retailer_name",          StringType(),     True),
        StructField("retail_segment",         StringType(),     True),
        StructField("channel",                StringType(),     True),
        StructField("region",                 StringType(),     True),
        StructField("state",                  StringType(),     True),
        StructField("city",                   StringType(),     True),
        StructField("credit_terms_days",      IntegerType(),    True),
        StructField("account_activation_date",DateType(),       True),
        StructField("account_status",         StringType(),     True),
        StructField("valid_from",             DateType(),       True),
        StructField("valid_to",               DateType(),       True),
        StructField("is_current",             BooleanType(),    False),
    ])
    data = [
        (401, "CUST-001", "Metro Foods 1",   "Enterprise", "Retail",    "NE", "NY", "New York",
         30, datetime.date(2020, 1, 1), "active",
         datetime.date(2020, 1, 1), None, True),
        (402, "CUST-002", "Regional Mart 2", "Mid-Market",  "Wholesale", "SE", "FL", "Miami",
         60, datetime.date(2021, 3, 1), "active",
         datetime.date(2021, 3, 1), None, True),
        (403, "CUST-003", "Corner Shop 3",   "SMB",         "DTC",       "MW", "IL", "Chicago",
         15, datetime.date(2022, 6, 1), "inactive",
         datetime.date(2022, 6, 1), None, True),
    ]
    return spark.createDataFrame(data, schema=schema)
