# Data Quality Rules — FreshSip Beverages CPG Data Platform

**Version:** 1.0
**Date:** 2026-04-05
**Author:** Data Architect Agent
**Status:** Final — Phase 3 Solutioning

---

## Severity Legend

| Severity | Code | Description | Pipeline Action |
|---|---|---|---|
| **CRITICAL** | BLOCKER | Rule failure blocks pipeline progress; records are quarantined to `slv_freshsip.dq_rejected_records`; if failure rate exceeds 5% of batch, pipeline halts and alert is sent | Quarantine record; halt pipeline if threshold exceeded |
| **WARNING** | WARN | Rule violation is flagged and logged; record passes through to Silver with a flag column or annotation; dashboard alert raised | Flag record; log to monitoring; continue pipeline |
| **INFO** | INFO | Informational metric logged for trend monitoring; no action on individual records | Log count to pipeline monitoring table |

---

## Rule ID Format

`{LAYER}-{DOMAIN}-{ENTITY}-{SEQUENCE}`

Examples: `BRZ-SALES-POS-001`, `SLV-INV-STOCK-003`, `GLD-SALES-REV-001`

---

## Bronze Layer Rules

Bronze rules focus on schema presence, completeness of metadata columns, and file-level health. No type casting validation occurs in Bronze (all values are STRING).

### Domain: All Bronze Tables

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-ALL-META-001` | All Bronze tables | `_ingested_at` | NOT_NULL | `_ingested_at IS NOT NULL` | BLOCKER | Metadata timestamp must always be set by pipeline |
| `BRZ-ALL-META-002` | All Bronze tables | `_source_file` | NOT_NULL | `_source_file IS NOT NULL AND _source_file != ''` | BLOCKER | Source file path must be present for lineage |
| `BRZ-ALL-META-003` | All Bronze tables | `_batch_id` | NOT_NULL | `_batch_id IS NOT NULL` | BLOCKER | Batch ID required for traceability |
| `BRZ-ALL-META-004` | All Bronze tables | `_pipeline_run_id` | NOT_NULL | `_pipeline_run_id IS NOT NULL` | BLOCKER | Pipeline run ID required for traceability |
| `BRZ-ALL-META-005` | All Bronze tables | `ingestion_date` | NOT_NULL | `ingestion_date IS NOT NULL` | BLOCKER | Partition key must always be populated |
| `BRZ-ALL-FRESH-001` | All Bronze tables | `_ingested_at` | TIMELINESS | `_ingested_at >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOURS)` | WARNING | Records older than 24 hours may indicate stale ingestion |
| `BRZ-ALL-DUP-001` | All Bronze tables | `_source_file` | COMPLETENESS | `COUNT(*) > 0` per batch | WARNING | Empty batch (zero records from source file) logged as missing-file alert |

### Domain: Sales — POS Transactions

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-SALES-POS-001` | `pos_transactions_raw` | `transaction_id` | NOT_NULL | `transaction_id IS NOT NULL` | BLOCKER | POS records without transaction_id cannot be processed in Silver |
| `BRZ-SALES-POS-002` | `pos_transactions_raw` | `retailer_id` | NOT_NULL | `retailer_id IS NOT NULL` | BLOCKER | Cannot attribute revenue without retailer |
| `BRZ-SALES-POS-003` | `pos_transactions_raw` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL` | BLOCKER | Cannot compute product-level KPIs |
| `BRZ-SALES-POS-004` | `pos_transactions_raw` | `unit_price` | FORMAT | `unit_price RLIKE '^[0-9]+(\.[0-9]+)?$'` | BLOCKER | unit_price must be parseable as numeric string |
| `BRZ-SALES-POS-005` | `pos_transactions_raw` | `quantity` | FORMAT | `quantity RLIKE '^[0-9]+$'` | BLOCKER | quantity must be parseable as integer string |
| `BRZ-SALES-POS-006` | `pos_transactions_raw` | `transaction_timestamp` | FORMAT | `transaction_timestamp RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'` | WARNING | Timestamp format should be ISO 8601 |
| `BRZ-SALES-POS-007` | `pos_transactions_raw` | `channel` | NOT_NULL | `channel IS NOT NULL` | WARNING | Missing channel will cause Silver rejection |

### Domain: Sales — ERP Sales

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-SALES-ERP-001` | `erp_sales_raw` | `order_id` | NOT_NULL | `order_id IS NOT NULL` | BLOCKER | Order ID is required for matching with returns |
| `BRZ-SALES-ERP-002` | `erp_sales_raw` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL` | BLOCKER | Cannot compute product KPIs |
| `BRZ-SALES-ERP-003` | `erp_sales_raw` | `invoice_price` | FORMAT | `invoice_price RLIKE '^[0-9]+(\.[0-9]+)?$'` | BLOCKER | Must be numeric |
| `BRZ-SALES-ERP-004` | `erp_sales_raw` | `order_date` | FORMAT | `order_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` | BLOCKER | Must be parseable as DATE |
| `BRZ-SALES-ERP-005` | `erp_sales_raw` | `retailer_id` | NOT_NULL | `retailer_id IS NOT NULL` | BLOCKER | Required for customer attribution |

### Domain: Sales — ERP Returns

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-SALES-RET-001` | `erp_returns_raw` | `return_id` | NOT_NULL | `return_id IS NOT NULL` | BLOCKER | Return ID required for deduplication |
| `BRZ-SALES-RET-002` | `erp_returns_raw` | `return_amount` | FORMAT | `return_amount RLIKE '^[0-9]+(\.[0-9]+)?$'` | BLOCKER | Must be numeric |
| `BRZ-SALES-RET-003` | `erp_returns_raw` | `return_date` | FORMAT | `return_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` | BLOCKER | Must be parseable as DATE |
| `BRZ-SALES-RET-004` | `erp_returns_raw` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL` | WARNING | Missing SKU will cause Silver reference integrity warning |

### Domain: Inventory

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-INV-SNAP-001` | `erp_inventory_raw` | `warehouse_id` | NOT_NULL | `warehouse_id IS NOT NULL` | BLOCKER | Cannot load stock levels without warehouse |
| `BRZ-INV-SNAP-002` | `erp_inventory_raw` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL` | BLOCKER | Cannot load stock levels without SKU |
| `BRZ-INV-SNAP-003` | `erp_inventory_raw` | `units_on_hand` | FORMAT | `units_on_hand RLIKE '^-?[0-9]+$'` | BLOCKER | Must be parseable as integer |
| `BRZ-INV-SNAP-004` | `erp_inventory_raw` | `snapshot_date` | FORMAT | `snapshot_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` | BLOCKER | Must be parseable as DATE |
| `BRZ-INV-SNAP-005` | `erp_inventory_raw` | `standard_cost_per_unit` | FORMAT | `standard_cost_per_unit RLIKE '^[0-9]+(\.[0-9]+)?$'` | WARNING | Required for inventory value calculation |

### Domain: Production (IoT)

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-PROD-IOT-001` | `iot_sensor_events_raw` | `event_id` | NOT_NULL | `event_id IS NOT NULL` | BLOCKER | Required for deduplication in Silver |
| `BRZ-PROD-IOT-002` | `iot_sensor_events_raw` | `batch_id` | NOT_NULL | `batch_id IS NOT NULL` | BLOCKER | Required to group events into batches |
| `BRZ-PROD-IOT-003` | `iot_sensor_events_raw` | `event_type` | NOT_NULL | `event_type IS NOT NULL` | BLOCKER | Cannot process events without type |
| `BRZ-PROD-IOT-004` | `iot_sensor_events_raw` | `event_timestamp` | FORMAT | `event_timestamp RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'` | WARNING | Timestamp format must be ISO 8601 |
| `BRZ-PROD-IOT-005` | `iot_sensor_events_raw` | `production_line_id` | NOT_NULL | `production_line_id IS NOT NULL` | BLOCKER | Required for production line KPIs |

### Domain: Distribution

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-DIST-SHIP-001` | `logistics_shipments_raw` | `shipment_id` | NOT_NULL | `shipment_id IS NOT NULL` | BLOCKER | Required for deduplication |
| `BRZ-DIST-SHIP-002` | `logistics_shipments_raw` | `promised_delivery_date` | FORMAT | `promised_delivery_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` | BLOCKER | Required for OTD KPI |
| `BRZ-DIST-SHIP-003` | `logistics_shipments_raw` | `logistics_cost_usd` | FORMAT | `logistics_cost_usd RLIKE '^[0-9]+(\.[0-9]+)?$'` | WARNING | Required for cost-per-case KPI |
| `BRZ-DIST-SHIP-004` | `logistics_shipments_raw` | `route_id` | NOT_NULL | `route_id IS NOT NULL` | WARNING | Required for route performance KPI |
| `BRZ-DIST-SHIP-005` | `logistics_shipments_raw` | `cases_delivered` | FORMAT | `cases_delivered RLIKE '^[0-9]+$'` | WARNING | Required for cost-per-case KPI |

### Domain: Customers

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-CUST-ERP-001` | `erp_customers_raw` | `retailer_id` | NOT_NULL | `retailer_id IS NOT NULL` | BLOCKER | Required for all customer KPIs |
| `BRZ-CUST-ERP-002` | `erp_customers_raw` | `retailer_name` | NOT_NULL | `retailer_name IS NOT NULL` | WARNING | Blank names will pass through with warning |
| `BRZ-CUST-ERP-003` | `erp_customers_raw` | `account_activation_date` | FORMAT | `account_activation_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` | WARNING | Required for CAC calculation |
| `BRZ-CUST-ERP-004` | `erp_customers_raw` | `retail_segment` | NOT_NULL | `retail_segment IS NOT NULL` | WARNING | Required for CAC segmentation |
| `BRZ-CUST-ERP-005` | `erp_customers_raw` | `record_effective_date` | FORMAT | `record_effective_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` | BLOCKER | Required for SCD Type 2 in Silver |

### Domain: Products

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `BRZ-PROD-ERP-001` | `erp_products_raw` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL` | BLOCKER | Required for all product KPIs |
| `BRZ-PROD-ERP-002` | `erp_products_raw` | `standard_cost_per_unit` | FORMAT | `standard_cost_per_unit RLIKE '^[0-9]+(\.[0-9]+)?$'` | BLOCKER | Required for gross margin KPIs |
| `BRZ-PROD-ERP-003` | `erp_products_raw` | `product_category` | NOT_NULL | `product_category IS NOT NULL` | BLOCKER | Required for all category-level KPIs |
| `BRZ-PROD-ERP-004` | `erp_products_raw` | `record_effective_date` | FORMAT | `record_effective_date RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'` | BLOCKER | Required for SCD Type 2 |
| `BRZ-PROD-ERP-005` | `erp_products_raw` | `is_active` | FORMAT | `is_active IN ('true', 'false', '1', '0', 'TRUE', 'FALSE')` | WARNING | Must be parseable as boolean |

---

## Silver Layer Rules

Silver rules enforce type validity, business logic, range constraints, and referential integrity. Records failing BLOCKER rules are quarantined.

### Domain: Sales — Transactions

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-SALES-TXN-001` | `sales_transactions` | `transaction_id` | NOT_NULL | `transaction_id IS NOT NULL` | BLOCKER | PK cannot be null |
| `SLV-SALES-TXN-002` | `sales_transactions` | `transaction_id` | UNIQUENESS | `COUNT(*) = COUNT(DISTINCT transaction_id)` | BLOCKER | Duplicate transaction IDs rejected |
| `SLV-SALES-TXN-003` | `sales_transactions` | `unit_price` | RANGE | `unit_price > 0 AND unit_price < 10000` | BLOCKER | Invalid price rejected |
| `SLV-SALES-TXN-004` | `sales_transactions` | `quantity_sold` | RANGE | `quantity_sold > 0` | BLOCKER | Non-positive quantity rejected |
| `SLV-SALES-TXN-005` | `sales_transactions` | `retailer_id` | REFERENTIAL | `retailer_id IN (SELECT retailer_id FROM slv_freshsip.customers WHERE is_current = true)` | BLOCKER | Unknown retailers rejected |
| `SLV-SALES-TXN-006` | `sales_transactions` | `sku_id` | REFERENTIAL | `sku_id IN (SELECT sku_id FROM slv_freshsip.ref_products)` | WARNING | Unknown SKUs flagged (may be new products) |
| `SLV-SALES-TXN-007` | `sales_transactions` | `channel` | FORMAT | `channel IN ('Retail', 'Wholesale', 'DTC')` | BLOCKER | Invalid channel rejected |
| `SLV-SALES-TXN-008` | `sales_transactions` | `transaction_date` | TIMELINESS | `transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAYS)` | WARNING | Transactions older than 13 months flagged as potentially stale |
| `SLV-SALES-TXN-009` | `sales_transactions` | `net_line_amount` | RANGE | `net_line_amount = unit_price * quantity_sold` | BLOCKER | Computed column integrity check |
| `SLV-SALES-TXN-010` | `sales_transactions` | `transaction_date` | NOT_NULL | `transaction_date IS NOT NULL` | BLOCKER | Partition key cannot be null |

### Domain: Sales — Returns

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-SALES-RET-001` | `sales_returns` | `return_id` | NOT_NULL | `return_id IS NOT NULL` | BLOCKER | PK cannot be null |
| `SLV-SALES-RET-002` | `sales_returns` | `return_id` | UNIQUENESS | `COUNT(*) = COUNT(DISTINCT return_id)` | BLOCKER | Duplicate returns rejected |
| `SLV-SALES-RET-003` | `sales_returns` | `quantity_returned` | RANGE | `quantity_returned > 0` | BLOCKER | Invalid quantity rejected |
| `SLV-SALES-RET-004` | `sales_returns` | `return_amount` | RANGE | `return_amount > 0` | BLOCKER | Invalid amount rejected |
| `SLV-SALES-RET-005` | `sales_returns` | `return_reason_code` | FORMAT | `return_reason_code IN ('DAMAGED', 'EXPIRED', 'WRONG_ITEM', 'QUALITY', 'OTHER')` | WARNING | Unknown reason codes flagged |
| `SLV-SALES-RET-006` | `sales_returns` | `transaction_id` | REFERENTIAL | `transaction_id IS NULL OR transaction_id IN (SELECT transaction_id FROM slv_freshsip.sales_transactions)` | WARNING | Orphaned returns flagged |

### Domain: Sales — Spend

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-SALES-SPEND-001` | `sales_spend` | `retailer_id` | NOT_NULL | `retailer_id IS NOT NULL` | BLOCKER | Required for CAC computation |
| `SLV-SALES-SPEND-002` | `sales_spend` | `trade_spend_usd` | RANGE | `trade_spend_usd >= 0` | BLOCKER | Negative spend rejected |
| `SLV-SALES-SPEND-003` | `sales_spend` | `broker_commission_usd` | RANGE | `broker_commission_usd >= 0` | BLOCKER | Negative commission rejected |
| `SLV-SALES-SPEND-004` | `sales_spend` | `field_sales_cost_usd` | RANGE | `field_sales_cost_usd >= 0` | BLOCKER | Negative cost rejected |
| `SLV-SALES-SPEND-005` | `sales_spend` | `total_acquisition_cost_usd` | RANGE | `total_acquisition_cost_usd = trade_spend_usd + broker_commission_usd + field_sales_cost_usd` | BLOCKER | Computed column integrity |
| `SLV-SALES-SPEND-006` | `sales_spend` | `period_end_date` | RANGE | `period_end_date > period_start_date` | BLOCKER | Period must have positive duration |

### Domain: Inventory — Stock

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-INV-STOCK-001` | `inventory_stock` | `warehouse_id` | NOT_NULL | `warehouse_id IS NOT NULL` | BLOCKER | Required for all inventory KPIs |
| `SLV-INV-STOCK-002` | `inventory_stock` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL` | BLOCKER | Required for product-level stock KPIs |
| `SLV-INV-STOCK-003` | `inventory_stock` | `units_on_hand` | RANGE | `units_on_hand >= 0` | BLOCKER | Negative stock rejected with reason INVALID_STOCK_LEVEL |
| `SLV-INV-STOCK-004` | `inventory_stock` | `standard_cost_per_unit` | RANGE | `standard_cost_per_unit > 0` | BLOCKER | Required for inventory value calculation |
| `SLV-INV-STOCK-005` | `inventory_stock` | `inventory_value` | RANGE | `inventory_value = units_on_hand * standard_cost_per_unit` | BLOCKER | Computed column integrity |
| `SLV-INV-STOCK-006` | `inventory_stock` | `(sku_id, warehouse_id, snapshot_date)` | UNIQUENESS | No duplicate (sku_id, warehouse_id, snapshot_date) | BLOCKER | Each SKU/warehouse/date must be unique |
| `SLV-INV-STOCK-007` | `inventory_stock` | `sku_id` | REFERENTIAL | `sku_id IN (SELECT sku_id FROM slv_freshsip.ref_products)` | WARNING | Unknown SKUs flagged |
| `SLV-INV-STOCK-008` | `inventory_stock` | `units_in_transit` | RANGE | `units_in_transit >= 0` | BLOCKER | Cannot be negative |

### Domain: Inventory — Reorder Points

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-INV-ROP-001` | `ref_reorder_points` | `(sku_id, warehouse_id)` | UNIQUENESS | No duplicate (sku_id, warehouse_id) | BLOCKER | Reference table must have unique keys |
| `SLV-INV-ROP-002` | `ref_reorder_points` | `reorder_point_units` | RANGE | `reorder_point_units > 0` | BLOCKER | Reorder point must be positive |
| `SLV-INV-ROP-003` | `ref_reorder_points` | `safety_stock_units` | RANGE | `safety_stock_units >= 0 AND safety_stock_units < reorder_point_units` | WARNING | Safety stock should be below reorder point |
| `SLV-INV-ROP-004` | `ref_reorder_points` | COMPLETENESS | All (sku_id, warehouse_id) in inventory_stock covered | `COUNT(DISTINCT CONCAT(sku_id, '_', warehouse_id)) IN ref_reorder_points >= COUNT in inventory_stock` | WARNING | Missing reorder points for active SKU/warehouse pairs |
| `SLV-INV-ROP-005` | `ref_reorder_points` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL AND warehouse_id IS NOT NULL` | BLOCKER | Both key parts required |

### Domain: Production — Batches

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-PROD-BATCH-001` | `production_batches` | `batch_id` | NOT_NULL | `batch_id IS NOT NULL` | BLOCKER | PK cannot be null |
| `SLV-PROD-BATCH-002` | `production_batches` | `batch_id` | UNIQUENESS | `COUNT(*) = COUNT(DISTINCT batch_id)` | BLOCKER | Duplicate batch IDs rejected |
| `SLV-PROD-BATCH-003` | `production_batches` | `expected_output_cases` | RANGE | `expected_output_cases > 0` | BLOCKER | Expected output must be positive |
| `SLV-PROD-BATCH-004` | `production_batches` | `yield_rate_pct` | RANGE | `yield_rate_pct IS NULL OR (yield_rate_pct >= 0 AND yield_rate_pct <= 110)` | BLOCKER if < 0; WARNING if > 105 | Yield rate must be in valid range |
| `SLV-PROD-BATCH-005` | `production_batches` | `qc_status` | FORMAT | `qc_status IS NULL OR qc_status IN ('PASS', 'FAIL', 'PENDING')` | BLOCKER | Unknown QC status rejected |
| `SLV-PROD-BATCH-006` | `production_batches` | `batch_end_ts` | RANGE | `batch_end_ts IS NULL OR batch_end_ts > batch_start_ts` | BLOCKER | End must be after start |
| `SLV-PROD-BATCH-007` | `production_batches` | `sku_id` | REFERENTIAL | `sku_id IN (SELECT sku_id FROM slv_freshsip.ref_products)` | WARNING | Unknown SKUs flagged |
| `SLV-PROD-BATCH-008` | `production_batches` | `production_line_id` | NOT_NULL | `production_line_id IS NOT NULL` | BLOCKER | Required for line-level KPIs |

### Domain: Production — Events

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-PROD-EVT-001` | `production_events` | `event_id` | NOT_NULL | `event_id IS NOT NULL` | BLOCKER | PK cannot be null |
| `SLV-PROD-EVT-002` | `production_events` | `event_id` | UNIQUENESS | `COUNT(*) = COUNT(DISTINCT event_id)` | BLOCKER | Duplicate events rejected |
| `SLV-PROD-EVT-003` | `production_events` | `event_type` | FORMAT | `event_type IN ('BATCH_START','BATCH_END','QC_CHECK','DOWNTIME_UNPLANNED','DOWNTIME_PLANNED')` | BLOCKER | Unknown event types rejected |
| `SLV-PROD-EVT-004` | `production_events` | `downtime_hours` | RANGE | `downtime_hours IS NULL OR downtime_hours >= 0` | BLOCKER | Negative downtime rejected |
| `SLV-PROD-EVT-005` | `production_events` | `downtime_end_ts` | RANGE | `downtime_end_ts IS NULL OR downtime_end_ts > downtime_start_ts` | BLOCKER | End must be after start |
| `SLV-PROD-EVT-006` | `production_events` | `sensor_temperature` | RANGE | `sensor_temperature IS NULL OR (sensor_temperature BETWEEN -10 AND 150)` | WARNING | Out-of-range temperature flagged |
| `SLV-PROD-EVT-007` | `production_events` | `batch_id` | REFERENTIAL | `batch_id IN (SELECT batch_id FROM slv_freshsip.production_batches)` | WARNING | Orphaned events flagged |

### Domain: Distribution — Shipments

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-DIST-SHIP-001` | `shipments` | `shipment_id` | NOT_NULL | `shipment_id IS NOT NULL` | BLOCKER | PK cannot be null |
| `SLV-DIST-SHIP-002` | `shipments` | `shipment_id` | UNIQUENESS | `COUNT(*) = COUNT(DISTINCT shipment_id)` | BLOCKER | Duplicate shipments rejected |
| `SLV-DIST-SHIP-003` | `shipments` | `promised_delivery_date` | RANGE | `promised_delivery_date >= ship_date` | BLOCKER | Promise cannot be before ship |
| `SLV-DIST-SHIP-004` | `shipments` | `logistics_cost_usd` | RANGE | `logistics_cost_usd >= 0` | BLOCKER | Negative cost rejected |
| `SLV-DIST-SHIP-005` | `shipments` | `cases_delivered` | RANGE | `cases_delivered >= 0` | BLOCKER | Negative cases rejected |
| `SLV-DIST-SHIP-006` | `shipments` | `channel` | FORMAT | `channel IN ('Retail', 'Wholesale', 'DTC')` | BLOCKER | Invalid channel rejected |
| `SLV-DIST-SHIP-007` | `shipments` | `actual_delivery_date` | RANGE | `actual_delivery_date IS NULL OR actual_delivery_date >= ship_date` | BLOCKER | Actual delivery before ship date is invalid |
| `SLV-DIST-SHIP-008` | `shipments` | `quantity_ordered` | RANGE | `quantity_ordered > 0` | BLOCKER | Must have positive ordered quantity |
| `SLV-DIST-SHIP-009` | `shipments` | `retailer_id` | REFERENTIAL | `retailer_id IN (SELECT retailer_id FROM slv_freshsip.customers WHERE is_current = true)` | WARNING | Unknown retailers flagged |

### Domain: Customers (SCD Type 2)

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-CUST-001` | `customers` | `retailer_id` | NOT_NULL | `retailer_id IS NOT NULL` | BLOCKER | Business key cannot be null |
| `SLV-CUST-002` | `customers` | `retailer_name` | NOT_NULL | `retailer_name IS NOT NULL` | BLOCKER | Name required |
| `SLV-CUST-003` | `customers` | `account_status` | FORMAT | `account_status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED')` | BLOCKER | Invalid status rejected |
| `SLV-CUST-004` | `customers` | `valid_to` | RANGE | `valid_to IS NULL OR valid_to > valid_from` | BLOCKER | SCD validity window must be positive |
| `SLV-CUST-005` | `customers` | `is_current` | UNIQUENESS | `SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) = 1 per retailer_id` | BLOCKER | Exactly one current record per retailer |
| `SLV-CUST-006` | `customers` | `credit_terms_days` | RANGE | `credit_terms_days IN (15, 30, 45, 60, 90)` | WARNING | Non-standard payment terms flagged |
| `SLV-CUST-007` | `customers` | `account_activation_date` | RANGE | `account_activation_date <= CURRENT_DATE()` | WARNING | Future activation dates flagged |

### Domain: Products (SCD Type 2)

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `SLV-PROD-SKU-001` | `products` | `sku_id` | NOT_NULL | `sku_id IS NOT NULL` | BLOCKER | Business key cannot be null |
| `SLV-PROD-SKU-002` | `products` | `standard_cost_per_unit` | RANGE | `standard_cost_per_unit > 0` | BLOCKER | Cost must be positive for margin calculations |
| `SLV-PROD-SKU-003` | `products` | `product_category` | FORMAT | `product_category IN ('Carbonated Soft Drinks', 'Flavored Water', 'Energy Drinks', 'Juice Blends')` | BLOCKER | Invalid category rejected |
| `SLV-PROD-SKU-004` | `products` | `list_price` | RANGE | `list_price > standard_cost_per_unit` | WARNING | List price below standard cost is a data anomaly |
| `SLV-PROD-SKU-005` | `products` | `is_current` | UNIQUENESS | `SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) = 1 per sku_id` | BLOCKER | Exactly one current record per SKU |
| `SLV-PROD-SKU-006` | `products` | `valid_to` | RANGE | `valid_to IS NULL OR valid_to > valid_from` | BLOCKER | SCD validity window must be positive |
| `SLV-PROD-SKU-007` | `products` | `price_tier` | FORMAT | `price_tier IN ('Economy', 'Standard', 'Premium')` | BLOCKER | Invalid tier rejected |

---

## Gold Layer Rules

Gold rules verify KPI output correctness, freshness, and referential consistency to dimension tables.

### Domain: Sales KPIs

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `GLD-SALES-REV-001` | `sales_daily_revenue` | `net_revenue` | NOT_NULL | `net_revenue IS NOT NULL` | BLOCKER | Core KPI value cannot be null |
| `GLD-SALES-REV-002` | `sales_daily_revenue` | `net_revenue` | RANGE | `net_revenue >= 0` | BLOCKER | Revenue cannot be negative (returns capped at gross sales) |
| `GLD-SALES-REV-003` | `sales_daily_revenue` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOURS)` | BLOCKER | Hourly SLA: Gold table must be refreshed within 2 hours |
| `GLD-SALES-REV-004` | `sales_daily_revenue` | `(report_date, product_category, region, channel)` | UNIQUENESS | No duplicate grain keys | BLOCKER | Each daily grain must be unique |
| `GLD-SALES-MOM-001` | `sales_period_comparison` | `mom_pct_change` | RANGE | `mom_pct_change IS NULL OR (mom_pct_change BETWEEN -100 AND 500)` | WARNING | Extreme MoM values flagged for review |
| `GLD-SALES-MOM-002` | `sales_period_comparison` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOURS)` | BLOCKER | Daily SLA must be met |
| `GLD-SALES-MOM-003` | `sales_period_comparison` | `current_month_revenue` | NOT_NULL | `current_month_revenue IS NOT NULL` | BLOCKER | Current period revenue required |
| `GLD-SALES-MARG-001` | `sales_gross_margin_sku` | `gross_margin_pct` | RANGE | `gross_margin_pct IS NULL OR (gross_margin_pct BETWEEN -100 AND 100)` | WARNING | Extreme margins flagged |
| `GLD-SALES-MARG-002` | `sales_gross_margin_sku` | `net_revenue` | NOT_NULL | `net_revenue IS NOT NULL` | BLOCKER | Core KPI value required |
| `GLD-SALES-MARG-003` | `sales_gross_margin_sku` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOURS)` | BLOCKER | Daily SLA must be met |

### Domain: Inventory KPIs

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `GLD-INV-STOCK-001` | `inventory_stock_levels` | `units_on_hand` | NOT_NULL | `units_on_hand IS NOT NULL` | BLOCKER | Core stock level cannot be null |
| `GLD-INV-STOCK-002` | `inventory_stock_levels` | `units_on_hand` | RANGE | `units_on_hand >= 0` | BLOCKER | Negative stock is invalid in Gold |
| `GLD-INV-STOCK-003` | `inventory_stock_levels` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOURS)` | BLOCKER | Hourly SLA for inventory KPI |
| `GLD-INV-STOCK-004` | `inventory_stock_levels` | `reorder_alert_flag` | NOT_NULL | `reorder_alert_flag IS NOT NULL` | BLOCKER | Alert flag must always be set |
| `GLD-INV-TURN-001` | `inventory_turnover` | `inventory_turnover_rate` | RANGE | `inventory_turnover_rate IS NULL OR inventory_turnover_rate >= 0` | BLOCKER | Rate cannot be negative |
| `GLD-INV-TURN-002` | `inventory_turnover` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAYS)` | BLOCKER | Weekly SLA must be met |
| `GLD-INV-TURN-003` | `inventory_turnover` | `cogs_30d` | NOT_NULL | `cogs_30d IS NOT NULL` | BLOCKER | Source COGS required |
| `GLD-INV-DSI-001` | `inventory_dsi` | `dsi_days` | RANGE | `dsi_days IS NULL OR (dsi_days >= 0 AND dsi_days <= 365)` | WARNING | DSI > 365 is unrealistic |
| `GLD-INV-DSI-002` | `inventory_dsi` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOURS)` | BLOCKER | Daily SLA must be met |
| `GLD-INV-DSI-003` | `inventory_dsi` | `units_on_hand` | NOT_NULL | `units_on_hand IS NOT NULL` | BLOCKER | Stock level required for DSI |

### Domain: Production KPIs

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `GLD-PROD-YIELD-001` | `production_yield` | `yield_rate_pct` | RANGE | `yield_rate_pct IS NULL OR (yield_rate_pct >= 0 AND yield_rate_pct <= 110)` | BLOCKER if < 0; WARNING if > 105 | Yield must be in valid range |
| `GLD-PROD-YIELD-002` | `production_yield` | `batch_id` | UNIQUENESS | `COUNT(*) = COUNT(DISTINCT batch_id)` | BLOCKER | One row per batch |
| `GLD-PROD-YIELD-003` | `production_yield` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTES)` | BLOCKER | Micro-batch SLA: must refresh within 15 minutes |
| `GLD-PROD-QC-001` | `production_quality` | `qc_pass_rate_pct` | RANGE | `qc_pass_rate_pct IS NULL OR (qc_pass_rate_pct BETWEEN 0 AND 100)` | BLOCKER | Rate must be 0-100 |
| `GLD-PROD-QC-002` | `production_quality` | `total_batches` | RANGE | `total_batches >= 0` | BLOCKER | Cannot be negative |
| `GLD-PROD-QC-003` | `production_quality` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOURS)` | BLOCKER | Daily SLA |
| `GLD-PROD-DT-001` | `production_downtime` | `downtime_hours` | RANGE | `downtime_hours >= 0` | BLOCKER | Cannot be negative |
| `GLD-PROD-DT-002` | `production_downtime` | `downtime_hours` | RANGE | `downtime_hours <= 24` | WARNING | More than 24 hours in a day is suspicious |
| `GLD-PROD-DT-003` | `production_downtime` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOURS)` | BLOCKER | Daily SLA |

### Domain: Distribution KPIs

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `GLD-DIST-OTD-001` | `distribution_otd` | `otd_pct` | RANGE | `otd_pct IS NULL OR (otd_pct BETWEEN 0 AND 100)` | BLOCKER | OTD% must be 0-100 |
| `GLD-DIST-OTD-002` | `distribution_otd` | `on_time_shipments` | RANGE | `on_time_shipments <= total_shipments` | BLOCKER | On-time count cannot exceed total |
| `GLD-DIST-OTD-003` | `distribution_otd` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 13 HOURS)` | BLOCKER | 12-hour max SLA for distribution |
| `GLD-DIST-FULL-001` | `distribution_fulfillment` | `fulfillment_rate_pct` | RANGE | `fulfillment_rate_pct IS NULL OR (fulfillment_rate_pct BETWEEN 0 AND 100)` | BLOCKER | Rate must be 0-100 |
| `GLD-DIST-FULL-002` | `distribution_fulfillment` | `fully_shipped_orders` | RANGE | `fully_shipped_orders <= total_orders` | BLOCKER | Cannot exceed total |
| `GLD-DIST-FULL-003` | `distribution_fulfillment` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 13 HOURS)` | BLOCKER | 12-hour max SLA |
| `GLD-DIST-COST-001` | `distribution_cost` | `cost_per_case` | RANGE | `cost_per_case IS NULL OR (cost_per_case > 0 AND cost_per_case < 100)` | WARNING | Extreme cost per case flagged |
| `GLD-DIST-COST-002` | `distribution_cost` | `total_cases_delivered` | RANGE | `total_cases_delivered >= 0` | BLOCKER | Cannot be negative |
| `GLD-DIST-COST-003` | `distribution_cost` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAYS)` | BLOCKER | Weekly SLA |

### Domain: Customer KPIs

| Rule ID | Table | Column | Rule Type | Expression | Severity | Description |
|---|---|---|---|---|---|---|
| `GLD-CUST-TOP-001` | `customers_top_retailers` | `revenue_rank` | RANGE | `revenue_rank BETWEEN 1 AND 20` | BLOCKER | Only top 20 should be in table |
| `GLD-CUST-TOP-002` | `customers_top_retailers` | `retailer_net_revenue` | RANGE | `retailer_net_revenue >= 0` | BLOCKER | Revenue cannot be negative |
| `GLD-CUST-TOP-003` | `customers_top_retailers` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAYS)` | BLOCKER | Weekly SLA |
| `GLD-CUST-CAC-001` | `customers_cac` | `cac_usd` | RANGE | `cac_usd IS NULL OR (cac_usd >= 0 AND cac_usd < 100000)` | WARNING | Extreme CAC flagged |
| `GLD-CUST-CAC-002` | `customers_cac` | `new_accounts_count` | RANGE | `new_accounts_count >= 0` | BLOCKER | Cannot be negative |
| `GLD-CUST-CAC-003` | `customers_cac` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAYS)` | BLOCKER | Monthly SLA |
| `GLD-CUST-RET-001` | `customers_retention` | `retention_rate_pct` | RANGE | `retention_rate_pct IS NULL OR (retention_rate_pct BETWEEN 0 AND 100)` | BLOCKER | Rate must be 0-100 |
| `GLD-CUST-RET-002` | `customers_retention` | `retained_count` | RANGE | `retained_count <= prior_period_active_count` | BLOCKER | Retained cannot exceed prior active |
| `GLD-CUST-RET-003` | `customers_retention` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAYS)` | BLOCKER | Monthly SLA |
| `GLD-CUST-CONC-001` | `customers_concentration_risk` | `top5_concentration_pct` | RANGE | `top5_concentration_pct IS NULL OR (top5_concentration_pct BETWEEN 0 AND 100)` | BLOCKER | Concentration % must be 0-100 |
| `GLD-CUST-CONC-002` | `customers_concentration_risk` | `top5_net_revenue` | RANGE | `top5_net_revenue <= total_net_revenue` | BLOCKER | Top 5 cannot exceed total |
| `GLD-CUST-CONC-003` | `customers_concentration_risk` | `last_updated_ts` | TIMELINESS | `last_updated_ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAYS)` | BLOCKER | Monthly SLA |

---

## Pipeline Halt Thresholds

These thresholds determine when a BLOCKER failure rate should halt the pipeline rather than merely quarantine individual records:

| Layer | Domain | Max Failure Rate Before Halt | Rationale |
|---|---|---|---|
| Silver | Sales Transactions | 5% per batch | High-volume; occasional bad records acceptable |
| Silver | Inventory Stock | 1% per batch | Low-volume; near-zero failures expected in ERP data |
| Silver | Production Batches | 2% per batch | IoT data may have sensor anomalies |
| Silver | Shipments | 2% per batch | Logistics partner data quality variable |
| Silver | Customers | 0.5% per batch | Small reference table; any failures are significant |
| Silver | Products | 0.5% per batch | Small reference table; any failures are significant |
| Gold | All KPI tables | 0% (any failure = halt) | Gold failures block dashboard; zero tolerance |

---

## Monitoring Table: `slv_freshsip.pipeline_monitoring`

All pipelines write a summary record to this table on each run. Dashboard alerts reference this table.

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | Databricks job run ID |
| `pipeline_name` | STRING | Job or pipeline name |
| `source_table` | STRING | Source table |
| `target_table` | STRING | Target table |
| `run_timestamp` | TIMESTAMP | When the run started |
| `completed_timestamp` | TIMESTAMP | When the run completed |
| `rows_read` | BIGINT | Records read from source |
| `rows_written` | BIGINT | Records written to target |
| `rows_rejected` | BIGINT | Records rejected to DQ log |
| `rejection_rate_pct` | DECIMAL(6,4) | `rows_rejected / rows_read * 100` |
| `pipeline_status` | STRING | COMPLETED, FAILED, PARTIAL |
| `alert_triggered` | BOOLEAN | Whether a DQ alert was raised |
