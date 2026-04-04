# Data Lineage — FreshSip Beverages CPG Data Platform

**Version:** 1.0
**Date:** 2026-04-05
**Author:** Data Architect Agent
**Status:** Final — Phase 3 Solutioning

---

## Overview

This document defines end-to-end data lineage for all five domains of the FreshSip CPG Data Platform. Each section provides:

1. A Mermaid flowchart diagram showing source → Bronze → Silver → Gold flow
2. A lineage table with transformation details and quality gates
3. Column-level lineage for key KPI computations

---

## Domain 1: Sales

### Sales Lineage Diagram

```mermaid
flowchart LR
    subgraph SOURCES_S["Source Systems"]
        POS["POS Transactions\nJSON / Hourly\n/mnt/landing/pos/"]
        ERP_S["ERP Sales Orders\nCSV / Daily\n/mnt/landing/erp/sales/"]
        ERP_R["ERP Returns\nCSV / Daily\n/mnt/landing/erp/returns/"]
    end

    subgraph BRONZE_S["Bronze — brz_freshsip"]
        B_POS["pos_transactions_raw\n- All columns as STRING\n- Append-only\n- Partition: ingestion_date"]
        B_ERP_S["erp_sales_raw\n- All columns as STRING\n- Append-only\n- Partition: ingestion_date"]
        B_ERP_R["erp_returns_raw\n- All columns as STRING\n- Append-only\n- Partition: ingestion_date"]
    end

    subgraph SILVER_S["Silver — slv_freshsip"]
        SV_TXN["sales_transactions\n- Typed columns\n- Dedup on transaction_id\n- MERGE upsert\n- Partition: transaction_date"]
        SV_RET["sales_returns\n- Typed columns\n- Dedup on return_id\n- Partition: return_date"]
        SV_SPEND["sales_spend\n- Spend per retailer/period\n- For CAC calculation"]
        SV_REF_P["ref_products\n- Current product attributes\n- standard_cost_per_unit\n- SCD Type 1 overwrite"]
    end

    subgraph GOLD_S["Gold — gld_freshsip"]
        G_DR["sales_daily_revenue\nKPI-S01\nSUM(unit_price x qty) - SUM(return_amount)\nGroup by date, category, region, channel"]
        G_PC["sales_period_comparison\nKPI-S02 (MoM%) + KPI-S03 (YoY%)\nRolls up sales_daily_revenue"]
        G_GM["sales_gross_margin_sku\nKPI-S04\n(net_revenue - cogs) / net_revenue * 100\nGroup by week, sku_id"]
        G_TR["customers_top_retailers\nKPI-C01\nRanked SUM(net_revenue) per retailer"]
    end

    POS --> B_POS
    ERP_S --> B_ERP_S
    ERP_R --> B_ERP_R

    B_POS --> SV_TXN
    B_ERP_S --> SV_TXN
    B_ERP_R --> SV_RET
    B_ERP_R --> SV_SPEND

    SV_TXN --> G_DR
    SV_RET --> G_DR
    SV_REF_P --> G_DR

    G_DR --> G_PC
    SV_TXN --> G_GM
    SV_RET --> G_GM
    SV_REF_P --> G_GM

    SV_TXN --> G_TR
    SV_RET --> G_TR
```

### Sales Lineage Table

| Stage | Table | Source | Transformation | Quality Gate |
|---|---|---|---|---|
| Ingestion | `brz_freshsip.pos_transactions_raw` | POS JSON hourly files | Append-only; all columns as STRING; metadata columns auto-added | BRZ-SALES-POS-001 through 007 |
| Ingestion | `brz_freshsip.erp_sales_raw` | ERP CSV daily | Append-only; schema-on-read | BRZ-SALES-ERP-001 through 005 |
| Ingestion | `brz_freshsip.erp_returns_raw` | ERP returns CSV daily | Append-only | BRZ-SALES-RET-001 through 004 |
| Cleaning | `slv_freshsip.sales_transactions` | pos_transactions_raw + erp_sales_raw | Dedup on transaction_id; type cast STRING→typed; null handling; channel validation | SLV-SALES-TXN-001 through 010 |
| Cleaning | `slv_freshsip.sales_returns` | erp_returns_raw | Dedup on return_id; type cast; reason code validation | SLV-SALES-RET-001 through 006 |
| Aggregation | `gld_freshsip.sales_daily_revenue` | sales_transactions + sales_returns + ref_products | GROUP BY (date, category, region, channel); compute net_revenue = gross - returns | GLD-SALES-REV-001 through 004 |
| Aggregation | `gld_freshsip.sales_period_comparison` | sales_daily_revenue | MoM window: current MTD vs prior MTD; YoY window: YTD vs prior YTD | GLD-SALES-MOM-001 through 003 |
| Aggregation | `gld_freshsip.sales_gross_margin_sku` | sales_transactions + sales_returns + ref_products | Weekly GROUP BY sku_id; gross_margin = net_revenue - (standard_cost x qty) | GLD-SALES-MARG-001 through 003 |

### Sales Column-Level Lineage (KPI-S01)

```
slv_freshsip.sales_transactions.unit_price (DECIMAL)
  └─ * slv_freshsip.sales_transactions.quantity_sold (INTEGER)
      = gross_sales_amount
          └─ - slv_freshsip.sales_returns.return_amount (DECIMAL, joined on transaction_id)
              = gld_freshsip.sales_daily_revenue.net_revenue

slv_freshsip.ref_products.product_category (STRING)
  └─ JOIN on sku_id
      → gld_freshsip.sales_daily_revenue.product_category

slv_freshsip.sales_transactions.region (STRING)
  └─ → gld_freshsip.sales_daily_revenue.region

slv_freshsip.sales_transactions.transaction_date (DATE)
  └─ → gld_freshsip.sales_daily_revenue.report_date
```

### Sales Column-Level Lineage (KPI-S04)

```
slv_freshsip.ref_products.standard_cost_per_unit (DECIMAL)
  └─ * slv_freshsip.sales_transactions.quantity_sold (INTEGER)
      = cogs per line
          └─ SUM(cogs) / SUM(net_revenue) * 100
              = gld_freshsip.sales_gross_margin_sku.gross_margin_pct
```

---

## Domain 2: Inventory

### Inventory Lineage Diagram

```mermaid
flowchart LR
    subgraph SOURCES_I["Source Systems"]
        ERP_INV["ERP Inventory Snapshots\nCSV / Daily\n/mnt/landing/erp/inventory/"]
    end

    subgraph BRONZE_I["Bronze — brz_freshsip"]
        B_INV["erp_inventory_raw\n- All columns as STRING\n- Append-only\n- Partition: ingestion_date"]
    end

    subgraph SILVER_I["Silver — slv_freshsip"]
        SV_STOCK["inventory_stock\n- Typed columns\n- Unique on sku+wh+date\n- MERGE upsert\n- Partition: snapshot_date"]
        SV_ROP["ref_reorder_points\n- reorder thresholds per sku+wh\n- SCD Type 1 overwrite"]
        SV_REF_P2["ref_products\n- standard_cost_per_unit\n- product_category"]
    end

    subgraph GOLD_I["Gold — gld_freshsip"]
        G_SL["inventory_stock_levels\nKPI-I01 and KPI-I04\nCurrent stock + reorder_alert_flag\nHourly refresh"]
        G_IT["inventory_turnover\nKPI-I02\nSUM(COGS_30d) / AVG(inventory_value_30d)\nWeekly by warehouse"]
        G_DSI["inventory_dsi\nKPI-I03\nunits_on_hand / avg_daily_sales\nDaily by sku+warehouse"]
    end

    subgraph SALES_REF["Sales Silver (cross-domain join)"]
        SV_SALES["sales_transactions\n- quantity_sold\n- transaction_date\n- sku_id"]
    end

    ERP_INV --> B_INV
    B_INV --> SV_STOCK
    B_INV --> SV_ROP

    SV_STOCK --> G_SL
    SV_ROP --> G_SL
    SV_REF_P2 --> G_SL

    SV_STOCK --> G_IT
    SV_SALES --> G_IT
    SV_REF_P2 --> G_IT

    SV_STOCK --> G_DSI
    SV_SALES --> G_DSI
```

### Inventory Lineage Table

| Stage | Table | Source | Transformation | Quality Gate |
|---|---|---|---|---|
| Ingestion | `brz_freshsip.erp_inventory_raw` | ERP inventory CSV daily | Append-only; schema-on-read | BRZ-INV-SNAP-001 through 005 |
| Cleaning | `slv_freshsip.inventory_stock` | erp_inventory_raw | Dedup on (sku_id, warehouse_id, snapshot_date); type cast; compute inventory_value; reject negative stock | SLV-INV-STOCK-001 through 008 |
| Reference | `slv_freshsip.ref_reorder_points` | erp_inventory_raw (reorder_point_units column) | Extract reorder thresholds; MERGE overwrite on (sku_id, warehouse_id) | SLV-INV-ROP-001 through 005 |
| KPI | `gld_freshsip.inventory_stock_levels` | inventory_stock + ref_reorder_points + ref_products | Join on sku_id+warehouse_id; compute reorder_alert_flag and deficit_units | GLD-INV-STOCK-001 through 004 |
| KPI | `gld_freshsip.inventory_turnover` | inventory_stock + sales_transactions + ref_products | 30-day rolling COGS / avg inventory value by warehouse | GLD-INV-TURN-001 through 003 |
| KPI | `gld_freshsip.inventory_dsi` | inventory_stock + sales_transactions | units_on_hand / (30-day avg daily sales) per sku+warehouse | GLD-INV-DSI-001 through 003 |

### Inventory Column-Level Lineage (KPI-I04 — Reorder Alert)

```
slv_freshsip.inventory_stock.units_on_hand (INTEGER)
  └─ <= slv_freshsip.ref_reorder_points.reorder_point_units (INTEGER)
      = gld_freshsip.inventory_stock_levels.reorder_alert_flag (BOOLEAN)

slv_freshsip.ref_reorder_points.reorder_point_units (INTEGER)
  └─ - slv_freshsip.inventory_stock.units_on_hand (INTEGER)
      = gld_freshsip.inventory_stock_levels.deficit_units (INTEGER)
```

### Inventory Column-Level Lineage (KPI-I03 — DSI)

```
slv_freshsip.inventory_stock.units_on_hand (INTEGER)
  └─ / (SUM(slv_freshsip.sales_transactions.quantity_sold) / 30.0)
       [trailing 30 days, grouped by sku_id]
      = gld_freshsip.inventory_dsi.dsi_days (DECIMAL)
```

---

## Domain 3: Production

### Production Lineage Diagram

```mermaid
flowchart LR
    subgraph SOURCES_P["Source Systems"]
        IOT["IoT Production Sensors\nCSV/JSON / 5-min micro-batch\n/mnt/landing/iot/"]
    end

    subgraph BRONZE_P["Bronze — brz_freshsip"]
        B_IOT["iot_sensor_events_raw\n- All columns as STRING\n- Append-only micro-batch\n- Partition: ingestion_date"]
    end

    subgraph SILVER_P["Silver — slv_freshsip"]
        SV_BATCH["production_batches\n- One row per batch_id\n- Aggregated from events\n- yield_rate_pct computed\n- MERGE upsert on batch_id\n- Partition: batch_date"]
        SV_EVENTS["production_events\n- One row per event_id\n- downtime_hours computed\n- Append-only\n- Partition: event_date"]
    end

    subgraph GOLD_P["Gold — gld_freshsip"]
        G_YIELD["production_yield\nKPI-P01\nactual_output / expected_output * 100\nPer batch, MERGE upsert"]
        G_QC["production_quality\nKPI-P02\nSUM(PASS) / COUNT(batch_id) * 100\nDaily by line + category"]
        G_DT["production_downtime\nKPI-P03\nSUM(downtime_end - downtime_start) / 3600\nDaily by production_line_id"]
        G_TRACE["production_traceability\nKPI-P04\nbatch -> shipment -> retailer chain\nDaily completeness check"]
    end

    subgraph DIST_REF["Distribution Silver (cross-domain)"]
        SV_SHIP2["shipments\n- shipment_id\n- retailer_id\n- batch_id linkage via order_id"]
    end

    IOT --> B_IOT
    B_IOT --> SV_BATCH
    B_IOT --> SV_EVENTS

    SV_BATCH --> G_YIELD
    SV_BATCH --> G_QC
    SV_EVENTS --> G_DT
    SV_BATCH --> G_TRACE
    SV_SHIP2 --> G_TRACE
```

### Production Lineage Table

| Stage | Table | Source | Transformation | Quality Gate |
|---|---|---|---|---|
| Ingestion | `brz_freshsip.iot_sensor_events_raw` | IoT sensor CSV/JSON micro-batch every 5 min | Append-only; schema-on-read; triggered via Structured Streaming | BRZ-PROD-IOT-001 through 005 |
| Aggregation | `slv_freshsip.production_batches` | iot_sensor_events_raw | Pivot BATCH_START/BATCH_END/QC_CHECK events into one row per batch_id; compute yield_rate_pct | SLV-PROD-BATCH-001 through 008 |
| Cleaning | `slv_freshsip.production_events` | iot_sensor_events_raw | One row per event_id; compute downtime_hours from timestamps | SLV-PROD-EVT-001 through 007 |
| KPI | `gld_freshsip.production_yield` | production_batches + ref_products | MERGE on batch_id; compute yield alert flags | GLD-PROD-YIELD-001 through 003 |
| KPI | `gld_freshsip.production_quality` | production_batches + ref_products | Daily GROUP BY line + category; qc_pass_rate | GLD-PROD-QC-001 through 003 |
| KPI | `gld_freshsip.production_downtime` | production_events | Daily GROUP BY production_line_id; SUM downtime hours for DOWNTIME_UNPLANNED events | GLD-PROD-DT-001 through 003 |
| KPI | `gld_freshsip.production_traceability` | production_batches + shipments + customers | Left join chain; compute is_fully_traceable flag | N/A (count check only) |

### Production Column-Level Lineage (KPI-P01 — Yield Rate)

```
brz_freshsip.iot_sensor_events_raw.actual_output_cases (STRING)
  └─ CAST to INTEGER
      → slv_freshsip.production_batches.actual_output_cases (INTEGER)
          └─ / slv_freshsip.production_batches.expected_output_cases (INTEGER)
              * 100
              = gld_freshsip.production_yield.yield_rate_pct (DECIMAL)

brz_freshsip.iot_sensor_events_raw.downtime_start_ts (STRING)
brz_freshsip.iot_sensor_events_raw.downtime_end_ts (STRING)
  └─ CAST to TIMESTAMP
      → (downtime_end_ts - downtime_start_ts) / 3600.0
          = slv_freshsip.production_events.downtime_hours (DECIMAL)
              → SUM per (event_date, production_line_id)
                  = gld_freshsip.production_downtime.downtime_hours (DECIMAL)
```

---

## Domain 4: Distribution

### Distribution Lineage Diagram

```mermaid
flowchart LR
    subgraph SOURCES_D["Source Systems"]
        LOG["Logistics Partner Shipment Data\nCSV / Daily\n/mnt/landing/logistics/"]
    end

    subgraph BRONZE_D["Bronze — brz_freshsip"]
        B_LOG["logistics_shipments_raw\n- All columns as STRING\n- Append-only\n- Partition: ingestion_date"]
    end

    subgraph SILVER_D["Silver — slv_freshsip"]
        SV_SHIP["shipments\n- Typed columns\n- on_time_flag computed\n- is_fully_shipped computed\n- MERGE upsert on shipment_id\n- Partition: ship_date"]
    end

    subgraph GOLD_D["Gold — gld_freshsip"]
        G_OTD["distribution_otd\nKPI-D01\nSUM(on_time) / COUNT(*) * 100\nDaily by channel + region"]
        G_FULL["distribution_fulfillment\nKPI-D02\nSUM(is_fully_shipped) / COUNT(order_id) * 100\nDaily by channel"]
        G_COST["distribution_cost\nKPI-D03\nSUM(logistics_cost) / SUM(cases_delivered)\nWeekly by region + route"]
        G_ROUTE["distribution_route_performance\nKPI-D04\nRANK routes by OTD ascending\nWeekly top 10 worst routes"]
    end

    LOG --> B_LOG
    B_LOG --> SV_SHIP

    SV_SHIP --> G_OTD
    SV_SHIP --> G_FULL
    SV_SHIP --> G_COST
    SV_SHIP --> G_ROUTE
```

### Distribution Lineage Table

| Stage | Table | Source | Transformation | Quality Gate |
|---|---|---|---|---|
| Ingestion | `brz_freshsip.logistics_shipments_raw` | Logistics partner CSV daily | Append-only; schema-on-read | BRZ-DIST-SHIP-001 through 005 |
| Cleaning | `slv_freshsip.shipments` | logistics_shipments_raw | Dedup on shipment_id; type cast; compute on_time_flag = (actual_delivery_date <= promised_delivery_date); compute is_fully_shipped | SLV-DIST-SHIP-001 through 009 |
| KPI | `gld_freshsip.distribution_otd` | shipments | Daily GROUP BY (channel, region); SUM on_time_flag / COUNT(shipment_id) * 100 | GLD-DIST-OTD-001 through 003 |
| KPI | `gld_freshsip.distribution_fulfillment` | shipments | Daily GROUP BY channel; COUNT fully_shipped orders / total orders | GLD-DIST-FULL-001 through 003 |
| KPI | `gld_freshsip.distribution_cost` | shipments | Weekly GROUP BY (region, route_id, channel); logistics_cost / cases_delivered | GLD-DIST-COST-001 through 003 |
| KPI | `gld_freshsip.distribution_route_performance` | shipments | Weekly GROUP BY route_id; RANK by OTD% ascending; retain top 10 worst | N/A (ranking) |

### Distribution Column-Level Lineage (KPI-D01 — OTD)

```
brz_freshsip.logistics_shipments_raw.actual_delivery_date (STRING)
  └─ CAST to DATE
      → slv_freshsip.shipments.actual_delivery_date (DATE)
          └─ <= slv_freshsip.shipments.promised_delivery_date (DATE)
              = slv_freshsip.shipments.on_time_flag (BOOLEAN)
                  → SUM(CASE WHEN on_time_flag THEN 1 ELSE 0 END) / COUNT(shipment_id) * 100
                      = gld_freshsip.distribution_otd.otd_pct (DECIMAL)

brz_freshsip.logistics_shipments_raw.logistics_cost_usd (STRING)
  └─ CAST to DECIMAL
      → slv_freshsip.shipments.logistics_cost_usd (DECIMAL)
          → SUM(logistics_cost_usd) / SUM(cases_delivered)
              = gld_freshsip.distribution_cost.cost_per_case (DECIMAL)
```

---

## Domain 5: Customers

### Customers Lineage Diagram

```mermaid
flowchart LR
    subgraph SOURCES_C["Source Systems"]
        ERP_C["ERP Customer / Retailer Master\nCSV / Daily\n/mnt/landing/erp/customers/"]
        ERP_P_C["ERP Product Master\nCSV / Daily\n/mnt/landing/erp/products/"]
    end

    subgraph BRONZE_C["Bronze — brz_freshsip"]
        B_CUST["erp_customers_raw\n- All columns as STRING\n- Append-only\n- Partition: ingestion_date"]
        B_PROD["erp_products_raw\n- All columns as STRING\n- Append-only\n- Partition: ingestion_date"]
    end

    subgraph SILVER_C["Silver — slv_freshsip"]
        SV_CUST["customers\nSCD Type 2\n- surrogate_key, valid_from/to, is_current\n- MERGE on retailer_id"]
        SV_PROD["products\nSCD Type 2\n- surrogate_key, valid_from/to, is_current\n- MERGE on sku_id"]
        SV_REF_P3["ref_products\n- SCD Type 1 overwrite\n- Current product attrs\n- standard_cost_per_unit"]
        SV_SPEND2["sales_spend\n- trade_spend / broker_commission\n- field_sales_cost per retailer"]
    end

    subgraph GOLD_C["Gold — gld_freshsip"]
        G_TOP["customers_top_retailers\nKPI-C01\nRanked SUM(net_revenue) per retailer\nWeekly + MTD"]
        G_CAC["customers_cac\nKPI-C02\nSUM(acquisition_spend) / COUNT(new_accounts)\nMonthly by segment"]
        G_RET["customers_retention\nKPI-C03\nRETAINED / PRIOR_ACTIVE * 100\nMonthly by region"]
        G_CONC["customers_concentration_risk\nKPI-C04\nTop5_revenue / total_revenue * 100\nMonthly"]
    end

    subgraph SALES_REF2["Sales Silver (cross-domain)"]
        SV_SALES2["sales_transactions\n- retailer_id\n- unit_price, quantity_sold\n- transaction_date"]
        SV_RET2["sales_returns\n- return_amount\n- transaction_id"]
    end

    ERP_C --> B_CUST
    ERP_P_C --> B_PROD

    B_CUST --> SV_CUST
    B_CUST --> SV_SPEND2
    B_PROD --> SV_PROD
    B_PROD --> SV_REF_P3

    SV_SALES2 --> G_TOP
    SV_RET2 --> G_TOP
    SV_CUST --> G_TOP

    SV_CUST --> G_CAC
    SV_SPEND2 --> G_CAC

    SV_CUST --> G_RET
    SV_SALES2 --> G_RET

    G_TOP --> G_CONC
    SV_SALES2 --> G_CONC
    SV_CUST --> G_CONC
```

### Customers Lineage Table

| Stage | Table | Source | Transformation | Quality Gate |
|---|---|---|---|---|
| Ingestion | `brz_freshsip.erp_customers_raw` | ERP customer CSV daily | Append-only; schema-on-read | BRZ-CUST-ERP-001 through 005 |
| Ingestion | `brz_freshsip.erp_products_raw` | ERP product CSV daily | Append-only; schema-on-read | BRZ-PROD-ERP-001 through 005 |
| SCD Type 2 | `slv_freshsip.customers` | erp_customers_raw | Detect changed attributes; close prior record (set valid_to, is_current=false); insert new version | SLV-CUST-001 through 007 |
| SCD Type 2 | `slv_freshsip.products` | erp_products_raw | Same SCD Type 2 MERGE pattern as customers | SLV-PROD-SKU-001 through 007 |
| SCD Type 1 | `slv_freshsip.ref_products` | products (is_current=true) | Full overwrite with current-version attributes; used for Gold joins | SLV-PROD-SKU-001 through 007 |
| Spend | `slv_freshsip.sales_spend` | erp_customers_raw (spend columns) | Extract trade_spend, broker_commission, field_sales_cost per retailer per period | SLV-SALES-SPEND-001 through 006 |
| KPI | `gld_freshsip.customers_top_retailers` | sales_transactions + sales_returns + customers | Ranked net_revenue per retailer; weekly window | GLD-CUST-TOP-001 through 003 |
| KPI | `gld_freshsip.customers_cac` | customers + sales_spend | New accounts (activation_date in period) + SUM(acquisition_spend) / count | GLD-CUST-CAC-001 through 003 |
| KPI | `gld_freshsip.customers_retention` | customers + sales_transactions | Distinct active retailers in prior vs. current period | GLD-CUST-RET-001 through 003 |
| KPI | `gld_freshsip.customers_concentration_risk` | customers_top_retailers + sales_transactions | Top5 revenue / total revenue | GLD-CUST-CONC-001 through 003 |

### Customers Column-Level Lineage (KPI-C02 — CAC)

```
brz_freshsip.erp_customers_raw.trade_spend_usd (STRING)
brz_freshsip.erp_customers_raw.broker_commission_usd (STRING)
brz_freshsip.erp_customers_raw.field_sales_cost_usd (STRING)
  └─ CAST to DECIMAL
      → slv_freshsip.sales_spend.trade_spend_usd
      → slv_freshsip.sales_spend.broker_commission_usd
      → slv_freshsip.sales_spend.field_sales_cost_usd
          └─ SUM(all three)
              = slv_freshsip.sales_spend.total_acquisition_cost_usd (DECIMAL)

brz_freshsip.erp_customers_raw.account_activation_date (STRING)
  └─ CAST to DATE
      → slv_freshsip.customers.account_activation_date (DATE)
          └─ WHERE activation_date IN current period
              = COUNT(DISTINCT new_account_id)

SUM(total_acquisition_cost_usd) / COUNT(DISTINCT new_account_id)
  = gld_freshsip.customers_cac.cac_usd (DECIMAL)
```

### Customers Column-Level Lineage (KPI-C04 — Concentration Risk)

```
slv_freshsip.sales_transactions.unit_price (DECIMAL)
  └─ * slv_freshsip.sales_transactions.quantity_sold (INTEGER)
      - slv_freshsip.sales_returns.return_amount (DECIMAL)
          = retailer_net_revenue per retailer
              └─ RANK() OVER ORDER BY retailer_net_revenue DESC
                  → gld_freshsip.customers_top_retailers.revenue_rank

gld_freshsip.customers_top_retailers.retailer_net_revenue
  └─ WHERE revenue_rank <= 5
      SUM(top5_revenue) / SUM(all_revenue) * 100
          = gld_freshsip.customers_concentration_risk.top5_concentration_pct
```

---

## Cross-Domain Joins Summary

The following Silver tables are joined across domain boundaries in Gold KPI computations:

| Join | Left Table | Right Table | Join Keys | Used In KPI |
|---|---|---|---|---|
| Sales + Products | `sales_transactions` | `ref_products` | `sku_id` | KPI-S01, S04, I02, I03 |
| Sales + Customers | `sales_transactions` | `customers` | `retailer_id` | KPI-C01, C03, C04 |
| Sales + Returns | `sales_transactions` | `sales_returns` | `transaction_id` | KPI-S01, S04, C01, C04 |
| Inventory + Products | `inventory_stock` | `ref_products` | `sku_id` | KPI-I01, I02, I04 |
| Inventory + Reorder Points | `inventory_stock` | `ref_reorder_points` | `sku_id, warehouse_id` | KPI-I01, I04 |
| Inventory + Sales | `inventory_stock` | `sales_transactions` | `sku_id` | KPI-I02, I03 |
| Production + Products | `production_batches` | `ref_products` | `sku_id` | KPI-P01, P02 |
| Production + Shipments | `production_batches` | `shipments` | `batch_id` (via order_id) | KPI-P04 |
| Customers + Spend | `customers` | `sales_spend` | `retailer_id` | KPI-C02 |
| Customers + Transactions | `customers` | `sales_transactions` | `retailer_id` | KPI-C01, C03 |

---

## Lineage Summary Table

| Source File | Bronze Table | Silver Table(s) | Gold KPI Table(s) | KPI IDs |
|---|---|---|---|---|
| POS JSON (hourly) | `pos_transactions_raw` | `sales_transactions` | `sales_daily_revenue`, `sales_period_comparison`, `sales_gross_margin_sku`, `customers_top_retailers` | S01, S02, S03, S04, C01 |
| ERP Sales CSV (daily) | `erp_sales_raw` | `sales_transactions` | (same as above) | S01, S02, S03, S04 |
| ERP Returns CSV (daily) | `erp_returns_raw` | `sales_returns`, `sales_spend` | `sales_daily_revenue`, `sales_gross_margin_sku`, `customers_cac` | S01, S04, C02 |
| ERP Inventory CSV (daily) | `erp_inventory_raw` | `inventory_stock`, `ref_reorder_points` | `inventory_stock_levels`, `inventory_turnover`, `inventory_dsi` | I01, I02, I03, I04 |
| IoT Sensors (5-min) | `iot_sensor_events_raw` | `production_batches`, `production_events` | `production_yield`, `production_quality`, `production_downtime`, `production_traceability` | P01, P02, P03, P04 |
| Logistics CSV (daily) | `logistics_shipments_raw` | `shipments` | `distribution_otd`, `distribution_fulfillment`, `distribution_cost`, `distribution_route_performance` | D01, D02, D03, D04 |
| ERP Customers CSV (daily) | `erp_customers_raw` | `customers`, `sales_spend` | `customers_top_retailers`, `customers_cac`, `customers_retention`, `customers_concentration_risk` | C01, C02, C03, C04 |
| ERP Products CSV (daily) | `erp_products_raw` | `products`, `ref_products` | (reference used in all margin KPIs) | S04, I01, I02, P01, P02 |
