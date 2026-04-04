# FreshSip Beverages — KPI Registry

**Version:** 1.0
**Date:** 2026-04-05
**Status:** Draft
**Author:** Product Owner Agent
**Linked PRD:** PRD-001-freshsip-cpg-platform.md

> This is the authoritative KPI registry for the FreshSip CPG Data Platform. All Gold layer tables must implement KPIs exactly as defined here. Any change to a formula or threshold requires a version bump and Product Owner approval.

---

## Domain Index

- [Sales Domain (KPI-S01 through KPI-S04)](#sales-domain)
- [Inventory Domain (KPI-I01 through KPI-I04)](#inventory-domain)
- [Production Domain (KPI-P01 through KPI-P04)](#production-domain)
- [Distribution Domain (KPI-D01 through KPI-D04)](#distribution-domain)
- [Customers Domain (KPI-C01 through KPI-C04)](#customers-domain)

---

## Sales Domain

### KPI-S01: Daily Revenue

- **Business Question:** What is the total net revenue generated today (and on any given day) by product category, region, and channel — and how does it compare to prior periods?
- **Formula (SQL):**
  ```sql
  SUM(t.unit_price * t.quantity_sold) - SUM(COALESCE(r.return_amount, 0))
  ```
  Where `t` = `slv_freshsip.sales_transactions` and `r` = `slv_freshsip.sales_returns` joined on `transaction_id`.
- **Gold Layer Table:** `gld_freshsip.sales_daily_revenue`
- **Source Tables:**
  - `slv_freshsip.sales_transactions`
  - `slv_freshsip.sales_returns`
  - `slv_freshsip.ref_products`
- **Key Columns:** `unit_price`, `quantity_sold`, `return_amount`, `transaction_date`, `product_category`, `region`, `channel`, `sku_id`
- **Granularity:** Daily by `product_category`, `region`, and `channel`
- **Refresh Frequency:** Hourly (aligned with POS ingestion cadence); Gold table recalculated each run
- **Target:** $2.5M/day company-wide
- **Alert Threshold (Warn):** Daily revenue < $2.0M company-wide
- **Alert Threshold (Critical):** Daily revenue < $1.5M company-wide
- **Dashboard Widget Type:** KPI card (total) + line chart (daily trend, 30-day rolling) + bar chart (by category and region)
- **Owner:** VP Sales
- **MoSCoW:** Must Have

---

### KPI-S02: Revenue vs. Prior Month (MoM %)

- **Business Question:** Is revenue growing or declining compared to the same period last month, and by how much for each category and region?
- **Formula (SQL):**
  ```sql
  (SUM(CASE WHEN period = 'current_month' THEN net_revenue ELSE 0 END)
   - SUM(CASE WHEN period = 'prior_month' THEN net_revenue ELSE 0 END))
  / NULLIF(SUM(CASE WHEN period = 'prior_month' THEN net_revenue ELSE 0 END), 0) * 100
  ```
  Where `net_revenue` is the pre-computed column from `gld_freshsip.sales_daily_revenue` rolled up to month, and `period` is a derived label comparing the current calendar month-to-date vs. the same MTD window in the prior month.
- **Gold Layer Table:** `gld_freshsip.sales_period_comparison`
- **Source Tables:**
  - `gld_freshsip.sales_daily_revenue`
- **Key Columns:** `net_revenue`, `transaction_date`, `product_category`, `region`
- **Granularity:** Month-to-date by `product_category` and `region`
- **Refresh Frequency:** Daily at 06:00 UTC
- **Target:** >= 0% MoM growth (flat or positive)
- **Alert Threshold (Warn):** MoM % < -5% for any category
- **Alert Threshold (Critical):** MoM % < -15% company-wide
- **Dashboard Widget Type:** KPI card with delta indicator (green/red arrow) + comparison bar chart (current vs. prior month)
- **Owner:** VP Sales
- **MoSCoW:** Must Have

---

### KPI-S03: Revenue vs. Prior Year (YoY %)

- **Business Question:** How does this period's revenue compare to the same period one year ago — is FreshSip growing year-over-year?
- **Formula (SQL):**
  ```sql
  (SUM(CASE WHEN year_label = 'current_year' THEN net_revenue ELSE 0 END)
   - SUM(CASE WHEN year_label = 'prior_year' THEN net_revenue ELSE 0 END))
  / NULLIF(SUM(CASE WHEN year_label = 'prior_year' THEN net_revenue ELSE 0 END), 0) * 100
  ```
  Where `year_label` distinguishes the current year-to-date window from the same window in the prior year, derived from `transaction_date` in `gld_freshsip.sales_daily_revenue`.
- **Gold Layer Table:** `gld_freshsip.sales_period_comparison`
- **Source Tables:**
  - `gld_freshsip.sales_daily_revenue`
- **Key Columns:** `net_revenue`, `transaction_date`, `channel`
- **Granularity:** Year-to-date company-wide and by `channel`
- **Refresh Frequency:** Daily at 06:00 UTC
- **Target:** >= 10% YoY growth (company strategic goal)
- **Alert Threshold (Warn):** YoY % < 5% company-wide
- **Alert Threshold (Critical):** YoY % < 0% (negative growth) company-wide
- **Dashboard Widget Type:** KPI card with YoY delta + dual-axis line chart (current year vs. prior year, same x-axis calendar week)
- **Owner:** CEO
- **MoSCoW:** Must Have

---

### KPI-S04: Gross Margin by SKU

- **Business Question:** Which SKUs are the most and least profitable after accounting for cost of goods sold — and is gross margin trending in the right direction?
- **Formula (SQL):**
  ```sql
  (SUM(net_revenue) - SUM(cogs)) / NULLIF(SUM(net_revenue), 0) * 100
  ```
  Where `net_revenue = unit_price * quantity_sold - return_amount` and `cogs = standard_cost_per_unit * quantity_sold`, with `standard_cost_per_unit` sourced from `slv_freshsip.ref_products`.
- **Gold Layer Table:** `gld_freshsip.sales_gross_margin_sku`
- **Source Tables:**
  - `slv_freshsip.sales_transactions`
  - `slv_freshsip.sales_returns`
  - `slv_freshsip.ref_products`
- **Key Columns:** `unit_price`, `quantity_sold`, `return_amount`, `standard_cost_per_unit`, `sku_id`, `product_category`, `transaction_date`
- **Granularity:** Weekly by `sku_id` and `product_category`
- **Refresh Frequency:** Daily at 06:00 UTC (weekly aggregation; current week updated incrementally)
- **Target:** >= 40% gross margin across all SKUs
- **Alert Threshold (Warn):** Any SKU gross margin < 30%
- **Alert Threshold (Critical):** Any SKU gross margin < 20% or company-wide gross margin < 35%
- **Dashboard Widget Type:** Ranked table (SKU, revenue, margin %, trend arrow) + scatter plot (revenue vs. margin % with category color coding)
- **Owner:** VP Sales
- **MoSCoW:** Must Have

---

## Inventory Domain

### KPI-I01: Current Stock Level

- **Business Question:** How many units of each SKU are currently on hand at each warehouse — and are any at risk of running out before the next replenishment?
- **Formula (SQL):**
  ```sql
  SUM(i.units_on_hand) AS current_stock_units
  ```
  Joined with `slv_freshsip.ref_reorder_points` on `sku_id` and `warehouse_id` to flag SKUs below threshold.
- **Gold Layer Table:** `gld_freshsip.inventory_stock_levels`
- **Source Tables:**
  - `slv_freshsip.inventory_stock`
  - `slv_freshsip.ref_reorder_points`
  - `slv_freshsip.ref_products`
- **Key Columns:** `units_on_hand`, `sku_id`, `warehouse_id`, `snapshot_timestamp`, `reorder_point_units`
- **Granularity:** Current snapshot by `sku_id` and `warehouse_id`; historical daily snapshots retained for trend
- **Refresh Frequency:** Hourly
- **Target:** All SKUs at all warehouses maintaining >= 7 days of supply
- **Alert Threshold (Warn):** Any SKU at any warehouse below reorder point defined in `slv_freshsip.ref_reorder_points`
- **Alert Threshold (Critical):** Any SKU at any warehouse with 0 units on hand (stockout)
- **Dashboard Widget Type:** Heat map (warehouse x SKU, color = stock health) + KPI card (count of SKUs below reorder point) + sortable table
- **Owner:** Supply Chain Manager
- **MoSCoW:** Must Have

---

### KPI-I02: Inventory Turnover Rate

- **Business Question:** How efficiently is FreshSip converting inventory into sales — are we holding too much or too little stock relative to what we sell?
- **Formula (SQL):**
  ```sql
  SUM(cogs_30d) / NULLIF(AVG(inventory_value), 0)
  ```
  Where `cogs_30d` = cost of goods sold in the trailing 30 days (from `slv_freshsip.sales_transactions` joined with `slv_freshsip.ref_products` for `standard_cost_per_unit`) and `inventory_value` = `AVG(units_on_hand * standard_cost_per_unit)` over the same 30-day window from `slv_freshsip.inventory_stock`.
- **Gold Layer Table:** `gld_freshsip.inventory_turnover`
- **Source Tables:**
  - `slv_freshsip.inventory_stock`
  - `slv_freshsip.sales_transactions`
  - `slv_freshsip.ref_products`
- **Key Columns:** `units_on_hand`, `standard_cost_per_unit`, `quantity_sold`, `warehouse_id`, `snapshot_date`, `transaction_date`
- **Granularity:** Weekly by `warehouse_id`; trailing 30-day rolling window
- **Refresh Frequency:** Weekly (recalculated every Monday at 05:00 UTC)
- **Target:** Inventory turnover >= 8x per year (or >= 0.67x per rolling 30-day window)
- **Alert Threshold (Warn):** Turnover rate < 0.5x per 30-day window for any warehouse
- **Alert Threshold (Critical):** Turnover rate < 0.3x per 30-day window for any warehouse
- **Dashboard Widget Type:** KPI card (company-wide turnover) + bar chart (by warehouse) + trend line (weekly rolling)
- **Owner:** Supply Chain Manager
- **MoSCoW:** Should Have

---

### KPI-I03: Days Sales of Inventory (DSI)

- **Business Question:** At current sales velocity, how many days of inventory do we have on hand — and which SKUs or warehouses are at risk of stockout before the next reorder?
- **Formula (SQL):**
  ```sql
  AVG(i.units_on_hand) / NULLIF(AVG(s.avg_daily_sales_units), 0)
  ```
  Where `avg_daily_sales_units` = `SUM(quantity_sold) / 30` over the trailing 30 days from `slv_freshsip.sales_transactions`, grouped by `sku_id` and `warehouse_id`.
- **Gold Layer Table:** `gld_freshsip.inventory_dsi`
- **Source Tables:**
  - `slv_freshsip.inventory_stock`
  - `slv_freshsip.sales_transactions`
- **Key Columns:** `units_on_hand`, `quantity_sold`, `sku_id`, `warehouse_id`, `snapshot_date`, `transaction_date`
- **Granularity:** Daily by `sku_id` and `warehouse_id`; company-wide rollup also computed
- **Refresh Frequency:** Daily at 06:00 UTC
- **Target:** DSI between 14 and 45 days for all SKUs (14 = lean; 45 = maximum carry)
- **Alert Threshold (Warn):** DSI < 10 days or > 60 days for any SKU at any warehouse
- **Alert Threshold (Critical):** DSI < 7 days for any SKU at any warehouse (imminent stockout risk)
- **Dashboard Widget Type:** KPI card (median DSI company-wide) + color-coded table (SKU x warehouse, red < 7 days, yellow 7-14 days, green >= 14 days)
- **Owner:** Supply Chain Manager
- **MoSCoW:** Must Have

---

### KPI-I04: Reorder Alert Flag

- **Business Question:** Which specific SKUs at which warehouses need to be reordered right now — before a stockout occurs?
- **Formula (SQL):**
  ```sql
  CASE
    WHEN i.units_on_hand <= r.reorder_point_units THEN true
    ELSE false
  END AS reorder_alert_flag
  ```
  Where `r` = `slv_freshsip.ref_reorder_points` joined on `sku_id` and `warehouse_id`.
- **Gold Layer Table:** `gld_freshsip.inventory_stock_levels`
- **Source Tables:**
  - `slv_freshsip.inventory_stock`
  - `slv_freshsip.ref_reorder_points`
- **Key Columns:** `units_on_hand`, `reorder_point_units`, `sku_id`, `warehouse_id`, `snapshot_timestamp`
- **Granularity:** Current state per `sku_id` and `warehouse_id`; alert flags refreshed hourly
- **Refresh Frequency:** Hourly
- **Target:** Zero active reorder alerts (all SKUs above reorder point)
- **Alert Threshold (Warn):** >= 1 SKU below reorder point at any warehouse
- **Alert Threshold (Critical):** >= 5 SKUs below reorder point simultaneously, or any stockout (units_on_hand = 0)
- **Dashboard Widget Type:** Alert badge (count of active reorder flags) + filterable alert table (SKU, warehouse, units on hand, reorder point, deficit units)
- **Owner:** Supply Chain Manager
- **MoSCoW:** Must Have

---

## Production Domain

### KPI-P01: Batch Yield Rate

- **Business Question:** For each production batch, what percentage of the expected output was actually achieved — and are any production lines underperforming?
- **Formula (SQL):**
  ```sql
  SUM(actual_output_cases) / NULLIF(SUM(expected_output_cases), 0) * 100
  ```
  Computed per batch from `slv_freshsip.production_batches`, which receives input from IoT sensor micro-batch data.
- **Gold Layer Table:** `gld_freshsip.production_yield`
- **Source Tables:**
  - `slv_freshsip.production_batches`
- **Key Columns:** `actual_output_cases`, `expected_output_cases`, `batch_id`, `production_line_id`, `product_category`, `batch_start_ts`, `batch_end_ts`
- **Granularity:** Per `batch_id`; daily rollup by `production_line_id` and `product_category`
- **Refresh Frequency:** Micro-batch (every 5 minutes via Structured Streaming trigger); Gold table updated per batch completion event
- **Target:** Batch yield rate >= 95% for all production lines
- **Alert Threshold (Warn):** Any batch yield rate < 92%
- **Alert Threshold (Critical):** Any batch yield rate < 85% (potential quality event; trigger investigation)
- **Dashboard Widget Type:** KPI card (current shift average yield %) + time-series line chart (yield rate per batch, last 30 days) + bar chart by production line
- **Owner:** VP Operations
- **MoSCoW:** Should Have

---

### KPI-P02: Quality Check Pass Rate

- **Business Question:** What percentage of production batches are passing quality control inspections — and are there recurring failures on specific lines or products?
- **Formula (SQL):**
  ```sql
  SUM(CASE WHEN qc_status = 'PASS' THEN 1 ELSE 0 END)
  / NULLIF(COUNT(batch_id), 0) * 100
  ```
  Computed from `slv_freshsip.production_batches` where `qc_status` is populated after QC inspection is recorded in IoT data.
- **Gold Layer Table:** `gld_freshsip.production_quality`
- **Source Tables:**
  - `slv_freshsip.production_batches`
- **Key Columns:** `qc_status`, `batch_id`, `production_line_id`, `product_category`, `batch_end_ts`
- **Granularity:** Daily by `production_line_id` and `product_category`
- **Refresh Frequency:** Daily at 06:00 UTC (end-of-day QC completion assumed); micro-batch for real-time view
- **Target:** QC pass rate >= 98% daily across all lines
- **Alert Threshold (Warn):** QC pass rate < 96% on any production line on any day
- **Alert Threshold (Critical):** QC pass rate < 90% on any production line on any day (potential systematic quality failure)
- **Dashboard Widget Type:** KPI card (daily pass rate %) + trend line (30-day rolling) + heat map (production line x week, color = pass rate)
- **Owner:** VP Operations
- **MoSCoW:** Should Have

---

### KPI-P03: Downtime Hours

- **Business Question:** How many hours of unplanned production downtime occurred today and this week — and which production lines are responsible?
- **Formula (SQL):**
  ```sql
  SUM(
    CASE WHEN event_type = 'DOWNTIME_UNPLANNED'
    THEN (downtime_end_ts - downtime_start_ts) / 3600.0
    ELSE 0 END
  ) AS downtime_hours
  ```
  Sourced from `slv_freshsip.production_events` which captures IoT-derived stoppage events.
- **Gold Layer Table:** `gld_freshsip.production_downtime`
- **Source Tables:**
  - `slv_freshsip.production_batches`
  - `slv_freshsip.production_events`
- **Key Columns:** `event_type`, `downtime_start_ts`, `downtime_end_ts`, `production_line_id`, `event_date`
- **Granularity:** Daily by `production_line_id`; weekly rollup also computed
- **Refresh Frequency:** Daily at 06:00 UTC; micro-batch for active stoppage alerts
- **Target:** <= 2 hours of unplanned downtime per production line per day
- **Alert Threshold (Warn):** Any production line with > 2 unplanned downtime hours in a single day
- **Alert Threshold (Critical):** Any production line with > 6 unplanned downtime hours in a single day (shift capacity at risk)
- **Dashboard Widget Type:** KPI card (total downtime hours today) + bar chart (downtime hours by production line, last 7 days) + event log table (line, start time, duration, reason code)
- **Owner:** VP Operations
- **MoSCoW:** Should Have

---

### KPI-P04: Batch Traceability Index

- **Business Question:** For any given production batch, can we trace the complete chain from raw material input through to shipment and retailer delivery — enabling rapid recall response?
- **Formula (SQL):**
  ```sql
  SUM(
    CASE WHEN b.batch_id IS NOT NULL
          AND s.shipment_id IS NOT NULL
          AND s.retailer_id IS NOT NULL
    THEN 1 ELSE 0 END
  ) / NULLIF(COUNT(DISTINCT b.batch_id), 0) * 100
  ```
  Where `b` = `slv_freshsip.production_batches`, joined to `slv_freshsip.shipments` on `batch_id`, joined to `slv_freshsip.customers` on `retailer_id`. The index measures what percentage of batches have a complete traceable chain.
- **Gold Layer Table:** `gld_freshsip.production_traceability`
- **Source Tables:**
  - `slv_freshsip.production_batches`
  - `slv_freshsip.shipments`
  - `slv_freshsip.customers`
- **Key Columns:** `batch_id`, `shipment_id`, `retailer_id`, `batch_end_ts`, `ship_date`
- **Granularity:** Per `batch_id`; daily summary (% of batches with complete traceability chain)
- **Refresh Frequency:** Daily at 06:00 UTC
- **Target:** 100% of batches have complete batch → shipment → retailer traceability
- **Alert Threshold (Warn):** Traceability index < 98% (any batch with broken chain)
- **Alert Threshold (Critical):** Traceability index < 95% or any batch flagged for recall investigation missing traceability data
- **Dashboard Widget Type:** KPI card (% traceable) + drillable table (batch_id, status, shipment link, retailer link; highlight broken chains)
- **Owner:** VP Operations
- **MoSCoW:** Could Have

---

## Distribution Domain

### KPI-D01: On-Time Delivery %

- **Business Question:** What percentage of shipments arrived at the destination on or before the promised delivery date — and which regions and channels are underperforming?
- **Formula (SQL):**
  ```sql
  SUM(CASE WHEN actual_delivery_date <= promised_delivery_date THEN 1 ELSE 0 END)
  / NULLIF(COUNT(shipment_id), 0) * 100
  ```
  Sourced from `slv_freshsip.shipments` where `actual_delivery_date` = receipt date at destination (retailer DC or warehouse) and `promised_delivery_date` = committed date from ERP order.
- **Gold Layer Table:** `gld_freshsip.distribution_otd`
- **Source Tables:**
  - `slv_freshsip.shipments`
  - `slv_freshsip.customers`
- **Key Columns:** `actual_delivery_date`, `promised_delivery_date`, `shipment_id`, `channel`, `region`, `route_id`, `ship_date`
- **Granularity:** Daily by `channel` and `region`; weekly rollup also computed
- **Refresh Frequency:** Daily at 04:00 UTC (aligned with logistics partner CSV delivery)
- **Target:** On-Time Delivery >= 95% company-wide
- **Alert Threshold (Warn):** OTD% < 90% for any channel or region in a given week
- **Alert Threshold (Critical):** OTD% < 80% company-wide in any 7-day rolling window
- **Dashboard Widget Type:** KPI card (company-wide OTD%) + regional map (US states, color-coded by OTD%) + trend line (30-day rolling OTD% by channel)
- **Owner:** VP Operations
- **MoSCoW:** Should Have

---

### KPI-D02: Order Fulfillment Rate

- **Business Question:** What percentage of customer orders were fully shipped — with no backorders or partial fulfillments — and where are the fulfillment gaps?
- **Formula (SQL):**
  ```sql
  SUM(CASE WHEN is_fully_shipped = true THEN 1 ELSE 0 END)
  / NULLIF(COUNT(order_id), 0) * 100
  ```
  Where `is_fully_shipped` = `true` when all order lines on a given `order_id` have `quantity_shipped >= quantity_ordered`, computed during Silver layer transformation in `slv_freshsip.shipments`.
- **Gold Layer Table:** `gld_freshsip.distribution_fulfillment`
- **Source Tables:**
  - `slv_freshsip.shipments`
- **Key Columns:** `is_fully_shipped`, `order_id`, `channel`, `order_date`, `ship_date`, `quantity_shipped`, `quantity_ordered`
- **Granularity:** Daily by `channel`; weekly rollup
- **Refresh Frequency:** Daily at 04:00 UTC
- **Target:** Order Fulfillment Rate >= 98% across all channels
- **Alert Threshold (Warn):** Fulfillment rate < 95% for any channel in a given day
- **Alert Threshold (Critical):** Fulfillment rate < 90% company-wide in any 7-day rolling window
- **Dashboard Widget Type:** KPI card (current fulfillment rate %) + bar chart (by channel) + trend line (30-day rolling) + drill-down table (unfulfilled orders with reason)
- **Owner:** VP Operations
- **MoSCoW:** Should Have

---

### KPI-D03: Cost Per Case Delivered

- **Business Question:** How much does it cost to deliver one case of product to the customer, and which regions or routes are driving up logistics costs?
- **Formula (SQL):**
  ```sql
  SUM(logistics_cost_usd) / NULLIF(SUM(cases_delivered), 0)
  ```
  Where `logistics_cost_usd` and `cases_delivered` are sourced from `slv_freshsip.shipments`, populated from the logistics partner daily CSV feed.
- **Gold Layer Table:** `gld_freshsip.distribution_cost`
- **Source Tables:**
  - `slv_freshsip.shipments`
- **Key Columns:** `logistics_cost_usd`, `cases_delivered`, `region`, `route_id`, `ship_date`, `channel`
- **Granularity:** Weekly by `region` and `route_id`; monthly rollup for trend
- **Refresh Frequency:** Weekly (recalculated every Monday at 05:00 UTC)
- **Target:** Cost per case <= $4.50 company-wide
- **Alert Threshold (Warn):** Cost per case > $5.00 for any region in a given week
- **Alert Threshold (Critical):** Cost per case > $6.00 for any region, or company-wide average exceeds $5.50
- **Dashboard Widget Type:** KPI card (company-wide average cost per case) + bar chart (by region) + trend line (monthly rolling) + comparison vs. target
- **Owner:** VP Operations
- **MoSCoW:** Should Have

---

### KPI-D04: Worst-Performing Routes Score

- **Business Question:** Which distribution routes have the worst combination of late deliveries and high cost — so that operations can prioritize corrective action?
- **Formula (SQL):**
  ```sql
  -- Composite score per route (lower score = worse performance)
  -- Step 1: OTD% per route
  SUM(CASE WHEN actual_delivery_date <= promised_delivery_date THEN 1 ELSE 0 END)
  / NULLIF(COUNT(shipment_id), 0) * 100 AS route_otd_pct,
  -- Step 2: Avg cost per case per route
  SUM(logistics_cost_usd) / NULLIF(SUM(cases_delivered), 0) AS route_cost_per_case,
  -- Step 3: Rank routes ascending by OTD% (worst first), break ties by cost desc
  RANK() OVER (ORDER BY route_otd_pct ASC, route_cost_per_case DESC) AS worst_route_rank
  ```
  Top 10 routes by `worst_route_rank` are surfaced in the Gold table.
- **Gold Layer Table:** `gld_freshsip.distribution_route_performance`
- **Source Tables:**
  - `slv_freshsip.shipments`
- **Key Columns:** `route_id`, `actual_delivery_date`, `promised_delivery_date`, `logistics_cost_usd`, `cases_delivered`, `shipment_id`, `ship_date`, `region`
- **Granularity:** Weekly by `route_id`; top 10 worst routes retained in Gold table
- **Refresh Frequency:** Weekly (recalculated every Monday at 05:00 UTC)
- **Target:** No route in the top 10 worst list with OTD% < 85%
- **Alert Threshold (Warn):** Any route with OTD% < 80% appearing in the weekly worst-10 list
- **Alert Threshold (Critical):** Any route with OTD% < 70% or appearing in the worst-10 list for 3 consecutive weeks
- **Dashboard Widget Type:** Ranked table (route_id, region, OTD%, cost per case, weeks on worst list) + map overlay (route lines colored by OTD%)
- **Owner:** VP Operations
- **MoSCoW:** Could Have

---

## Customers Domain

### KPI-C01: Top 20 Retailers by Revenue (Retailer Revenue Rank)

- **Business Question:** Who are FreshSip's top 20 retailer accounts by net revenue, what share of total revenue do they represent, and how has their ranking shifted over time?
- **Formula (SQL):**
  ```sql
  SUM(t.unit_price * t.quantity_sold) - SUM(COALESCE(r.return_amount, 0)) AS retailer_net_revenue,
  retailer_net_revenue / NULLIF(SUM(retailer_net_revenue) OVER (), 0) * 100 AS pct_of_total_revenue,
  RANK() OVER (ORDER BY retailer_net_revenue DESC) AS revenue_rank
  ```
  Where `t` = `slv_freshsip.sales_transactions` joined to `slv_freshsip.customers` on `retailer_id`, and `r` = `slv_freshsip.sales_returns`.
- **Gold Layer Table:** `gld_freshsip.customers_top_retailers`
- **Source Tables:**
  - `slv_freshsip.sales_transactions`
  - `slv_freshsip.sales_returns`
  - `slv_freshsip.customers`
- **Key Columns:** `retailer_id`, `retailer_name`, `unit_price`, `quantity_sold`, `return_amount`, `transaction_date`, `region`, `channel`
- **Granularity:** Weekly by `retailer_id`; current month-to-date snapshot; year-to-date snapshot
- **Refresh Frequency:** Weekly (recalculated every Monday at 05:00 UTC); MTD updated daily
- **Target:** Top 20 retailers account for no more than 60% of total revenue (concentration management)
- **Alert Threshold (Warn):** Top 5 retailers account for > 30% of total revenue
- **Alert Threshold (Critical):** Any single retailer accounts for > 15% of total revenue (single-account concentration risk)
- **Dashboard Widget Type:** Ranked table with rank movement indicator (up/down arrows vs. prior week) + bar chart (revenue by retailer, top 20) + donut chart (top 5 vs. rest)
- **Owner:** VP Sales
- **MoSCoW:** Should Have

---

### KPI-C02: Customer Acquisition Cost (CAC)

- **Business Question:** How much does FreshSip spend, on average, to acquire each new retailer account — and is that cost trending up or down by retail segment?
- **Formula (SQL):**
  ```sql
  SUM(trade_spend_usd + broker_commission_usd + field_sales_cost_usd)
  / NULLIF(COUNT(DISTINCT new_account_id), 0)
  ```
  Where spend columns are sourced from `slv_freshsip.sales_spend` (ERP-derived) filtered to the acquisition period, and `new_account_id` = `retailer_id` where `account_activation_date` falls within the current period, from `slv_freshsip.customers`.
- **Gold Layer Table:** `gld_freshsip.customers_cac`
- **Source Tables:**
  - `slv_freshsip.customers`
  - `slv_freshsip.sales_spend`
- **Key Columns:** `trade_spend_usd`, `broker_commission_usd`, `field_sales_cost_usd`, `new_account_id`, `account_activation_date`, `retail_segment`, `region`
- **Granularity:** Monthly by `retail_segment`
- **Refresh Frequency:** Monthly (first business day of each month, covering prior calendar month)
- **Target:** CAC <= $2,500 per new retailer account (all segments)
- **Alert Threshold (Warn):** CAC > $3,000 for any retail segment in a given month
- **Alert Threshold (Critical):** CAC > $5,000 for any segment, or company-wide CAC exceeds $4,000
- **Dashboard Widget Type:** KPI card (current month CAC) + trend line (12-month rolling CAC by segment) + bar chart (CAC by retail segment)
- **Owner:** VP Sales
- **MoSCoW:** Could Have

---

### KPI-C03: Retailer Retention Rate

- **Business Question:** What percentage of active retailer accounts from the prior period are still purchasing from FreshSip this period — and where is churn highest?
- **Formula (SQL):**
  ```sql
  COUNT(DISTINCT CASE WHEN c.retailer_id IN (prior_period_active) AND c.retailer_id IN (current_period_active)
        THEN c.retailer_id END)
  / NULLIF(COUNT(DISTINCT prior_period_active.retailer_id), 0) * 100
  ```
  Where `prior_period_active` = retailers with at least one transaction in the prior calendar month, and `current_period_active` = retailers with at least one transaction in the current calendar month, both sourced from `slv_freshsip.sales_transactions` joined to `slv_freshsip.customers`.
- **Gold Layer Table:** `gld_freshsip.customers_retention`
- **Source Tables:**
  - `slv_freshsip.customers`
  - `slv_freshsip.sales_transactions`
- **Key Columns:** `retailer_id`, `transaction_date`, `region`, `retail_segment`
- **Granularity:** Monthly by `region`
- **Refresh Frequency:** Monthly (first business day of each month, covering prior calendar month)
- **Target:** Retailer retention rate >= 90% month-over-month
- **Alert Threshold (Warn):** Retention rate < 85% in any region in a given month
- **Alert Threshold (Critical):** Retention rate < 75% company-wide or in any single region
- **Dashboard Widget Type:** KPI card (current month retention %) + trend line (12-month rolling) + regional bar chart (retention % by region) + churned accounts table
- **Owner:** VP Sales
- **MoSCoW:** Could Have

---

### KPI-C04: Revenue Concentration Risk

- **Business Question:** What percentage of total revenue comes from our top 5 retailers — and is FreshSip becoming dangerously dependent on a small number of accounts?
- **Formula (SQL):**
  ```sql
  -- Top 5 retailer revenue
  SUM(CASE WHEN revenue_rank <= 5 THEN retailer_net_revenue ELSE 0 END)
  / NULLIF(SUM(retailer_net_revenue), 0) * 100
  ```
  Computed from `gld_freshsip.customers_top_retailers` using pre-ranked data, or directly from `slv_freshsip.sales_transactions` joined to `slv_freshsip.customers`.
- **Gold Layer Table:** `gld_freshsip.customers_concentration_risk`
- **Source Tables:**
  - `gld_freshsip.customers_top_retailers`
  - `slv_freshsip.sales_transactions`
  - `slv_freshsip.customers`
- **Key Columns:** `retailer_id`, `retailer_net_revenue`, `revenue_rank`, `transaction_date`
- **Granularity:** Monthly, company-wide; year-to-date snapshot also computed
- **Refresh Frequency:** Monthly (first business day of each month); daily during board presentation period
- **Target:** Top 5 retailers account for <= 25% of total revenue
- **Alert Threshold (Warn):** Top 5 retailers account for > 30% of total revenue
- **Alert Threshold (Critical):** Top 5 retailers account for > 40% of total revenue, or any single retailer exceeds 15%
- **Dashboard Widget Type:** KPI card (top-5 concentration %) + donut chart (top 5 vs. retailers 6-20 vs. long tail) + trend line (monthly concentration % over 12 months)
- **Owner:** CEO
- **MoSCoW:** Should Have

---

## KPI Summary Table

| KPI ID | KPI Name | Domain | Gold Table | Refresh | MoSCoW | Owner |
|---|---|---|---|---|---|---|
| KPI-S01 | Daily Revenue | Sales | `gld_freshsip.sales_daily_revenue` | Hourly | Must Have | VP Sales |
| KPI-S02 | Revenue vs. Prior Month (MoM %) | Sales | `gld_freshsip.sales_period_comparison` | Daily | Must Have | VP Sales |
| KPI-S03 | Revenue vs. Prior Year (YoY %) | Sales | `gld_freshsip.sales_period_comparison` | Daily | Must Have | CEO |
| KPI-S04 | Gross Margin by SKU | Sales | `gld_freshsip.sales_gross_margin_sku` | Daily | Must Have | VP Sales |
| KPI-I01 | Current Stock Level | Inventory | `gld_freshsip.inventory_stock_levels` | Hourly | Must Have | Supply Chain Manager |
| KPI-I02 | Inventory Turnover Rate | Inventory | `gld_freshsip.inventory_turnover` | Weekly | Should Have | Supply Chain Manager |
| KPI-I03 | Days Sales of Inventory (DSI) | Inventory | `gld_freshsip.inventory_dsi` | Daily | Must Have | Supply Chain Manager |
| KPI-I04 | Reorder Alert Flag | Inventory | `gld_freshsip.inventory_stock_levels` | Hourly | Must Have | Supply Chain Manager |
| KPI-P01 | Batch Yield Rate | Production | `gld_freshsip.production_yield` | Micro-batch (5 min) | Should Have | VP Operations |
| KPI-P02 | Quality Check Pass Rate | Production | `gld_freshsip.production_quality` | Daily | Should Have | VP Operations |
| KPI-P03 | Downtime Hours | Production | `gld_freshsip.production_downtime` | Daily | Should Have | VP Operations |
| KPI-P04 | Batch Traceability Index | Production | `gld_freshsip.production_traceability` | Daily | Could Have | VP Operations |
| KPI-D01 | On-Time Delivery % | Distribution | `gld_freshsip.distribution_otd` | Daily | Should Have | VP Operations |
| KPI-D02 | Order Fulfillment Rate | Distribution | `gld_freshsip.distribution_fulfillment` | Daily | Should Have | VP Operations |
| KPI-D03 | Cost Per Case Delivered | Distribution | `gld_freshsip.distribution_cost` | Weekly | Should Have | VP Operations |
| KPI-D04 | Worst-Performing Routes Score | Distribution | `gld_freshsip.distribution_route_performance` | Weekly | Could Have | VP Operations |
| KPI-C01 | Top 20 Retailers by Revenue | Customers | `gld_freshsip.customers_top_retailers` | Weekly | Should Have | VP Sales |
| KPI-C02 | Customer Acquisition Cost (CAC) | Customers | `gld_freshsip.customers_cac` | Monthly | Could Have | VP Sales |
| KPI-C03 | Retailer Retention Rate | Customers | `gld_freshsip.customers_retention` | Monthly | Could Have | VP Sales |
| KPI-C04 | Revenue Concentration Risk | Customers | `gld_freshsip.customers_concentration_risk` | Monthly | Should Have | CEO |

---

*Registry maintained by the Product Owner. Any change to a formula, target, or threshold requires a version increment and stakeholder sign-off before implementation.*
