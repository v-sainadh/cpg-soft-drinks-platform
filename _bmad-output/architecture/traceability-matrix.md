# Traceability Matrix — FreshSip Beverages CPG Data Platform

**Version:** 1.0
**Date:** 2026-04-05
**Status:** Draft — Phase 3 Solutioning Validation
**Author:** Product Owner Agent
**Validated Against:**
- KPI Registry: `_bmad-output/requirements/kpi-definitions.md`
- PRD: `_bmad-output/requirements/PRD-001-freshsip-cpg-platform.md`
- Gold Schema: `_bmad-output/architecture/schema-gold.md`
- Silver Schema: `_bmad-output/architecture/schema-silver.md`
- Architecture Overview: `_bmad-output/architecture/architecture-overview.md`

---

## Domain Coverage Summary

| Domain | KPIs Required | KPIs Fully Covered | KPIs Partial | KPIs with Gap | Silver Tables Defined | Status |
|---|---|---|---|---|---|---|
| Sales | 4 (S01–S04) | 3 | 0 | 1 (S02/S03 formula note) | 3 of 3 defined | PARTIAL |
| Inventory | 4 (I01–I04) | 3 | 1 | 0 | 2 of 2 defined | PARTIAL |
| Production | 4 (P01–P04) | 2 | 1 | 1 (P03 source mismatch; P04 join gap) | 2 of 2 defined | PARTIAL |
| Distribution | 4 (D01–D04) | 3 | 0 | 1 (D04 stateful logic) | 1 of 1 defined | PARTIAL |
| Customers | 4 (C01–C04) | 3 | 1 | 0 (C02 spend source concern) | 2 of 2 defined | PARTIAL |
| **Total** | **20** | **14** | **2** | **4** | | |

> **Overall status: 14/20 KPIs fully covered. 6 KPIs carry gaps of varying severity.**

---

## Full Traceability Matrix

| PRD Story ID | Business Requirement | KPI ID | KPI Name | Silver Source Tables | Gold Table | Dashboard View | Status | Gaps |
|---|---|---|---|---|---|---|---|---|
| CPG-006 | Daily net revenue by category, region, channel; hourly refresh | KPI-S01 | Daily Revenue | `slv_freshsip.sales_transactions`, `slv_freshsip.sales_returns`, `slv_freshsip.ref_products` | `gld_freshsip.sales_daily_revenue` | Sales View | COVERED | None |
| CPG-006 | MoM revenue comparison by category and region | KPI-S02 | Revenue vs. Prior Month (MoM %) | `gld_freshsip.sales_daily_revenue` (Gold-on-Gold) | `gld_freshsip.sales_period_comparison` | Sales View | COVERED | KPI sources Gold table not Silver; acceptable by design. S02 uses `channel` column which is present. |
| CPG-006 | YoY revenue comparison by channel | KPI-S03 | Revenue vs. Prior Year (YoY %) | `gld_freshsip.sales_daily_revenue` (Gold-on-Gold) | `gld_freshsip.sales_period_comparison` | Sales View | COVERED | None |
| CPG-007 | Gross margin by SKU, weekly; margin alert flags | KPI-S04 | Gross Margin by SKU | `slv_freshsip.sales_transactions`, `slv_freshsip.sales_returns`, `slv_freshsip.ref_products` | `gld_freshsip.sales_gross_margin_sku` | Sales View | COVERED | None — all columns (`unit_price`, `quantity_sold`, `standard_cost_per_unit`, `return_amount`, `sku_id`, `product_category`, `transaction_date`) confirmed present in Silver source tables |
| CPG-011 | Current stock per SKU per warehouse; hourly | KPI-I01 | Current Stock Level | `slv_freshsip.inventory_stock`, `slv_freshsip.ref_reorder_points`, `slv_freshsip.ref_products` | `gld_freshsip.inventory_stock_levels` | Inventory View | COVERED | None — `units_on_hand`, `sku_id`, `warehouse_id`, `snapshot_timestamp` confirmed present |
| CPG-010 | Inventory turnover by warehouse; trailing 30-day window | KPI-I02 | Inventory Turnover Rate | `slv_freshsip.inventory_stock`, `slv_freshsip.sales_transactions`, `slv_freshsip.ref_products` | `gld_freshsip.inventory_turnover` | Inventory View | PARTIAL | GAP-03 (medium): Gold SQL in KPI-I02 contains a variable alias error (`s.transaction_date` references alias `s` which is not defined in the CTE; should be `t.transaction_date`). Alert column in PRD story CPG-010 AC-3 references `turnover_alert_flag`; Gold schema defines `turnover_warn_flag`. Column name mismatch between PRD and architecture. |
| CPG-011 | DSI per SKU per warehouse; daily | KPI-I03 | Days Sales of Inventory (DSI) | `slv_freshsip.inventory_stock`, `slv_freshsip.sales_transactions` | `gld_freshsip.inventory_dsi` | Inventory View | PARTIAL | GAP-04 (medium): KPI registry granularity is "daily by `sku_id` and `warehouse_id`" but the Gold SQL avg_daily_sales CTE aggregates only by `sku_id` — warehouse_id dimension is missing from the daily sales average subquery. This means all warehouses holding the same SKU receive the same avg_daily_sales figure regardless of warehouse-level sales mix. Formula deviation from KPI registry. |
| CPG-011 | Reorder alert flag per SKU per warehouse; hourly | KPI-I04 | Reorder Alert Flag | `slv_freshsip.inventory_stock`, `slv_freshsip.ref_reorder_points` | `gld_freshsip.inventory_stock_levels` | Inventory View | COVERED | None — shares table with KPI-I01; `reorder_alert_flag` and `stockout_flag` columns both present |
| CPG-014 | Batch yield rate per batch; micro-batch refresh | KPI-P01 | Batch Yield Rate | `slv_freshsip.production_batches`, `slv_freshsip.ref_products` | `gld_freshsip.production_yield` | Production View | COVERED | None — all required columns confirmed present; KPI-P01 formula matches registry exactly |
| CPG-014 | Daily QC pass rate by production line | KPI-P02 | Quality Check Pass Rate | `slv_freshsip.production_batches`, `slv_freshsip.ref_products` | `gld_freshsip.production_quality` | Production View | COVERED | None — `qc_status`, `batch_id`, `production_line_id`, `product_category`, `batch_end_ts` all confirmed present |
| CPG-015 | Unplanned downtime hours by production line; daily | KPI-P03 | Downtime Hours | `slv_freshsip.production_events` | `gld_freshsip.production_downtime` | Production View | PARTIAL | GAP-05 (low): KPI registry lists `slv_freshsip.production_batches` as a source table alongside `production_events`, but the Gold schema `production_downtime` sources only from `slv_freshsip.production_events`. In practice the computation does not require `production_batches` directly (events contain the line and timing data), so this is a registry inconsistency rather than a missing dependency. The KPI is computable. Recommend updating registry to remove `production_batches` from KPI-P03 source tables. |
| CPG-015 | Batch traceability index; batch-to-shipment-to-retailer chain | KPI-P04 | Batch Traceability Index | `slv_freshsip.production_batches`, `slv_freshsip.shipments`, `slv_freshsip.customers` | `gld_freshsip.production_traceability` | Production View | GAP | GAP-02 (critical): The Gold SQL joins `production_batches` to `shipments` using `b.batch_id = s.order_id`. The column `batch_id` does not exist on `slv_freshsip.shipments` — only `order_id` exists. There is no explicit linkage column between a production batch and the shipment that carries its output. The join predicate `b.batch_id = s.order_id` is semantically incorrect and will produce wrong or empty results unless the source data populates `order_id` with `batch_id` values by convention. No business rule in either the KPI registry or PRD establishes this equivalence. KPI-P04 is not reliably computable without a `batch_id` foreign key on `slv_freshsip.shipments`. |
| CPG-018 | On-time delivery % by channel and region; daily | KPI-D01 | On-Time Delivery % | `slv_freshsip.shipments`, `slv_freshsip.customers` | `gld_freshsip.distribution_otd` | Distribution View | COVERED | Note: Gold SQL sources only from `slv_freshsip.shipments` (no join to `customers`); the KPI registry lists both. Omitting `customers` join is acceptable since `channel` and `region` are denormalized onto `shipments`. Not a functional gap. |
| CPG-018 | Order fulfillment rate by channel; daily | KPI-D02 | Order Fulfillment Rate | `slv_freshsip.shipments` | `gld_freshsip.distribution_fulfillment` | Distribution View | COVERED | Note: CPG-018 AC-3 references `fulfillment_alert_flag` but Gold schema defines `fulfillment_warn_flag`. Column name mismatch between PRD acceptance criterion and architecture. Engineers should implement `fulfillment_warn_flag` as specified in schema (PRD AC wording should be updated). GAP-07 (low). |
| CPG-019 | Cost per case by region and route; weekly | KPI-D03 | Cost Per Case Delivered | `slv_freshsip.shipments` | `gld_freshsip.distribution_cost` | Distribution View | COVERED | Note: CPG-019 AC-1 references column `cost_per_case_usd`; Gold schema defines `cost_per_case`. GAP-08 (low) — column name mismatch in PRD acceptance criteria. |
| CPG-019 | Worst-performing routes by OTD and cost; weekly top 10 | KPI-D04 | Worst-Performing Routes Score | `slv_freshsip.shipments` | `gld_freshsip.distribution_route_performance` | Distribution View | GAP | GAP-01 (high): Gold SQL hardcodes `consecutive_weeks_in_worst10 = 0` (stateful counter not implemented). KPI registry critical alert threshold requires "3 consecutive weeks in worst-10 list" but this requires stateful lookback over prior Gold table partitions. `route_critical_flag` only evaluates `route_otd_pct < 70` — the consecutive-weeks condition is absent. Full critical-threshold alert for KPI-D04 is not computable as designed. |
| CPG-022 | Top 20 retailers by revenue; weekly with rank movement | KPI-C01 | Top 20 Retailers by Revenue | `slv_freshsip.sales_transactions`, `slv_freshsip.sales_returns`, `slv_freshsip.customers` | `gld_freshsip.customers_top_retailers` | Customer View | COVERED | Note: Gold SQL sets `prior_period_rank = NULL` and `rank_movement = NULL` — rank movement computation is deferred. CPG-022 AC-4 requires rank change column. GAP-09 (medium): prior-period rank is not computed in current Gold SQL. |
| CPG-023 | CAC by retail segment; monthly | KPI-C02 | Customer Acquisition Cost (CAC) | `slv_freshsip.customers`, `slv_freshsip.sales_spend` | `gld_freshsip.customers_cac` | Customer View | PARTIAL | GAP-06 (high): `slv_freshsip.sales_spend` is sourced from `brz_freshsip.erp_customers_raw` (spend columns extracted as part of the customer record). No dedicated Bronze table (`brz_freshsip.erp_spend_raw`) is defined. If the ERP customer CSV does not contain spend allocation columns, `sales_spend` will be empty and CAC will compute as zero. The architecture depends on the assumption that trade spend, broker commission, and field sales costs are columns within the customer ERP extract. This assumption is not documented as a business rule or validated in any acceptance criterion. |
| CPG-023 | Retailer retention rate by region; monthly | KPI-C03 | Retailer Retention Rate | `slv_freshsip.customers`, `slv_freshsip.sales_transactions` | `gld_freshsip.customers_retention` | Customer View | COVERED | None — formula, columns, and source tables all confirmed present and correctly implemented |
| CPG-023 | Revenue concentration from top 5 retailers; monthly | KPI-C04 | Revenue Concentration Risk | `gld_freshsip.customers_top_retailers`, `slv_freshsip.sales_transactions`, `slv_freshsip.customers` | `gld_freshsip.customers_concentration_risk` | Customer View | COVERED | None — Gold SQL computes directly from Silver (not from `customers_top_retailers` as optionally specified in registry); both paths are acceptable per registry definition |

---

## PRD Story Table Name Reference Check

The following PRD user stories reference specific table names in their acceptance criteria. This section validates each named table exists in the architecture.

| Story ID | AC Reference | Table or Column Named | Exists in Architecture | Status |
|---|---|---|---|---|
| CPG-003 | AC-1 | `brz_freshsip.pos_transactions_raw` | Yes — defined in Bronze schema | PASS |
| CPG-004 | AC-1 | `brz_freshsip.erp_sales_raw` | Yes — defined in Bronze schema | PASS |
| CPG-004 | AC-4 | `brz_freshsip.erp_returns_raw` | Yes — defined in Bronze schema | PASS |
| CPG-005 | AC-1 | `slv_freshsip.sales_transactions` | Yes — fully defined in Silver schema | PASS |
| CPG-005 | AC-4 | `slv_freshsip.sales_returns` | Yes — fully defined in Silver schema | PASS |
| CPG-006 | AC-1 | `gld_freshsip.sales_daily_revenue` | Yes — fully defined in Gold schema | PASS |
| CPG-007 | AC-1 | `gld_freshsip.sales_gross_margin_sku` | Yes — fully defined in Gold schema | PASS |
| CPG-007 | AC-3 | `margin_alert_flag` column | Yes — column present in `sales_gross_margin_sku` | PASS |
| CPG-007 | AC-4 | `is_partial_week` column | Yes — column present in `sales_gross_margin_sku` | PASS |
| CPG-008 | AC-1 | `brz_freshsip.erp_inventory_raw` | Yes — defined in Bronze schema | PASS |
| CPG-009 | AC-1 | `slv_freshsip.inventory_stock` | Yes — fully defined in Silver schema | PASS |
| CPG-009 | AC-3 | `slv_freshsip.ref_reorder_points` | Yes — fully defined in Silver schema | PASS |
| CPG-010 | AC-1 | `gld_freshsip.inventory_turnover` | Yes — fully defined in Gold schema | PASS |
| CPG-010 | AC-3 | `turnover_alert_flag` column | **FAIL** — Gold schema defines `turnover_warn_flag`, not `turnover_alert_flag` | GAP-07b |
| CPG-011 | AC-1 | `gld_freshsip.inventory_dsi` | Yes — fully defined in Gold schema | PASS |
| CPG-011 | AC-3 | `gld_freshsip.inventory_stock_levels` | Yes — fully defined in Gold schema | PASS |
| CPG-012 | AC-1 | `brz_freshsip.iot_production_raw` | **FAIL** — architecture defines `brz_freshsip.iot_sensor_events_raw`, not `iot_production_raw` | GAP-10 |
| CPG-013 | AC-1 | `slv_freshsip.production_batches` | Yes — fully defined in Silver schema | PASS |
| CPG-014 | AC-1 | `gld_freshsip.production_yield` | Yes — fully defined in Gold schema | PASS |
| CPG-014 | AC-1 | `batch_yield_rate_pct` column | **FAIL** — Gold schema defines `yield_rate_pct`, not `batch_yield_rate_pct` | GAP-11 |
| CPG-014 | AC-3 | `gld_freshsip.production_quality` | Yes — fully defined in Gold schema | PASS |
| CPG-014 | AC-3 | `quality_pass_rate_pct` column | **FAIL** — Gold schema defines `qc_pass_rate_pct`, not `quality_pass_rate_pct` | GAP-12 |
| CPG-014 | AC-3 | `quality_alert_flag` column | **FAIL** — Gold schema defines `qc_warn_flag`, not `quality_alert_flag` | GAP-12 |
| CPG-015 | AC-1 | `gld_freshsip.production_downtime` | Yes — fully defined in Gold schema | PASS |
| CPG-015 | AC-3 | `gld_freshsip.production_traceability` | Yes — fully defined in Gold schema | PASS |
| CPG-016 | AC-1 | `brz_freshsip.logistics_shipments_raw` | Yes — defined in Bronze schema | PASS |
| CPG-017 | AC-1 | `slv_freshsip.shipments` | Yes — fully defined in Silver schema | PASS |
| CPG-018 | AC-1 | `gld_freshsip.distribution_otd` | Yes — fully defined in Gold schema | PASS |
| CPG-018 | AC-3 | `gld_freshsip.distribution_fulfillment` | Yes — fully defined in Gold schema | PASS |
| CPG-018 | AC-3 | `fulfillment_alert_flag` column | **FAIL** — Gold schema defines `fulfillment_warn_flag`, not `fulfillment_alert_flag` | GAP-07 |
| CPG-019 | AC-1 | `gld_freshsip.distribution_cost` | Yes — fully defined in Gold schema | PASS |
| CPG-019 | AC-1 | `cost_per_case_usd` column | **FAIL** — Gold schema defines `cost_per_case`, not `cost_per_case_usd` | GAP-08 |
| CPG-019 | AC-3 | `gld_freshsip.distribution_route_performance` | Yes — fully defined in Gold schema | PASS |
| CPG-020 | AC-1 | `brz_freshsip.erp_customers_raw` | Yes — defined in Bronze schema | PASS |
| CPG-021 | AC-1 | `slv_freshsip.customers` | Yes — fully defined in Silver schema | PASS |
| CPG-021 | AC-1 | `effective_start_date` column | **FAIL** — Silver schema uses `valid_from`, not `effective_start_date`; AC-4 also uses `effective_start_date` | GAP-13 |
| CPG-022 | AC-1 | `gld_freshsip.customers_top_retailers` | Yes — fully defined in Gold schema | PASS |
| CPG-022 | AC-4 | `rank_change` column | **FAIL** — Gold schema defines `rank_movement`, not `rank_change` | GAP-09 |
| CPG-023 | AC-1 | `gld_freshsip.customers_cac` | Yes — fully defined in Gold schema | PASS |
| CPG-023 | AC-3 | `gld_freshsip.customers_retention` | Yes — fully defined in Gold schema | PASS |
| CPG-023 | AC-4 | `gld_freshsip.customers_concentration_risk` | Yes — fully defined in Gold schema | PASS |
| CPG-023 | AC-4 | `top5_concentration_pct` column | Yes — column present in `customers_concentration_risk` | PASS |
| CPG-029 | AC-1 | `slv_freshsip.pipeline_dq_log` | **FAIL** — no `pipeline_dq_log` table is defined in Silver schema | GAP-14 |
| CPG-030 | AC-1 | `gld_freshsip.sales_daily_revenue` | Yes | PASS |
| CPG-030 | AC-2 | `gld_freshsip.inventory_stock_levels` | Yes | PASS |
| CPG-030 | AC-3 | `gld_freshsip.customers_top_retailers` | Yes | PASS |

---

## Gap Registry

All gaps are listed below with severity rating, description, affected KPI or story, and recommended resolution.

---

### GAP-01: KPI-D04 — Consecutive Weeks Stateful Counter Not Implemented

**Severity:** HIGH
**KPI Affected:** KPI-D04 (Worst-Performing Routes Score)
**PRD Story:** CPG-019
**Description:** The Gold SQL for `gld_freshsip.distribution_route_performance` hardcodes `consecutive_weeks_in_worst10 = 0` for all rows. The KPI-D04 critical alert threshold requires that any route appearing in the worst-10 list for 3 or more consecutive weeks triggers `route_critical_flag = true`. Since the counter is always zero, the consecutive-weeks condition of the critical alert is permanently inactive. The column `consecutive_weeks_in_worst10` stores only zeros; the `route_critical_flag` evaluates only `route_otd_pct < 70`, missing half its alerting specification.
**Resolution:** Implement a lookback join in the Gold pipeline that reads the prior 2 weeks of `distribution_route_performance` and computes consecutive-week count via a window function or incremental MERGE pattern. The Gold overwrite strategy must be changed from full-overwrite to MERGE to preserve historical route-week rows for lookback. This requires an open question on whether full history is retained in the partition scheme or if a separate state table is maintained.
**Flagged By:** Data Architect (confirmed by Product Owner)

---

### GAP-02: KPI-P04 — Missing `batch_id` Foreign Key on `slv_freshsip.shipments`

**Severity:** CRITICAL
**KPI Affected:** KPI-P04 (Batch Traceability Index)
**PRD Story:** CPG-015
**Description:** The KPI-P04 formula requires joining `slv_freshsip.production_batches` to `slv_freshsip.shipments` on `batch_id` to construct the complete batch → shipment → retailer traceability chain. However, `slv_freshsip.shipments` has no `batch_id` column. The Gold SQL uses the join condition `b.batch_id = s.order_id`, implicitly assuming that a shipment's `order_id` equals the production `batch_id`. This assumption is undocumented in any business rule or data dictionary. In a real supply chain, an `order_id` links a customer order to a shipment; a `batch_id` links a production run to finished goods. They are structurally different identifiers. Without a validated linkage key, KPI-P04 will produce incorrect traceability results or will return an empty trace chain for all batches.
**Resolution:** One of the following must be resolved:
  (a) Add a `batch_id` column to `slv_freshsip.shipments` sourced from the logistics CSV (if the logistics partner includes the production batch reference on shipment records), or
  (b) Add a bridge table `slv_freshsip.batch_shipment_link` (batch_id → order_id mapping sourced from ERP or production system), or
  (c) Formally document as a business rule that `order_id` on a shipment equals the `batch_id` of the production batch that fulfills it, and add a data quality check to validate this assumption.
**Flagged By:** Data Architect (confirmed by Product Owner)

---

### GAP-03: KPI-I02 — SQL CTE Alias Error in `inventory_turnover` Computation

**Severity:** HIGH
**KPI Affected:** KPI-I02 (Inventory Turnover Rate)
**PRD Story:** CPG-010
**Description:** In the KPI-I02 Gold SQL, the `cogs_30d` CTE uses `s.transaction_date` and `i.warehouse_id` where `s` is not defined (the CTE uses aliases `t` for `sales_transactions` and `i` for `inventory_stock`). The line `s.transaction_date` will produce a compile error or silently fail. Additionally, CPG-010 AC-3 references `turnover_alert_flag = true` but the Gold schema defines the column as `turnover_warn_flag`. This is both a SQL error and a column name mismatch between the PRD and the architecture.
**Resolution:**
  (a) Fix the SQL alias: replace `s.transaction_date` with `t.transaction_date` in the `cogs_30d` CTE.
  (b) Align column naming: either rename `turnover_warn_flag` to `turnover_alert_flag` in the Gold schema, or update CPG-010 AC-3 to reference `turnover_warn_flag`. The Product Owner recommends updating the PRD acceptance criterion to match the schema (schema is the implementation-level document; PRD AC wording should use `turnover_warn_flag`).
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-04: KPI-I03 — DSI avg_daily_sales CTE Missing `warehouse_id` Dimension

**Severity:** MEDIUM
**KPI Affected:** KPI-I03 (Days Sales of Inventory)
**PRD Story:** CPG-011
**Description:** The KPI registry specifies DSI granularity as "daily by `sku_id` and `warehouse_id`" because different warehouses can have materially different sales velocities for the same SKU (a SKU sold heavily in the Northeast may move slowly in the Southwest). The Gold SQL `avg_daily_sales` CTE aggregates `SUM(quantity_sold) / 30.0` grouped only by `sku_id`, omitting `warehouse_id`. This means every warehouse holding the same SKU receives the same average daily sales figure, ignoring warehouse-specific demand patterns. For warehouses with below-average sales velocity, DSI will be understated (false stockout warning); for warehouses with above-average velocity, DSI will be overstated (missed stockout alert).
**Resolution:** Add `warehouse_id` to the join between `sales_transactions` and `inventory_stock` in the avg_daily_sales CTE, and group by both `sku_id` and `warehouse_id`. Note: this requires the sales transaction data to contain or resolve to a `warehouse_id` — the current `slv_freshsip.sales_transactions` schema does not include `warehouse_id` directly. The recommended approach is to join through `slv_freshsip.inventory_stock` as a proxy for warehouse assignment, or accept that DSI is computed at SKU level (not SKU × warehouse) and update the KPI registry granularity accordingly. This is an open question that requires VP Operations or Supply Chain Manager input.
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-05: KPI-P03 — Registry Lists `production_batches` as Source; Schema Does Not Use It

**Severity:** LOW
**KPI Affected:** KPI-P03 (Downtime Hours)
**PRD Story:** CPG-015
**Description:** The KPI registry lists `slv_freshsip.production_batches` as a source table for KPI-P03 alongside `slv_freshsip.production_events`. The Gold schema sources `production_downtime` exclusively from `slv_freshsip.production_events`, which is correct — downtime events are recorded in `production_events`, not `production_batches`. The registry entry is inaccurate but does not block computation. The KPI is fully computable from `production_events` alone.
**Resolution:** Update the KPI registry (kpi-definitions.md) for KPI-P03 to remove `slv_freshsip.production_batches` from the source tables list. The correct and complete source is `slv_freshsip.production_events` only.
**Flagged By:** Product Owner (registry consistency gap)

---

### GAP-06: KPI-C02 — CAC Spend Data Source Not Independently Verified

**Severity:** HIGH
**KPI Affected:** KPI-C02 (Customer Acquisition Cost)
**PRD Story:** CPG-023
**Description:** `slv_freshsip.sales_spend` is sourced from `brz_freshsip.erp_customers_raw` with the description "spend columns extracted." No dedicated Bronze table for spend data (`brz_freshsip.erp_spend_raw`) is defined. The assumption is that the daily customer ERP CSV contains `trade_spend_usd`, `broker_commission_usd`, and `field_sales_cost_usd` as columns alongside retailer profile attributes. If the ERP customer CSV does not contain these spend allocation columns — a common real-world scenario where spend data lives in a separate ERP module — then `slv_freshsip.sales_spend` will contain zero-value spend rows or be empty. CAC will compute as $0 for all segments, which is business-meaningless and will pass all data quality checks without error because the schema permits valid zero values. No acceptance criterion in the PRD validates that `sales_spend` rows contain non-zero values.
**Resolution:** Document explicitly (as a business rule in the PRD or as a schema assumption) that the ERP customer CSV contains spend columns. Add a data quality rule to `slv_freshsip.sales_spend`: warn if more than 20% of rows have `total_acquisition_cost_usd = 0` for any period. In the synthetic data generator, ensure spend values are non-zero for new account activations. Consider adding a dedicated Bronze source for spend data if the ERP system provides a separate spend export.
**Flagged By:** Data Architect (confirmed and elaborated by Product Owner)

---

### GAP-07: Column Name Mismatches in PRD Acceptance Criteria vs. Gold Schema (Alert Flags)

**Severity:** LOW
**KPI Affected:** KPI-D02, KPI-I02
**PRD Stories:** CPG-010, CPG-018
**Description:** Multiple PRD acceptance criteria reference column names that do not match the Gold schema definition. These mismatches will cause acceptance test failures during UAT if tests are written directly from the PRD:
  - CPG-010 AC-3: references `turnover_alert_flag`; Gold schema defines `turnover_warn_flag`
  - CPG-018 AC-3: references `fulfillment_alert_flag`; Gold schema defines `fulfillment_warn_flag`
**Resolution:** Update the PRD acceptance criteria to use the column names as defined in the Gold schema. The schema is the authoritative source for column names. No schema change required.
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-08: CPG-019 AC-1 References `cost_per_case_usd`; Schema Defines `cost_per_case`

**Severity:** LOW
**KPI Affected:** KPI-D03
**PRD Story:** CPG-019
**Description:** CPG-019 AC-1 states "a non-null `cost_per_case_usd` column." The Gold schema defines the column as `cost_per_case` (no `_usd` suffix). Automated acceptance tests built directly from this AC will fail column-existence checks.
**Resolution:** Update CPG-019 AC-1 to reference `cost_per_case` as defined in `gld_freshsip.distribution_cost`.
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-09: KPI-C01 — `prior_period_rank` and `rank_movement` Not Computed in Gold SQL

**Severity:** MEDIUM
**KPI Affected:** KPI-C01 (Top 20 Retailers by Revenue)
**PRD Story:** CPG-022
**Description:** CPG-022 AC-4 requires that `rank_change` (the schema names it `rank_movement`) equals `prior_rank - current_rank`. The Gold schema DDL defines both `prior_period_rank` and `rank_movement` columns. However, the Gold computation SQL sets both to `NULL` for every row. The prior-period rank lookback is not implemented in the Gold pipeline. This means the rank-movement indicator on the dashboard will always show NULL/blank, and CPG-022 AC-4 will always fail.
**Resolution:** Implement a self-join against the prior week's `customers_top_retailers` partition when computing the current week's rows. The Gold pipeline must read the previous `report_period` rows to retrieve `revenue_rank` per `retailer_id` for rank movement computation. This requires the overwrite strategy to preserve prior-period partitions (currently designed as overwrite-per-period, which is correct — prior period is retained by partition key, only the current period overwrites).
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-10: CPG-012 AC-1 References `brz_freshsip.iot_production_raw`; Architecture Defines `brz_freshsip.iot_sensor_events_raw`

**Severity:** LOW
**KPI Affected:** KPI-P01, KPI-P02, KPI-P03, KPI-P04 (all production KPIs depend on this Bronze source)
**PRD Story:** CPG-012
**Description:** CPG-012 AC-1 names the Bronze IoT target table as `brz_freshsip.iot_production_raw`. The architecture overview, Bronze schema, and Silver pipeline description consistently use `brz_freshsip.iot_sensor_events_raw`. If acceptance tests are coded using the name from the PRD, they will fail to find the table. If the pipeline developer follows the PRD rather than the architecture, the wrong table will be created.
**Resolution:** Update CPG-012 AC-1 to reference `brz_freshsip.iot_sensor_events_raw`. The architecture schema is the authoritative source.
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-11: CPG-014 AC-1 References `batch_yield_rate_pct`; Gold Schema Defines `yield_rate_pct`

**Severity:** LOW
**KPI Affected:** KPI-P01
**PRD Story:** CPG-014
**Description:** CPG-014 AC-1 requires "a non-null `batch_yield_rate_pct` column" in `gld_freshsip.production_yield`. The Gold schema defines the column as `yield_rate_pct`.
**Resolution:** Update CPG-014 AC-1 to reference `yield_rate_pct`.
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-12: CPG-014 AC-3 References `quality_pass_rate_pct` and `quality_alert_flag`; Gold Schema Uses `qc_pass_rate_pct` and `qc_warn_flag`

**Severity:** LOW
**KPI Affected:** KPI-P02
**PRD Story:** CPG-014
**Description:** CPG-014 AC-3 references `quality_pass_rate_pct < 96%` and `quality_alert_flag = true`. The Gold schema `production_quality` defines `qc_pass_rate_pct` and `qc_warn_flag`.
**Resolution:** Update CPG-014 AC-3 to reference `qc_pass_rate_pct` and `qc_warn_flag` as defined in the Gold schema.
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-13: CPG-021 AC-1 and AC-4 Reference `effective_start_date`; Silver Schema Uses `valid_from`

**Severity:** LOW
**KPI Affected:** KPI-C01, KPI-C02, KPI-C03, KPI-C04 (all depend on `slv_freshsip.customers`)
**PRD Story:** CPG-021
**Description:** CPG-021 AC-1 states "a new row with `effective_start_date` = today" and AC-4 states "`effective_start_date` = `account_activation_date`". The Silver schema for `slv_freshsip.customers` uses the SCD Type 2 column name `valid_from`, not `effective_start_date`. Similarly, `effective_end_date` in AC-1 should be `valid_to`.
**Resolution:** Update CPG-021 AC-1 and AC-4 to reference `valid_from` and `valid_to` as defined in `slv_freshsip.customers`.
**Flagged By:** Product Owner (new gap identified during validation)

---

### GAP-14: `slv_freshsip.pipeline_dq_log` Referenced in CPG-029 Has No Schema Definition

**Severity:** MEDIUM
**KPI Affected:** N/A (infrastructure)
**PRD Story:** CPG-029
**Description:** CPG-029 AC-1 requires that every Silver pipeline run writes a row to `slv_freshsip.pipeline_dq_log` with columns `pipeline_name`, `run_ts`, `table_name`, `rows_processed`, `rows_rejected`, `null_rate_by_column` (JSON), `duplicate_count`, and `run_status`. This table is referenced in three other stories (CPG-029 AC-2, AC-3, AC-4) and is the centerpiece of the data quality framework. However, no DDL or table definition for `slv_freshsip.pipeline_dq_log` exists in `schema-silver.md`. Without a defined schema, engineers have no specification to implement against and no standard for what the monitoring table looks like.
**Resolution:** Add a full table definition for `slv_freshsip.pipeline_dq_log` to `schema-silver.md`, including all columns specified in CPG-029 AC-1. This is blocking for the DQ framework story and should be treated as MEDIUM severity since it is infrastructure rather than a KPI.
**Flagged By:** Product Owner (new gap identified during validation)

---

## Dimension Table Sourcing Note

`gld_freshsip.dim_warehouse` is described in the Gold schema as sourced from "distinct `warehouse_id` values in `slv_freshsip.inventory_stock` + reference file." The term "reference file" is undefined — no Bronze table, no Silver table, and no seed file path is specified for warehouse master attributes (`warehouse_name`, `region`, `state`, `warehouse_type`, `capacity_units`). Inventory stock records contain only `warehouse_id` (a key), not the descriptive warehouse attributes. This was flagged by the Data Architect as the "warehouse reference seeding" gap.

This gap is **MEDIUM severity** for implementation purposes: the dimension can be seeded from a small static configuration file, but the file location, format, and ingestion mechanism are unspecified. If `dim_warehouse` is incomplete (missing name/type attributes), all KPI tables joined to `dim_warehouse` will lack warehouse context. This affects all Inventory, Production, and Distribution fact tables.

**Resolution:** Define a seed file (`config/schemas/warehouse_reference.csv` or equivalent) with all warehouse attribute columns. Add a seeding step to the Gold pipeline initialization that reads this file and populates `dim_warehouse`. Document the file path and expected schema in the architecture.

---

## Validation Summary

| Metric | Count |
|---|---|
| Total KPIs in registry | 20 |
| KPIs with Gold table confirmed | 20 |
| KPIs with all Silver source tables confirmed | 20 |
| KPIs with all required columns confirmed present | 17 |
| KPIs fully COVERED (formula, table, columns all aligned) | 14 |
| KPIs PARTIAL (minor formula or column misalignment) | 3 |
| KPIs with CRITICAL or HIGH gap blocking correct computation | 3 |
| Total gaps identified | 14 |
| CRITICAL gaps | 1 (GAP-02) |
| HIGH gaps | 3 (GAP-01, GAP-03, GAP-06) |
| MEDIUM gaps | 3 (GAP-04, GAP-09, GAP-14) |
| LOW gaps | 7 (GAP-05, GAP-07, GAP-08, GAP-10, GAP-11, GAP-12, GAP-13) |
| PRD story table/column name references checked | 47 |
| PRD table/column name mismatches found | 9 |
| All 5 domains covered by architecture | Yes |
| Silver tables required: 11 | All 11 defined |
| Gold KPI tables required: 20 | All 20 defined |

---

## Recommended Resolution Priority

| Priority | Gap ID | Severity | Action Required Before Implementation |
|---|---|---|---|
| 1 | GAP-02 | CRITICAL | Add `batch_id` linkage to `slv_freshsip.shipments` OR define bridge table OR formally document and validate `batch_id = order_id` equivalence as a business rule. Blocks KPI-P04. |
| 2 | GAP-01 | HIGH | Design stateful consecutive-weeks counter for KPI-D04. Requires architectural decision on lookback pattern. Blocks full KPI-D04 critical alert. |
| 3 | GAP-03 | HIGH | Fix SQL alias error in KPI-I02 Gold pipeline (`s.` → `t.`). Update CPG-010 AC-3 column name. Blocks `inventory_turnover` from executing. |
| 4 | GAP-06 | HIGH | Validate that ERP customer CSV contains spend columns. Add DQ rule for zero-spend detection. Blocks reliable KPI-C02 values. |
| 5 | GAP-14 | MEDIUM | Define `slv_freshsip.pipeline_dq_log` schema in Silver schema doc. Blocks CPG-029 (DQ framework story). |
| 6 | GAP-04 | MEDIUM | Resolve DSI warehouse-level sales velocity question with Supply Chain Manager. Update Gold SQL or update KPI registry granularity. |
| 7 | GAP-09 | MEDIUM | Implement prior-period rank lookback in `customers_top_retailers` Gold pipeline. Blocks CPG-022 AC-4. |
| 8 | Dim Warehouse | MEDIUM | Define warehouse seed file and ingestion mechanism. Blocks `dim_warehouse` population. |
| 9 | GAP-05 | LOW | Update kpi-definitions.md KPI-P03 source tables (registry correction only). |
| 10 | GAP-07, GAP-08, GAP-10, GAP-11, GAP-12, GAP-13 | LOW | Update PRD acceptance criteria column and table names to match architecture. No schema changes required. |

---

*Traceability matrix produced by the Product Owner Agent. All gaps must be resolved or formally accepted (with documented risk) before the Data Engineer begins implementation. CRITICAL and HIGH gaps require stakeholder sign-off before proceeding.*
