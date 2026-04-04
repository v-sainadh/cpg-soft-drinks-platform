# PRD-001: FreshSip Beverages CPG Executive Dashboard Platform

**Version:** 1.0
**Date:** 2026-04-05
**Status:** Draft
**Author:** Product Owner Agent
**Input Artifacts:** `_bmad-output/project-context.md`, `_bmad-output/requirements/product-brief.md`, `_bmad-output/brainstorming/brainstorming-session-2026-04-05-001.md`

---

## 1. Executive Summary

FreshSip Beverages operates across 12 US states in three distribution channels — Retail, Wholesale, and Direct-to-Consumer — across four product categories (carbonated soft drinks, flavored water, energy drinks, and juice blends). Today, leadership has no unified operational view: decisions are made from disconnected ERP exports and spreadsheets, often with day-old or week-old data.

This PRD defines requirements for an end-to-end CPG data platform built on Databricks using Medallion Architecture (Bronze → Silver → Gold). The platform will ingest data from POS systems, ERP exports, IoT production sensors, and logistics partners; apply cleaning and business rule validation in a Silver layer; and surface pre-computed KPIs in a Gold layer powering a near-real-time executive dashboard.

The primary deliverable is a live Databricks AI/BI dashboard covering five operational domains — Sales, Inventory, Production, Distribution, and Customers — that enables the CEO and VP-level leadership to answer key business questions in under two minutes each. The platform will serve as both a daily operational tool and a board presentation showcase demonstrating AI-accelerated data engineering capability. The delivery deadline is a board presentation within four weeks of project start.

---

## 2. Business Objectives

1. **Unified operational visibility:** Provide the CEO and VP leadership with a single dashboard that consolidates performance data across all five operational domains, eliminating reliance on disconnected ERP reports and manual spreadsheets.

2. **Revenue accountability:** Enable the VP Sales to monitor daily net revenue by product category, region, and channel with MoM and YoY trend comparisons, reducing revenue reporting latency from 24-48 hours to 1 hour.

3. **Stockout prevention:** Surface inventory reorder alerts per SKU per warehouse on an hourly refresh cycle, enabling the Supply Chain Manager to initiate replenishment orders before stockouts occur and avoid an estimated $50K-$200K per incident in lost sales.

4. **Production quality monitoring:** Provide VP Operations with per-batch yield rate and quality check pass rate data refreshed within 5 minutes of batch completion, enabling early intervention on failing batches and reducing scrapped batch volume by a target of 10-20%.

5. **Logistics performance accountability:** Enable VP Operations to identify underperforming distribution routes and regions by on-time delivery percentage and cost per case, targeting a 5-15% logistics cost reduction in identified problem lanes.

6. **Customer concentration risk management:** Give the CEO a real-time view of revenue concentration across retailer accounts, including a top-20 retailer ranking and a top-5 concentration risk percentage, enabling proactive account diversification decisions.

7. **Board presentation readiness:** Deliver a live, interactive dashboard accessible via shared Databricks link within four weeks, to be used as the primary exhibit in the board of directors presentation demonstrating the company's AI-driven operational capability.

8. **Platform foundation:** Establish a reusable, extensible Medallion data platform on Databricks with documented schemas, orchestration workflows, CI/CD pipelines, and data quality frameworks that can be extended to support demand forecasting, real SAP integration, and RBAC in future phases.

---

## 3. Scope

### 3.1 In Scope

- Databricks workspace setup: schemas (`brz_freshsip`, `slv_freshsip`, `gld_freshsip`), cluster configuration, and Hive metastore database structure (Unity Catalog as optional upgrade)
- Synthetic data generation: 13 months of historical data covering all five domains in all required formats (CSV, JSON, Parquet for IoT micro-batch)
- Bronze layer ingestion pipelines for: Sales POS (JSON hourly), Sales ERP (CSV daily), Inventory ERP (CSV daily), Production IoT sensors (micro-batch every 5 minutes), Logistics partner (CSV daily), Customer/Retailer ERP (CSV daily)
- Silver layer pipelines: deduplication, null handling, type casting, business rule validation, SCD Type 2 for Customers and Products
- Gold layer KPI tables: all 20 KPIs defined in `_bmad-output/requirements/kpi-definitions.md`
- Databricks AI/BI dashboard: five domain pages (Sales, Inventory, Production, Distribution, Customers)
- Databricks Workflows orchestration: daily batch pipeline (06:00 UTC), hourly POS refresh, and 5-minute micro-batch for Production IoT
- CI/CD pipeline via GitHub Actions and Databricks Asset Bundles (DABs)
- Data quality framework: null rate checks, duplicate key detection, schema validation, row rejection logging
- Genie AI/BI natural language query space (stretch goal, Week 4)
- All synthetic data clearly labeled as "simulated" until real data backfill occurs

### 3.2 Out of Scope

- Real SAP or ERP data integration (live connection or historical backfill from production systems)
- Real-time Kafka or Zerobus streaming ingestion (micro-batch is the approved pattern for this release)
- ML demand forecasting or predictive analytics beyond rule-based reorder alerts
- Role-based access control (all permissioned users see the same dashboard view)
- Automated board deck PDF generation
- Multi-tenant support or deployment for any company other than FreshSip
- Mobile-optimized dashboard layout (desktop-first; mobile is a stretch)
- Integration with external alerting systems (email, Slack) — dashboard-only alerts in this release
- Regional Sales Manager filtered views (secondary user persona deferred to Phase 2)
- Unity Catalog governance features (fine-grained access control, data lineage, column-level tagging) — dependent on Unity Catalog availability on free tier

---

## 4. User Personas and Stories

### 4.1 User Personas

#### Persona 1: CEO (Primary — Executive Decision-Maker)

- **Goal:** Single-screen view of company health across all five domains; credible board presentation exhibit
- **Current pain:** Relies on VP summaries and manually assembled PowerPoint; no ability to drill into data in real time
- **Dashboard use pattern:** Checks dashboard each morning; uses it live during board meeting; asks ad hoc "why" questions
- **Key questions:** Total revenue vs. last week; worst-performing region; any critical quality events today; top 5 retailers and their trend
- **Technical comfort:** Low — needs a clean, self-explanatory UI; no SQL access

#### Persona 2: VP Sales (Primary — Revenue Accountability)

- **Goal:** Monitor daily revenue by product, region, and channel; track against monthly and annual targets; identify top and bottom SKUs
- **Current pain:** POS data arrives 24-48 hours late; no consolidated view across channels; gross margin by SKU requires manual ERP queries
- **Dashboard use pattern:** Logs in daily; uses trend views for weekly team meetings; exports data for monthly board slide preparation
- **Key questions:** Revenue vs. last month by category; which SKUs are margin-negative; which retailers are trending down
- **Technical comfort:** Medium — comfortable with filtered views and date ranges; occasional SQL for ad hoc

#### Persona 3: VP Operations (Primary — Supply Chain and Production Oversight)

- **Goal:** Monitor inventory, production yield, distribution performance in a single view; receive alerts before problems escalate
- **Current pain:** Inventory and production data live in separate ERP modules with no cross-domain linkage; distribution reports come from logistics partner portals with 24-hour lag
- **Dashboard use pattern:** Checks dashboard multiple times per day; uses production view during daily operations stand-up
- **Key questions:** Any batches below yield threshold today; which warehouse is closest to stockout; worst distribution routes this week; fulfillment rate by channel
- **Technical comfort:** Medium — comfortable with operational dashboards; may drill into tables

#### Persona 4: Supply Chain Manager (Primary — Day-to-Day Inventory and Logistics)

- **Goal:** Monitor stock levels hourly; act on reorder alerts before stockouts occur; track DSI and adjust reorder points
- **Current pain:** Tracks reorder points manually in spreadsheets; no consolidated view of stock across all 12 states; DSI calculation is a weekly manual exercise
- **Dashboard use pattern:** Heavy user; checks inventory heat map multiple times per day; primary responder to reorder alerts
- **Key questions:** Which SKUs are below reorder point right now; DSI for each warehouse; inventory turnover vs. last month
- **Technical comfort:** High — uses ERP daily; comfortable with table-level drill-down

---

### 4.2 User Stories

#### Epic CPG-E01: Sales Domain

---

**Story ID:** CPG-001
**Epic:** CPG-E01 — Sales Domain
**Domain:** Infrastructure (foundational)
**Priority:** Must Have

**As a** Data Engineer,
**I want** a fully configured Databricks workspace with Bronze, Silver, and Gold schemas, a running cluster policy, and GitHub repository connected via DABs,
**So that** all pipeline development has a consistent, reproducible environment from day one.

**Acceptance Criteria:**
- [ ] AC-1: Given the project repository, when the DABs deployment command is run, then the three Hive metastore databases (`brz_freshsip`, `slv_freshsip`, `gld_freshsip`) exist and are accessible.
- [ ] AC-2: Given the Databricks workspace, when a cluster is started using the project cluster policy, then it starts successfully within 5 minutes and remains active during pipeline execution.
- [ ] AC-3: Given the GitHub repository, when a push is made to the `main` branch, then the GitHub Actions CI/CD workflow runs and exits with status code 0.
- [ ] AC-4: Given the workspace, when any agent queries `SHOW DATABASES`, then all three layer databases are present.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 8
**Dependencies:** None

---

**Story ID:** CPG-002
**Epic:** CPG-E01 — Sales Domain
**Domain:** All Domains (foundational)
**Priority:** Must Have

**As a** Product Owner,
**I want** 13 months of realistic synthetic data generated for all five domains in their correct source formats (JSON for POS, CSV for ERP and logistics, sensor event records for IoT),
**So that** YoY and MoM trend comparisons are meaningful and the board presentation has credible-looking data without requiring access to real SAP systems.

**Acceptance Criteria:**
- [ ] AC-1: Given the data generation script, when it is executed, then all output files cover a date range from 13 months prior to the current date through today.
- [ ] AC-2: Given the synthetic POS JSON files, when the Bronze ingestion pipeline runs, then row count in `brz_freshsip.pos_transactions_raw` matches the expected synthetic row count within 1%.
- [ ] AC-3: Given the synthetic data, when any Gold KPI is computed for any trailing 12-month period, then the output contains non-null values for every month in that range.
- [ ] AC-4: Given the synthetic data, when revenue trend is plotted on the dashboard, then there are visible and plausible seasonal patterns (not flat or purely random values).
- [ ] AC-5: Given all domain datasets, when joined on shared keys (retailer_id, sku_id, batch_id, shipment_id), then referential integrity holds for >= 95% of records.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-001

---

**Story ID:** CPG-003
**Epic:** CPG-E01 — Sales Domain
**Domain:** Sales
**Priority:** Must Have

**As a** VP Sales,
**I want** POS transaction data ingested from hourly JSON files into the Bronze layer,
**So that** revenue data is available within 1 hour of the retailer reporting a sale.

**Acceptance Criteria:**
- [ ] AC-1: Given a new hourly POS JSON file lands in the source directory, when the Bronze ingestion pipeline runs, then all records from that file appear in `brz_freshsip.pos_transactions_raw` within 65 minutes of file arrival.
- [ ] AC-2: Given the Bronze pipeline, when it runs, then no schema transformations are applied — data is stored exactly as received (schema-on-read, append-only).
- [ ] AC-3: Given a JSON file with malformed records, when the Bronze pipeline processes it, then valid records are ingested and malformed records are logged to the pipeline error table with file name, line number, and error reason.
- [ ] AC-4: Given the Bronze table after ingestion, when the row count is compared to the source file record count, then the counts match exactly (no records lost or duplicated).

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-001, CPG-002

---

**Story ID:** CPG-004
**Epic:** CPG-E01 — Sales Domain
**Domain:** Sales
**Priority:** Must Have

**As a** VP Sales,
**I want** daily ERP sales order and return data ingested from CSV files into the Bronze layer,
**So that** net revenue calculations include ERP invoice prices and return adjustments.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily ERP CSV file, when the Bronze ingestion pipeline runs, then all records appear in `brz_freshsip.erp_sales_raw` within 30 minutes of file availability.
- [ ] AC-2: Given the Bronze pipeline, when it runs, then the raw CSV structure is preserved with no column renames or type conversions.
- [ ] AC-3: Given a CSV with a header row mismatch vs. the expected schema, when the pipeline runs, then it halts ingestion for that file and raises a schema alert, without corrupting previously loaded data.
- [ ] AC-4: Given returns data, when it is ingested, then it is stored in a separate Bronze partition or table (`brz_freshsip.erp_returns_raw`) distinguishable from forward sales.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-001, CPG-002

---

**Story ID:** CPG-005
**Epic:** CPG-E01 — Sales Domain
**Domain:** Sales
**Priority:** Must Have

**As a** VP Sales,
**I want** Bronze sales data cleaned, deduplicated, validated, and conformed into the Silver layer with business rule enforcement,
**So that** all downstream KPI calculations are based on accurate, de-duplicated transaction records with correct data types.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline, when it runs on the Bronze sales tables, then the null rate for all non-nullable columns (`transaction_id`, `sku_id`, `retailer_id`, `unit_price`, `quantity_sold`, `transaction_date`) in `slv_freshsip.sales_transactions` is less than 1%.
- [ ] AC-2: Given duplicate `transaction_id` values across Bronze loads, when the Silver pipeline runs, then `slv_freshsip.sales_transactions` contains exactly one record per `transaction_id`.
- [ ] AC-3: Given records with `unit_price <= 0` or `quantity_sold <= 0`, when the Silver pipeline runs, then those records are rejected to the DQ error log and not written to `slv_freshsip.sales_transactions`.
- [ ] AC-4: Given return records from `brz_freshsip.erp_returns_raw`, when the Silver pipeline runs, then they appear in `slv_freshsip.sales_returns` with correct `return_amount` and a valid reference back to the originating `transaction_id` in `slv_freshsip.sales_transactions`.
- [ ] AC-5: Given the Silver pipeline run, when it completes, then a DQ summary record is written to the pipeline monitoring table including: run timestamp, rows processed, rows rejected, null rates per column.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 8
**Dependencies:** CPG-003, CPG-004

---

**Story ID:** CPG-006
**Epic:** CPG-E01 — Sales Domain
**Domain:** Sales
**Priority:** Must Have

**As a** CEO,
**I want** a Daily Revenue Gold table computed from Silver sales data and refreshed hourly,
**So that** I can view current-day revenue and trailing trends by product category, region, and channel on the executive dashboard.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.sales_daily_revenue` contains one row per `(transaction_date, product_category, region, channel)` combination with a non-null `net_revenue` column.
- [ ] AC-2: Given a specific `(transaction_date, product_category, region, channel)` combination, when the `net_revenue` value in the Gold table is manually verified, then it equals `SUM(unit_price * quantity_sold) - SUM(return_amount)` for that combination in the Silver tables within a tolerance of $0.01.
- [ ] AC-3: Given the Gold table, when data for the most recent completed day is queried, then the `last_updated_ts` column reflects a timestamp within 65 minutes of the current time (hourly SLA).
- [ ] AC-4: Given 13 months of synthetic data, when the Gold table is queried for any calendar month in that range, then a non-null and non-zero `net_revenue` value is returned for at least 3 of the 4 product categories.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-005

---

**Story ID:** CPG-007
**Epic:** CPG-E01 — Sales Domain
**Domain:** Sales
**Priority:** Must Have

**As a** VP Sales,
**I want** a Gross Margin by SKU Gold table computed from Silver sales data joined with product cost reference data,
**So that** I can identify which SKUs are most and least profitable and take pricing or mix decisions accordingly.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.sales_gross_margin_sku` contains one row per `(week_start_date, sku_id, product_category)` with non-null `gross_margin_pct` and `net_revenue` columns.
- [ ] AC-2: Given any SKU, when the `gross_margin_pct` in the Gold table is manually verified, then it equals `(net_revenue - cogs) / net_revenue * 100` where `cogs = standard_cost_per_unit * quantity_sold`, within a tolerance of 0.1 percentage points.
- [ ] AC-3: Given a SKU with `gross_margin_pct < 30%`, when the Gold table is queried, then that SKU row has a `margin_alert_flag = true` column value.
- [ ] AC-4: Given the Gold table, when data for the current week is queried before the week is complete, then the partial-week values are present and labeled with `is_partial_week = true`.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-005

---

#### Epic CPG-E02: Inventory Domain

---

**Story ID:** CPG-008
**Epic:** CPG-E02 — Inventory Domain
**Domain:** Inventory
**Priority:** Must Have

**As a** Supply Chain Manager,
**I want** daily ERP inventory snapshot data ingested into the Bronze layer,
**So that** warehouse stock levels are available for processing in the Silver and Gold layers.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily ERP inventory CSV, when the Bronze pipeline runs, then records appear in `brz_freshsip.erp_inventory_raw` within 30 minutes of file availability.
- [ ] AC-2: Given the Bronze table, when queried after ingestion, then each record contains a non-null `warehouse_id`, `sku_id`, `units_on_hand`, and `snapshot_date`.
- [ ] AC-3: Given a day where no CSV file arrives (holiday or system outage), when the pipeline runs, then it logs a missing-file alert and does not create a zero-row snapshot that would corrupt stock level calculations.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-001, CPG-002

---

**Story ID:** CPG-009
**Epic:** CPG-E02 — Inventory Domain
**Domain:** Inventory
**Priority:** Must Have

**As a** Supply Chain Manager,
**I want** Bronze inventory data cleaned and conformed into the Silver layer, including a reorder point reference table populated per SKU and warehouse,
**So that** reorder alerts can be generated from a single, authoritative stock level source.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline, when it runs, then `slv_freshsip.inventory_stock` contains one row per `(sku_id, warehouse_id, snapshot_date)` with no duplicate combinations.
- [ ] AC-2: Given records with `units_on_hand < 0`, when the Silver pipeline runs, then those records are rejected to the DQ error log with reason code `INVALID_STOCK_LEVEL`.
- [ ] AC-3: Given the reorder point reference table, when `slv_freshsip.ref_reorder_points` is queried, then it contains a non-null `reorder_point_units` value for every `(sku_id, warehouse_id)` combination present in `slv_freshsip.inventory_stock`.
- [ ] AC-4: Given the Silver pipeline run, when it completes, then the pipeline monitoring table is updated with row counts, rejection counts, and run timestamp.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-008

---

**Story ID:** CPG-010
**Epic:** CPG-E02 — Inventory Domain
**Domain:** Inventory
**Priority:** Should Have

**As a** Supply Chain Manager,
**I want** an Inventory Turnover Rate Gold table computed from Silver inventory and sales data over a trailing 30-day window,
**So that** I can assess whether each warehouse is holding too much or too little inventory relative to sales velocity.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.inventory_turnover` contains one row per `(week_start_date, warehouse_id)` with a non-null `inventory_turnover_rate` column.
- [ ] AC-2: Given any warehouse row, when the turnover rate is manually verified, then it equals `SUM(cogs_30d) / AVG(inventory_value_30d)` for that warehouse within a tolerance of 0.01.
- [ ] AC-3: Given a warehouse with `inventory_turnover_rate < 0.5`, when the Gold table is queried, then that row has `turnover_alert_flag = true`.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-005, CPG-009

---

**Story ID:** CPG-011
**Epic:** CPG-E02 — Inventory Domain
**Domain:** Inventory
**Priority:** Must Have

**As a** Supply Chain Manager,
**I want** DSI and Reorder Alert Flag Gold tables refreshed hourly from Silver inventory and sales data,
**So that** I receive immediate visibility into which SKUs are at risk of stockout and exactly how many days of supply remain.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.inventory_dsi` contains one row per `(snapshot_date, sku_id, warehouse_id)` with a non-null `dsi_days` column.
- [ ] AC-2: Given any SKU-warehouse combination, when `dsi_days` is manually verified, then it equals `units_on_hand / avg_daily_sales_units_30d` for that combination within a tolerance of 0.1 days.
- [ ] AC-3: Given `gld_freshsip.inventory_stock_levels`, when any row has `units_on_hand <= reorder_point_units`, then that row has `reorder_alert_flag = true`.
- [ ] AC-4: Given the Gold tables, when the most recent row for any SKU-warehouse combination is queried, then the `last_updated_ts` reflects a timestamp within 65 minutes of the current time.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-005, CPG-009

---

#### Epic CPG-E03: Production Domain

---

**Story ID:** CPG-012
**Epic:** CPG-E03 — Production Domain
**Domain:** Production
**Priority:** Should Have

**As a** VP Operations,
**I want** IoT sensor data from production lines ingested using a micro-batch pattern (5-minute trigger intervals) into the Bronze layer,
**So that** batch yield events and downtime incidents are captured within 5 minutes of occurring on the production floor.

**Acceptance Criteria:**
- [ ] AC-1: Given new IoT sensor event records in the source directory, when the micro-batch pipeline trigger fires, then those records appear in `brz_freshsip.iot_production_raw` within 10 minutes of the trigger time.
- [ ] AC-2: Given the Bronze pipeline, when it runs, then IoT records are appended without transformation (schema-on-read, append-only).
- [ ] AC-3: Given a production line generating events at 1-second intervals, when the micro-batch runs, then no events are dropped between consecutive trigger windows (verified by event sequence number continuity check).
- [ ] AC-4: Given a trigger window with zero new events, when the pipeline runs, then it completes without error and does not write empty records.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 8
**Dependencies:** CPG-001, CPG-002

---

**Story ID:** CPG-013
**Epic:** CPG-E03 — Production Domain
**Domain:** Production
**Priority:** Should Have

**As a** VP Operations,
**I want** IoT Bronze data cleaned and conformed into production batch records and QC results in the Silver layer,
**So that** each batch has a single consolidated record with actual output, expected output, QC status, and production line assignment.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline, when it runs, then `slv_freshsip.production_batches` contains exactly one row per `batch_id` with non-null `actual_output_cases`, `expected_output_cases`, `qc_status`, `production_line_id`, `batch_start_ts`, and `batch_end_ts`.
- [ ] AC-2: Given a `batch_id` with multiple IoT events, when the Silver pipeline runs, then those events are aggregated into a single batch record; no `batch_id` appears more than once.
- [ ] AC-3: Given records where `actual_output_cases > expected_output_cases * 1.10` (greater than 10% over-yield, physically implausible), when the Silver pipeline runs, then those records are flagged with `dq_flag = 'YIELD_EXCEEDS_EXPECTED'` and logged.
- [ ] AC-4: Given batch records with `qc_status NOT IN ('PASS', 'FAIL', 'PENDING')`, when the Silver pipeline runs, then those records are rejected with reason code `INVALID_QC_STATUS`.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-012

---

**Story ID:** CPG-014
**Epic:** CPG-E03 — Production Domain
**Domain:** Production
**Priority:** Should Have

**As a** VP Operations,
**I want** Batch Yield Rate and Quality Check Pass Rate Gold tables computed from Silver production batch records,
**So that** I can monitor production efficiency and quality compliance in near real time on the executive dashboard.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.production_yield` contains one row per `batch_id` with a non-null `batch_yield_rate_pct` column.
- [ ] AC-2: Given any batch row, when `batch_yield_rate_pct` is manually verified, then it equals `actual_output_cases / expected_output_cases * 100` within a tolerance of 0.01 percentage points.
- [ ] AC-3: Given `gld_freshsip.production_quality`, when any row has `quality_pass_rate_pct < 96%` for a `(production_date, production_line_id)` combination, then that row has `quality_alert_flag = true`.
- [ ] AC-4: Given the Gold tables, when queried for the current day, then values reflect batch completions within the past 10 minutes (micro-batch SLA).

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-013

---

**Story ID:** CPG-015
**Epic:** CPG-E03 — Production Domain
**Domain:** Production
**Priority:** Could Have

**As a** VP Operations,
**I want** Downtime Hours and Batch Traceability Gold tables computed from Silver production and shipment data,
**So that** I can quantify unplanned production losses and demonstrate end-to-end traceability from raw material batch to retailer delivery for regulatory and recall readiness.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.production_downtime` contains one row per `(production_date, production_line_id)` with a non-null `downtime_hours` column.
- [ ] AC-2: Given any downtime row, when `downtime_hours` is manually verified, then it equals the sum of `(downtime_end_ts - downtime_start_ts) / 3600.0` for all `DOWNTIME_UNPLANNED` events on that line on that date, within a tolerance of 0.01 hours.
- [ ] AC-3: Given `gld_freshsip.production_traceability`, when any `batch_id` is queried, then the result includes a valid `shipment_id` and `retailer_id` for all batches in `slv_freshsip.production_batches` where `qc_status = 'PASS'` and `batch_end_ts` is more than 48 hours prior to query time.
- [ ] AC-4: Given `gld_freshsip.production_traceability`, when the traceability index is computed, then it equals the percentage of completed, passed batches with full batch → shipment → retailer chain, meeting the KPI-P04 formula exactly.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-013, CPG-017

---

#### Epic CPG-E04: Distribution Domain

---

**Story ID:** CPG-016
**Epic:** CPG-E04 — Distribution Domain
**Domain:** Distribution
**Priority:** Should Have

**As a** VP Operations,
**I want** daily logistics partner shipment CSV data ingested into the Bronze layer,
**So that** on-time delivery, fulfillment, cost, and route performance data is available for Silver and Gold processing.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily logistics CSV, when the Bronze pipeline runs, then records appear in `brz_freshsip.logistics_shipments_raw` within 30 minutes of file availability.
- [ ] AC-2: Given the Bronze table after ingestion, when queried, then all raw columns are present and unmodified (no type casts, renames, or derived columns).
- [ ] AC-3: Given a logistics file containing records for multiple carriers, when the pipeline runs, then all carriers' records are ingested into the same table partitioned by `ingestion_date`.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-001, CPG-002

---

**Story ID:** CPG-017
**Epic:** CPG-E04 — Distribution Domain
**Domain:** Distribution
**Priority:** Should Have

**As a** VP Operations,
**I want** Bronze logistics data cleaned and conformed into shipment and route records in the Silver layer, including the derivation of the `is_fully_shipped` flag per order,
**So that** all downstream distribution KPIs are computed from a single, validated shipment source of truth.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline, when it runs, then `slv_freshsip.shipments` contains one row per `shipment_id` with non-null `actual_delivery_date`, `promised_delivery_date`, `logistics_cost_usd`, `cases_delivered`, `order_id`, `channel`, `region`, and `route_id`.
- [ ] AC-2: Given an order with all line items fully shipped (`quantity_shipped >= quantity_ordered` for all lines), when the Silver pipeline runs, then that `order_id` has `is_fully_shipped = true` in `slv_freshsip.shipments`.
- [ ] AC-3: Given records where `actual_delivery_date` is null for shipments with `ship_date` older than 30 days, when the Silver pipeline runs, then those records are flagged with `dq_flag = 'DELIVERY_DATE_MISSING'` and logged.
- [ ] AC-4: Given records where `logistics_cost_usd < 0`, when the Silver pipeline runs, then those records are rejected with reason code `INVALID_LOGISTICS_COST`.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-016

---

**Story ID:** CPG-018
**Epic:** CPG-E04 — Distribution Domain
**Domain:** Distribution
**Priority:** Should Have

**As a** VP Operations,
**I want** On-Time Delivery % and Order Fulfillment Rate Gold tables computed from Silver shipment data and refreshed daily,
**So that** I can monitor delivery reliability and fulfillment performance by channel and region on the executive dashboard.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.distribution_otd` contains one row per `(ship_date, channel, region)` with a non-null `otd_pct` column.
- [ ] AC-2: Given any OTD row, when `otd_pct` is manually verified, then it equals `COUNT(shipment_id WHERE actual_delivery_date <= promised_delivery_date) / COUNT(shipment_id) * 100` for that combination within a tolerance of 0.01 percentage points.
- [ ] AC-3: Given `gld_freshsip.distribution_fulfillment`, when any row has `fulfillment_rate_pct < 95%`, then that row has `fulfillment_alert_flag = true`.
- [ ] AC-4: Given any fulfillment row, when `fulfillment_rate_pct` is manually verified, then it equals `COUNT(order_id WHERE is_fully_shipped = true) / COUNT(order_id) * 100` for that `(order_date, channel)` combination within a tolerance of 0.01 percentage points.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-017

---

**Story ID:** CPG-019
**Epic:** CPG-E04 — Distribution Domain
**Domain:** Distribution
**Priority:** Could Have

**As a** VP Operations,
**I want** Cost Per Case Delivered and Worst-Performing Routes Gold tables computed from Silver shipment data and refreshed weekly,
**So that** I can identify which routes are most expensive and underperforming on delivery reliability to prioritize corrective negotiation or rerouting.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.distribution_cost` contains one row per `(week_start_date, region, route_id)` with a non-null `cost_per_case_usd` column.
- [ ] AC-2: Given any cost row, when `cost_per_case_usd` is manually verified, then it equals `SUM(logistics_cost_usd) / SUM(cases_delivered)` for that combination within a tolerance of $0.01.
- [ ] AC-3: Given `gld_freshsip.distribution_route_performance`, when queried for the most recent week, then it contains the top 10 worst routes ranked by `worst_route_rank` ascending (rank 1 = worst).
- [ ] AC-4: Given any route row in `gld_freshsip.distribution_route_performance`, when `route_otd_pct` is manually verified, then it equals the OTD% formula applied to all shipments on that route in that week within a tolerance of 0.01 percentage points.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-017

---

#### Epic CPG-E05: Customers Domain

---

**Story ID:** CPG-020
**Epic:** CPG-E05 — Customers Domain
**Domain:** Customers
**Priority:** Should Have

**As a** VP Sales,
**I want** daily ERP customer and retailer profile data ingested into the Bronze layer,
**So that** retailer attributes (name, segment, region, account activation date) are available for Silver enrichment and Customer Gold KPI computation.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily ERP customer CSV, when the Bronze pipeline runs, then records appear in `brz_freshsip.erp_customers_raw` within 30 minutes of file availability.
- [ ] AC-2: Given the Bronze table after ingestion, when queried, then all records include a non-null `retailer_id` and `account_activation_date`.
- [ ] AC-3: Given records with duplicate `retailer_id` values in the same daily file, when the pipeline runs, then all duplicate records are ingested into Bronze (no deduplication at Bronze layer); the Silver layer handles deduplication.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-001, CPG-002

---

**Story ID:** CPG-021
**Epic:** CPG-E05 — Customers Domain
**Domain:** Customers
**Priority:** Should Have

**As a** VP Sales,
**I want** Bronze customer data cleaned, deduplicated, and loaded into a SCD Type 2 Silver table for retailer profiles,
**So that** historical changes to retailer attributes (segment reclassification, region reassignment) are preserved and KPIs can be computed correctly against the attribute values active at the time of each transaction.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline, when it runs on a day where a retailer's `retail_segment` has changed, then `slv_freshsip.customers` has a new row with `effective_start_date` = today, and the prior row has `effective_end_date` = yesterday and `is_current = false`.
- [ ] AC-2: Given `slv_freshsip.customers`, when queried for any `retailer_id`, then exactly one row has `is_current = true`.
- [ ] AC-3: Given the Silver pipeline, when it runs, then the null rate for `retailer_id`, `retailer_name`, `region`, `retail_segment`, and `account_activation_date` in `slv_freshsip.customers` is less than 1%.
- [ ] AC-4: Given a new retailer account appearing for the first time, when the Silver pipeline runs, then a new SCD record is created with `effective_start_date` = `account_activation_date` and `is_current = true`.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 8
**Dependencies:** CPG-020

---

**Story ID:** CPG-022
**Epic:** CPG-E05 — Customers Domain
**Domain:** Customers
**Priority:** Should Have

**As a** CEO,
**I want** a Top 20 Retailers by Revenue Gold table refreshed weekly with rank, net revenue, and percentage-of-total columns,
**So that** I can immediately identify my most important accounts and spot any rank changes week-over-week.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs, then `gld_freshsip.customers_top_retailers` contains exactly 20 rows for the most recent complete week, ranked 1 through 20 by `net_revenue` descending.
- [ ] AC-2: Given any retailer row, when `retailer_net_revenue` is manually verified, then it equals `SUM(unit_price * quantity_sold) - SUM(return_amount)` for that `retailer_id` in the most recent complete week in the Silver tables within a tolerance of $0.01.
- [ ] AC-3: Given the Gold table, when the sum of `pct_of_total_revenue` across all 20 rows is computed, then it is between 0% and 100% (exclusive), never summing to more than 100%.
- [ ] AC-4: Given a retailer appearing in the top 20 for the current week and the prior week, when both weekly rows are queried, then `rank_change` equals `prior_rank - current_rank` (positive = improved, negative = declined).

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-005, CPG-021

---

**Story ID:** CPG-023
**Epic:** CPG-E05 — Customers Domain
**Domain:** Customers
**Priority:** Could Have

**As a** CEO,
**I want** Customer Acquisition Cost, Retailer Retention Rate, and Revenue Concentration Risk Gold tables computed from Silver sales and customer data on a monthly refresh cycle,
**So that** I can track the health and diversity of FreshSip's retailer portfolio and assess whether account acquisition investment is returning appropriate value.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline, when it runs on the first business day of a month, then `gld_freshsip.customers_cac` contains one row per `(month, retail_segment)` covering the prior calendar month with a non-null `cac_usd` column.
- [ ] AC-2: Given any CAC row, when `cac_usd` is manually verified, then it equals `SUM(trade_spend_usd + broker_commission_usd + field_sales_cost_usd) / COUNT(DISTINCT new_account_id)` for that `(month, retail_segment)` within a tolerance of $1.00.
- [ ] AC-3: Given `gld_freshsip.customers_retention`, when any month's retention rate is manually verified, then it equals the percentage of `retailer_id` values active in the prior month that also had at least one transaction in the current month, within a tolerance of 0.1 percentage points.
- [ ] AC-4: Given `gld_freshsip.customers_concentration_risk`, when queried for the most recent month, then `top5_concentration_pct` equals the percentage of total revenue attributable to the top 5 retailers in `gld_freshsip.customers_top_retailers` for that month within a tolerance of 0.1 percentage points.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-005, CPG-021

---

#### Epic CPG-E06: Dashboard

---

**Story ID:** CPG-024
**Epic:** CPG-E06 — Dashboard
**Domain:** Sales, Inventory
**Priority:** Must Have

**As a** CEO,
**I want** Sales and Inventory dashboard pages live in the Databricks AI/BI Dashboard, covering all Must Have KPIs for those domains,
**So that** I can view revenue trends, gross margin by SKU, stock levels, DSI, and reorder alerts from a single dashboard link.

**Acceptance Criteria:**
- [ ] AC-1: Given a pre-warmed Databricks cluster, when the dashboard Sales page is loaded, then all charts render within 5 seconds of page open.
- [ ] AC-2: Given the Sales page, when it is viewed, then it displays: daily revenue trend (line chart), MoM revenue comparison (delta card), YoY revenue comparison (delta card), and gross margin by SKU table with alert flags.
- [ ] AC-3: Given the Inventory page, when it is viewed, then it displays: current stock level heat map (warehouse x SKU), DSI table with color coding, reorder alert badge with count, and inventory turnover bar chart.
- [ ] AC-4: Given any KPI widget on the Sales or Inventory pages, when the underlying data is a known test value, then the widget displays that value correctly (no rounding errors > $1 or 0.1%).

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 8
**Dependencies:** CPG-006, CPG-007, CPG-010, CPG-011

---

**Story ID:** CPG-025
**Epic:** CPG-E06 — Dashboard
**Domain:** Production, Distribution
**Priority:** Should Have

**As a** VP Operations,
**I want** Production and Distribution dashboard pages live in the Databricks AI/BI Dashboard,
**So that** I can monitor batch yield rate, quality pass rate, downtime, on-time delivery, and fulfillment rate in a single operational view.

**Acceptance Criteria:**
- [ ] AC-1: Given a pre-warmed cluster, when the Production dashboard page is loaded, then it renders within 5 seconds and displays: batch yield rate (KPI card + trend line), quality check pass rate (KPI card + heat map by production line), and downtime hours (bar chart by line).
- [ ] AC-2: Given the Distribution page, when it is viewed, then it displays: OTD% (KPI card + regional map), order fulfillment rate (KPI card + channel bar chart), cost per case (bar chart by region), and worst-performing routes (ranked table).
- [ ] AC-3: Given a batch yield rate below 92%, when the Production page is viewed, then the affected batch row is highlighted with a visual alert indicator (red color or alert icon).

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-014, CPG-015, CPG-018, CPG-019

---

**Story ID:** CPG-026
**Epic:** CPG-E06 — Dashboard
**Domain:** Customers
**Priority:** Should Have

**As a** CEO,
**I want** a Customers dashboard page showing top 20 retailers, revenue concentration risk, and customer retention trends,
**So that** I can assess the health of FreshSip's retailer portfolio and answer account concentration questions from the board.

**Acceptance Criteria:**
- [ ] AC-1: Given a pre-warmed cluster, when the Customers dashboard page is loaded, then it renders within 5 seconds and displays the top 20 retailers ranked table with rank-change indicators.
- [ ] AC-2: Given the Customers page, when it is viewed, then it also displays: revenue concentration donut chart (top 5 vs. rest), and monthly retention rate trend line.
- [ ] AC-3: Given a single retailer with > 15% of total revenue, when the Customers page is viewed, then that retailer row is highlighted with a concentration risk indicator.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 3
**Dependencies:** CPG-022, CPG-023

---

#### Epic CPG-E07: Infrastructure and Operations

---

**Story ID:** CPG-027
**Epic:** CPG-E07 — Infrastructure and Operations
**Domain:** All Domains
**Priority:** Must Have

**As a** Data Engineer,
**I want** Databricks Workflows jobs configured for all pipeline schedules (daily batch at 06:00 UTC, hourly POS refresh, 5-minute micro-batch for Production IoT),
**So that** pipelines run automatically on schedule without manual intervention and the dashboard data is always current.

**Acceptance Criteria:**
- [ ] AC-1: Given the Databricks Workflows configuration, when the daily batch job is viewed, then it shows a schedule of `0 6 * * *` (06:00 UTC daily) and includes all five domain Silver and Gold pipelines as tasks in dependency order.
- [ ] AC-2: Given the hourly POS job, when it runs, then it completes within 60 minutes and updates `gld_freshsip.sales_daily_revenue` before the next trigger fires.
- [ ] AC-3: Given the micro-batch Production job, when it runs at 5-minute intervals, then it processes all new IoT records since the last checkpoint and completes before the next trigger.
- [ ] AC-4: Given any job failure, when the Workflows UI is viewed, then the failed run is flagged with an error status and the error message is accessible in the run logs.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-006, CPG-007, CPG-010, CPG-011, CPG-014, CPG-018, CPG-022

---

**Story ID:** CPG-028
**Epic:** CPG-E07 — Infrastructure and Operations
**Domain:** All Domains (Infrastructure)
**Priority:** Should Have

**As a** Data Engineer,
**I want** a GitHub Actions CI/CD pipeline that validates code on pull requests and deploys to Databricks via DABs on merge to `main`,
**So that** all code changes are reviewed, tested, and deployed consistently without manual deployment steps.

**Acceptance Criteria:**
- [ ] AC-1: Given a pull request to the `main` branch, when the PR is opened, then the GitHub Actions workflow runs unit tests and linting checks within 5 minutes and posts a pass/fail status to the PR.
- [ ] AC-2: Given a merge to `main`, when the GitHub Actions deployment workflow runs, then the DABs `bundle deploy` command completes successfully and updates the Databricks jobs and pipeline definitions without manual intervention.
- [ ] AC-3: Given a deployment that fails (e.g., schema conflict, syntax error), when the GitHub Actions workflow runs, then it exits with a non-zero status code and the error details are available in the workflow run log.
- [ ] AC-4: Given the CI/CD workflow, when it runs, then it never exposes Databricks API tokens or other secrets in workflow logs (secrets are accessed only via GitHub Secrets).

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 8
**Dependencies:** CPG-001

---

**Story ID:** CPG-029
**Epic:** CPG-E07 — Infrastructure and Operations
**Domain:** All Domains
**Priority:** Must Have

**As a** Data Engineer,
**I want** a data quality framework that runs null checks, duplicate key detection, and range validation on every Silver and Gold pipeline run, with results logged to a monitoring table,
**So that** any data quality degradation is detectable and traceable without querying individual domain tables.

**Acceptance Criteria:**
- [ ] AC-1: Given any Silver pipeline run, when it completes, then a new row is written to `slv_freshsip.pipeline_dq_log` with: `pipeline_name`, `run_ts`, `table_name`, `rows_processed`, `rows_rejected`, `null_rate_by_column` (as a JSON map), `duplicate_count`, and `run_status`.
- [ ] AC-2: Given any table where the null rate for a required column exceeds 1%, when the DQ check runs, then `run_status` = `'WARN'` or `'FAIL'` (not `'OK'`) in the monitoring log.
- [ ] AC-3: Given any table where duplicate primary keys are detected, when the DQ check runs, then `run_status` = `'FAIL'` and the duplicate keys are listed in a `dq_detail` column.
- [ ] AC-4: Given the monitoring table, when queried across all pipeline runs for the current day, then every Silver and Gold table has at least one log entry showing successful execution.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 5
**Dependencies:** CPG-005, CPG-009, CPG-013, CPG-017, CPG-021

---

**Story ID:** CPG-030
**Epic:** CPG-E07 — Infrastructure and Operations
**Domain:** All Domains (Stretch)
**Priority:** Could Have

**As a** CEO,
**I want** a Databricks Genie AI/BI natural language query space connected to all Gold layer tables,
**So that** I can ask business questions in plain English (e.g., "Which region had the highest revenue last week?") and receive immediate answers without needing to know SQL or navigate to specific dashboard pages.

**Acceptance Criteria:**
- [ ] AC-1: Given the Genie space is configured, when the CEO types "What was total revenue last week?", then Genie returns a correct numeric answer matching the value in `gld_freshsip.sales_daily_revenue` for the prior calendar week within a tolerance of $1.00.
- [ ] AC-2: Given the Genie space, when the CEO asks "Which warehouse has the lowest stock level right now?", then Genie returns the correct `warehouse_id` and `units_on_hand` matching the most recent row in `gld_freshsip.inventory_stock_levels`.
- [ ] AC-3: Given the Genie space, when the CEO asks "Who are my top 5 retailers by revenue this month?", then Genie returns the correct top 5 retailer names matching `gld_freshsip.customers_top_retailers` for the current month-to-date.
- [ ] AC-4: Given the Genie space, when any query is answered, then the response includes the SQL query used (for transparency) and the last-updated timestamp of the underlying Gold table.

**Definition of Done:**
- Unit tests pass
- Code reviewed
- Data quality checks pass
- Dashboard updated (if applicable)

**Story Points:** 8
**Dependencies:** CPG-006, CPG-011, CPG-022, CPG-024, CPG-025, CPG-026

---

## 5. Functional Requirements by Domain

### 5.1 Sales Domain

**MoSCoW: Must Have**

#### Data Sources

| Source Name | System | Format | Frequency | Owner |
|---|---|---|---|---|
| POS Transactions | Point-of-Sale system (simulated) | JSON | Hourly | VP Sales |
| ERP Sales Orders | SAP ERP (simulated CSV export) | CSV | Daily | VP Sales |
| ERP Returns | SAP ERP (simulated CSV export) | CSV | Daily | VP Sales |
| Product Reference | SAP ERP / Master Data | CSV | Daily | VP Sales |

#### Business Rules

1. **Revenue is net of returns.** `net_revenue = SUM(unit_price * quantity_sold) - SUM(return_amount)`. Revenue must never be computed using shelf retail price; the ERP invoice price is the authoritative price source.
2. **Revenue excludes trade spend and promotional discounts.** Trade spend is tracked as a separate cost item in the CAC KPI. Revenue-level discounts applied at the invoice line level are excluded from `unit_price` in source data.
3. **Revenue is denominated in USD only.** No multi-currency conversion is required for this release.
4. **A transaction is valid only if** `transaction_id IS NOT NULL AND unit_price > 0 AND quantity_sold > 0 AND transaction_date IS NOT NULL`.
5. **A return is valid only if** it references a `transaction_id` that exists in `slv_freshsip.sales_transactions`. Orphan returns are flagged and logged, not processed into Silver.
6. **Deduplication key for POS transactions:** `transaction_id`. If the same `transaction_id` appears in multiple Bronze loads, only the first occurrence (by `ingestion_ts`) is retained in Silver.
7. **Product category must be one of:** `['Carbonated Soft Drinks', 'Flavored Water', 'Energy Drinks', 'Juice Blends']`. Records with unrecognized categories are rejected.
8. **Channel must be one of:** `['Retail', 'Wholesale', 'Direct-to-Consumer']`. Records with unrecognized channels are rejected.

#### Transformations

- Bronze → Silver: parse JSON (POS) and CSV (ERP) formats; cast `transaction_date` to `DATE`, `unit_price` and `quantity_sold` to `DECIMAL(12,2)` and `INTEGER` respectively; apply deduplication on `transaction_id`; validate business rules; derive `net_revenue` per transaction line.
- Silver → Gold (Daily Revenue): aggregate `net_revenue` by `(transaction_date, product_category, region, channel)`; compute MoM and YoY period comparisons.
- Silver → Gold (Gross Margin): join `slv_freshsip.sales_transactions` to `slv_freshsip.ref_products` on `sku_id` to retrieve `standard_cost_per_unit`; compute `cogs = standard_cost_per_unit * quantity_sold`; compute `gross_margin_pct`; aggregate weekly by `sku_id`.

---

### 5.2 Inventory Domain

**MoSCoW: Must Have (Stock Levels, DSI, Reorder Alert); Should Have (Inventory Turnover Rate)**

#### Data Sources

| Source Name | System | Format | Frequency | Owner |
|---|---|---|---|---|
| Inventory Snapshots | SAP ERP (simulated CSV export) | CSV | Daily | Supply Chain Manager |
| Reorder Point Reference | Configuration / Master Data | CSV | On-demand update | Supply Chain Manager |
| Product Reference | SAP ERP / Master Data | CSV | Daily | VP Sales |

#### Business Rules

1. **A valid inventory record requires:** `warehouse_id IS NOT NULL AND sku_id IS NOT NULL AND units_on_hand >= 0 AND snapshot_date IS NOT NULL`.
2. **Negative stock is physically invalid.** Records with `units_on_hand < 0` are rejected to the DQ error log.
3. **Reorder alert condition:** `units_on_hand <= reorder_point_units` where `reorder_point_units` is sourced from `slv_freshsip.ref_reorder_points` for the matching `(sku_id, warehouse_id)` combination.
4. **If no reorder point is defined for a given `(sku_id, warehouse_id)`**, the reorder alert flag defaults to `NULL` (unknown) — not `false`. The missing reference is logged as a data quality warning.
5. **DSI denominator guard:** If `avg_daily_sales_units_30d = 0` (no sales in trailing 30 days), DSI is reported as `NULL` and flagged with `dsi_flag = 'NO_RECENT_SALES'`.
6. **Inventory Turnover Rate uses a trailing 30-day COGS** from Silver sales data joined to the product cost reference table, not a fiscal calendar period.
7. **Snapshot deduplication key:** `(sku_id, warehouse_id, snapshot_date)`. If multiple snapshots exist for the same combination on the same date, the one with the latest `ingestion_ts` is retained.

#### Transformations

- Bronze → Silver: parse CSV; cast `snapshot_date` to `DATE`, `units_on_hand` to `INTEGER`; apply deduplication; validate business rules; join reorder point reference.
- Silver → Gold (Stock Levels + Reorder Alert): join `slv_freshsip.inventory_stock` to `slv_freshsip.ref_reorder_points`; compute `reorder_alert_flag`; write hourly snapshot to Gold.
- Silver → Gold (DSI): join `slv_freshsip.inventory_stock` to 30-day aggregated sales from `slv_freshsip.sales_transactions`; compute `avg_daily_sales_units_30d`; compute `dsi_days`.
- Silver → Gold (Inventory Turnover): compute trailing 30-day COGS from Silver sales joined to product cost; compute average inventory value; compute turnover rate by warehouse.

---

### 5.3 Production Domain

**MoSCoW: Should Have (Yield Rate, Quality Pass Rate, Downtime); Could Have (Batch Traceability)**

#### Data Sources

| Source Name | System | Format | Frequency | Owner |
|---|---|---|---|---|
| IoT Sensor Events | Production IoT system (simulated) | JSON events (micro-batch) | Every 5 minutes | VP Operations |
| Batch Recipe Reference | SAP ERP / Master Data | CSV | Daily | VP Operations |

#### Business Rules

1. **A valid batch record requires:** `batch_id IS NOT NULL AND production_line_id IS NOT NULL AND actual_output_cases >= 0 AND expected_output_cases > 0 AND batch_start_ts IS NOT NULL AND batch_end_ts IS NOT NULL`.
2. **Yield sanity check:** `actual_output_cases > expected_output_cases * 1.10` is physically implausible (greater than 10% over-yield). Such records are flagged `dq_flag = 'YIELD_EXCEEDS_EXPECTED'` and excluded from yield KPI aggregation until manually confirmed.
3. **QC status must be one of:** `['PASS', 'FAIL', 'PENDING']`. Records with any other value are rejected.
4. **Downtime events** are identified by `event_type = 'DOWNTIME_UNPLANNED'`. Planned maintenance events (`event_type = 'DOWNTIME_PLANNED'`) are stored in Silver but excluded from the Downtime Hours KPI.
5. **Batch completion event** is identified by `event_type = 'BATCH_COMPLETE'`. All IoT events belonging to a `batch_id` are aggregated into one Silver batch record upon receiving this event.
6. **Batch traceability chain:** A batch is considered fully traceable if: `batch_id` in `slv_freshsip.production_batches` has at least one linked `shipment_id` in `slv_freshsip.shipments`, and that shipment has a valid `retailer_id` in `slv_freshsip.customers`.

#### Transformations

- Bronze → Silver: aggregate micro-batch IoT events by `batch_id`; compute `actual_output_cases`, `expected_output_cases`, `batch_start_ts`, `batch_end_ts` from event sequence; apply QC status and downtime event derivation; validate business rules.
- Silver → Gold (Batch Yield Rate): compute `actual_output_cases / expected_output_cases * 100` per batch; aggregate daily by `production_line_id` and `product_category`.
- Silver → Gold (Quality Check Pass Rate): count `qc_status = 'PASS'` vs. total completed batches; aggregate daily by `production_line_id` and `product_category`.
- Silver → Gold (Downtime Hours): sum `(downtime_end_ts - downtime_start_ts) / 3600.0` for `DOWNTIME_UNPLANNED` events; aggregate daily by `production_line_id`.
- Silver → Gold (Batch Traceability): three-way join across `slv_freshsip.production_batches`, `slv_freshsip.shipments`, `slv_freshsip.customers`; compute traceability index.

---

### 5.4 Distribution Domain

**MoSCoW: Should Have (OTD%, Fulfillment Rate, Cost Per Case); Could Have (Worst Routes)**

#### Data Sources

| Source Name | System | Format | Frequency | Owner |
|---|---|---|---|---|
| Shipment Records | Logistics partner (simulated CSV export) | CSV | Daily | VP Operations |
| ERP Order Lines | SAP ERP (simulated CSV export) | CSV | Daily | VP Operations |

#### Business Rules

1. **On-time definition:** A shipment is on time if `actual_delivery_date <= promised_delivery_date`. Receipt at destination (retailer DC or warehouse) is the measurement point, not ship date from FreshSip warehouse.
2. **`promised_delivery_date`** is sourced from the ERP order record, not the logistics partner confirmation. If there is a discrepancy, ERP is authoritative.
3. **`is_fully_shipped`** = `true` when all order lines for a given `order_id` have `quantity_shipped >= quantity_ordered`. Partial shipments result in `is_fully_shipped = false`.
4. **`logistics_cost_usd`** must be >= 0. Negative logistics costs indicate a credit or data error and are rejected.
5. **Route ID must be non-null.** Shipments with null `route_id` are flagged for investigation but are still included in OTD% and Fulfillment Rate calculations using a sentinel value `route_id = 'UNKNOWN'`.
6. **Worst-performing routes ranking:** Routes are ranked by `route_otd_pct` ascending (lowest OTD% = worst). Ties are broken by `route_cost_per_case` descending (highest cost = worse). Only routes with >= 10 shipments in the week are included in the ranking.

#### Transformations

- Bronze → Silver: parse CSV; cast `actual_delivery_date` and `promised_delivery_date` to `DATE`; derive `is_fully_shipped`; validate business rules; join ERP order lines to logistics records on `order_id` and `shipment_id`.
- Silver → Gold (OTD%): compute on-time flag per shipment; aggregate by `(ship_date, channel, region)`.
- Silver → Gold (Fulfillment Rate): count `is_fully_shipped = true` by `(order_date, channel)`.
- Silver → Gold (Cost Per Case): compute `logistics_cost_usd / cases_delivered` per shipment; aggregate by `(week_start_date, region, route_id)`.
- Silver → Gold (Worst Routes): rank routes by OTD% ascending (trailing 7-day window); retain top 10 worst.

---

### 5.5 Customers Domain

**MoSCoW: Should Have (Top 20 Retailers, Revenue Concentration Risk); Could Have (CAC, Retention Rate)**

#### Data Sources

| Source Name | System | Format | Frequency | Owner |
|---|---|---|---|---|
| Retailer Profiles | SAP ERP / CRM (simulated CSV export) | CSV | Daily | VP Sales |
| Sales Spend Allocations | SAP ERP (simulated CSV export) | CSV | Monthly | VP Sales |

#### Business Rules

1. **Customer in this context means a retailer account (B2B).** End consumers are not tracked in this release.
2. **SCD Type 2 applies to:** `retail_segment`, `region`, `account_status`, and `credit_tier`. If any of these attributes change, a new SCD row is created and the prior row is closed.
3. **A retailer is considered "active" in a given month** if they have at least one transaction in `slv_freshsip.sales_transactions` in that calendar month.
4. **A new account** is defined by `account_activation_date` falling within the measurement period. Reactivated dormant accounts (previously active, then inactive for > 90 days, then active again) are not counted as new accounts for CAC purposes.
5. **CAC components:** `trade_spend_usd + broker_commission_usd + field_sales_cost_usd`. Marketing spend (brand advertising) is explicitly excluded from CAC for this release.
6. **Revenue Concentration Risk denominator** = total net revenue across all retailers in the measurement period. The numerator is the sum of net revenue for the top 5 retailers by revenue rank.
7. **Top 20 retailers list** is computed on a weekly calendar-week basis. Current week is updated daily as an MTD partial-week view.

#### Transformations

- Bronze → Silver: parse CSV; apply SCD Type 2 logic on `retail_segment`, `region`, `account_status`, `credit_tier`; validate referential integrity of `retailer_id` across sales transactions.
- Silver → Gold (Top 20 Retailers): join `slv_freshsip.sales_transactions` to `slv_freshsip.customers` on `retailer_id`; compute `retailer_net_revenue`; rank descending; retain top 20; compute `pct_of_total_revenue` and `rank_change` vs. prior week.
- Silver → Gold (CAC): join `slv_freshsip.sales_spend` to `slv_freshsip.customers` filtered to new accounts; aggregate spend and new account count by month and segment.
- Silver → Gold (Retention Rate): compute active retailer sets for current and prior month; compute intersection ratio by region.
- Silver → Gold (Revenue Concentration Risk): reference top-5 revenue from `gld_freshsip.customers_top_retailers`; compute as percentage of total revenue.

---

## 6. KPI Definitions

All KPIs are fully defined in `_bmad-output/requirements/kpi-definitions.md`. The table below lists all 20 KPIs by domain with their Gold layer table reference.

| KPI ID | KPI Name | Domain | Gold Table | MoSCoW |
|---|---|---|---|---|
| KPI-S01 | Daily Revenue | Sales | `gld_freshsip.sales_daily_revenue` | Must Have |
| KPI-S02 | Revenue vs. Prior Month (MoM %) | Sales | `gld_freshsip.sales_period_comparison` | Must Have |
| KPI-S03 | Revenue vs. Prior Year (YoY %) | Sales | `gld_freshsip.sales_period_comparison` | Must Have |
| KPI-S04 | Gross Margin by SKU | Sales | `gld_freshsip.sales_gross_margin_sku` | Must Have |
| KPI-I01 | Current Stock Level | Inventory | `gld_freshsip.inventory_stock_levels` | Must Have |
| KPI-I02 | Inventory Turnover Rate | Inventory | `gld_freshsip.inventory_turnover` | Should Have |
| KPI-I03 | Days Sales of Inventory (DSI) | Inventory | `gld_freshsip.inventory_dsi` | Must Have |
| KPI-I04 | Reorder Alert Flag | Inventory | `gld_freshsip.inventory_stock_levels` | Must Have |
| KPI-P01 | Batch Yield Rate | Production | `gld_freshsip.production_yield` | Should Have |
| KPI-P02 | Quality Check Pass Rate | Production | `gld_freshsip.production_quality` | Should Have |
| KPI-P03 | Downtime Hours | Production | `gld_freshsip.production_downtime` | Should Have |
| KPI-P04 | Batch Traceability Index | Production | `gld_freshsip.production_traceability` | Could Have |
| KPI-D01 | On-Time Delivery % | Distribution | `gld_freshsip.distribution_otd` | Should Have |
| KPI-D02 | Order Fulfillment Rate | Distribution | `gld_freshsip.distribution_fulfillment` | Should Have |
| KPI-D03 | Cost Per Case Delivered | Distribution | `gld_freshsip.distribution_cost` | Should Have |
| KPI-D04 | Worst-Performing Routes Score | Distribution | `gld_freshsip.distribution_route_performance` | Could Have |
| KPI-C01 | Top 20 Retailers by Revenue | Customers | `gld_freshsip.customers_top_retailers` | Should Have |
| KPI-C02 | Customer Acquisition Cost (CAC) | Customers | `gld_freshsip.customers_cac` | Could Have |
| KPI-C03 | Retailer Retention Rate | Customers | `gld_freshsip.customers_retention` | Could Have |
| KPI-C04 | Revenue Concentration Risk | Customers | `gld_freshsip.customers_concentration_risk` | Should Have |

---

## 7. Non-Functional Requirements

### 7.1 Performance

- **Dashboard load time:** All dashboard pages must render fully within 5 seconds on a pre-warmed Databricks cluster (Community Edition single-node cluster, pre-started 30 minutes before demo).
- **Daily batch pipeline end-to-end:** The full Bronze → Silver → Gold pipeline for all five domains must complete within 30 minutes of trigger, measured from the start of the Bronze ingestion step to the last Gold table commit.
- **Hourly POS pipeline:** The Sales POS Bronze → Gold incremental pipeline must complete within 60 minutes of each trigger to meet the 1-hour freshness SLA.
- **Micro-batch Production pipeline:** Each 5-minute trigger window for IoT data must process all queued events and commit to Bronze within 10 minutes of the trigger start.
- **Gold table query time:** Any individual Gold table query used by a dashboard widget must return results within 3 seconds when the Gold table is pre-computed (no on-demand aggregation in the dashboard query layer).

### 7.2 Availability

- **Free tier constraint:** Databricks Community Edition has no uptime SLA. The cluster auto-terminates after idle periods and must be manually started or pre-warmed before dashboard use.
- **Demo pre-warm requirement:** The cluster must be started at least 30 minutes before any board presentation or CEO demo session.
- **No streaming SLA:** Micro-batch Production ingestion is delivered on a best-effort basis on the free tier. True sub-minute latency is explicitly not guaranteed or promised.
- **Backup for board demo:** A static screenshot export of all dashboard pages must be maintained as a fallback in the event the cluster is unavailable during the board meeting.

### 7.3 Data Quality

- **Null rate:** The null rate for all non-nullable columns in Silver tables must be less than 1% per pipeline run, as validated by the DQ framework (CPG-029).
- **Duplicate key rate:** Zero duplicate primary keys are permitted in any Silver or Gold table. Duplicates trigger `run_status = 'FAIL'` in the DQ monitoring log.
- **Row rejection logging:** All rows rejected during Silver processing must be logged to the DQ error table with: source table, rejection reason code, rejection timestamp, and the original row values.
- **Referential integrity:** `sku_id` in sales transactions must exist in `slv_freshsip.ref_products`. `retailer_id` in sales transactions must exist in `slv_freshsip.customers`. Violations are flagged with a DQ warning (not necessarily rejected, to avoid data loss from reference lag).
- **KPI formula accuracy:** KPI values in Gold tables must match the formula definitions in `kpi-definitions.md` within a numerical tolerance of $0.01 (currency) or 0.01 percentage points. This is verified by the acceptance criteria spot-check queries.

### 7.4 Data Freshness SLAs

| Domain | Target Refresh | Acceptable Maximum | Trigger |
|---|---|---|---|
| Sales (POS) | 1 hour | 2 hours | Hourly job trigger |
| Sales (ERP) | 6 hours | 12 hours | Daily job at 06:00 UTC |
| Inventory | 1 hour | 4 hours | Hourly job trigger |
| Production | 5 minutes (micro-batch) | 15 minutes | 5-minute Structured Streaming trigger |
| Distribution | 4 hours | 12 hours | Daily job at 04:00 UTC |
| Customers | 24 hours | 48 hours | Daily job at 06:00 UTC |

---

## 8. Technical Constraints

| Constraint | Impact on Requirements | Mitigation |
|---|---|---|
| **Databricks Community Edition (free tier)** | No Unity Catalog; no SLAs; single cluster; auto-terminates after idle | Use Hive metastore with `brz_freshsip`, `slv_freshsip`, `gld_freshsip` databases; document Unity Catalog upgrade path; pre-warm cluster before demos |
| **No true Spark Structured Streaming SLA on free tier** | Production IoT micro-batch latency is best-effort, not guaranteed | Document as "near-real-time (5-min refresh)"; never promise sub-minute latency; micro-batch trigger via Structured Streaming `trigger(processingTime='5 minutes')` |
| **No Unity Catalog** | No fine-grained column-level access control; no automated lineage tracking | All governance controls are manual; RBAC deferred to Phase 2; lineage documented manually in architecture artifacts |
| **Single Databricks workspace user (CEO + AI agents)** | No multi-user access management required for MVP | Shared workspace; all agents operate under the same service principal; role-based filtering deferred to Phase 2 |
| **No real SAP/ERP data** | Historical backfill and YoY comparisons require synthetic data | Generate 13 months of synthetic data with realistic seasonality; label all data as "simulated" until real backfill |
| **DBFS storage limits (free tier)** | Large datasets risk storage exhaustion | Compact Delta tables weekly; purge Bronze raw data after 90 days; limit synthetic dataset to realistic CPG scale (not big data scale) |
| **No external alerting integrations** | Reorder alerts and yield alerts are dashboard-only | All alerts surfaced in dashboard widgets only; email/Slack alert integration deferred to Phase 2 |
| **4-week total timeline** | Insufficient for all 20 KPIs at full depth in one sprint | Phased delivery: Sales + Inventory as Week 1-2 MVP; remaining domains in Weeks 3-4 |

---

## 9. Acceptance Criteria

All acceptance criteria are binary (pass/fail). Criteria from individual user story AC blocks are aggregated here for platform-level verification at go-live.

**AC-001:** Given the Databricks workspace, when the three layer databases are queried, then `brz_freshsip`, `slv_freshsip`, and `gld_freshsip` all exist and are accessible.

**AC-002:** Given 13 months of synthetic data is loaded, when any Gold KPI table is queried for any month in the range, then non-null values are returned for all domain KPIs for that period.

**AC-003:** Given the daily batch pipeline, when it is triggered, then all Bronze → Silver → Gold processing for all five domains completes within 30 minutes.

**AC-004:** Given the hourly Sales POS pipeline, when it runs, then `gld_freshsip.sales_daily_revenue` is updated within 65 minutes of the trigger.

**AC-005:** Given the micro-batch Production pipeline, when a 5-minute trigger fires, then new IoT events appear in `brz_freshsip.iot_production_raw` within 10 minutes of the trigger start.

**AC-006:** Given any Silver table, when the null rate for required columns is computed from the DQ monitoring log, then the null rate is less than 1% for every required column.

**AC-007:** Given any Silver or Gold table, when duplicate primary keys are checked, then zero duplicate primary key records exist.

**AC-008:** Given `gld_freshsip.sales_daily_revenue` for any `(transaction_date, product_category, region, channel)`, when the `net_revenue` value is spot-checked against the Silver source tables, then the values match within $0.01.

**AC-009:** Given `gld_freshsip.inventory_stock_levels`, when any row with `units_on_hand <= reorder_point_units` is checked, then `reorder_alert_flag = true`.

**AC-010:** Given `gld_freshsip.production_yield`, when any batch yield rate is spot-checked against `slv_freshsip.production_batches`, then the value equals `actual_output_cases / expected_output_cases * 100` within 0.01 percentage points.

**AC-011:** Given `gld_freshsip.distribution_otd`, when OTD% for any `(ship_date, channel, region)` is spot-checked, then the value equals `COUNT(WHERE actual_delivery_date <= promised_delivery_date) / COUNT(shipment_id) * 100` within 0.01 percentage points.

**AC-012:** Given `gld_freshsip.customers_top_retailers` for any week, when queried, then exactly 20 rows are returned ranked 1 through 20.

**AC-013:** Given a pre-warmed Databricks cluster, when the Sales dashboard page is loaded, then it fully renders within 5 seconds.

**AC-014:** Given a pre-warmed Databricks cluster, when the Inventory dashboard page is loaded, then it fully renders within 5 seconds and the reorder alert count badge displays the correct number of active alerts.

**AC-015:** Given a pre-warmed cluster, when the Production dashboard page is loaded, then it fully renders within 5 seconds and the batch yield rate KPI card displays the most recent batch's yield rate.

**AC-016:** Given a pre-warmed cluster, when the Distribution dashboard page is loaded, then it fully renders within 5 seconds and OTD% matches the value in `gld_freshsip.distribution_otd` for the most recent 7 days.

**AC-017:** Given a pre-warmed cluster, when the Customers dashboard page is loaded, then it fully renders within 5 seconds and the top 20 retailers table displays ranks 1 through 20.

**AC-018:** Given the CEO's five business questions (from product-brief.md Section 9), when each is answered using the dashboard, then each answer can be located within 2 minutes of navigating to the dashboard.

**AC-019:** Given the pipeline DQ monitoring table, when queried for the current day, then every Silver and Gold table in all five domains has at least one log entry with `run_status` in (`'OK'`, `'WARN'`) — no `'FAIL'` status on any production pipeline run.

**AC-020:** Given the GitHub Actions CI/CD workflow, when a pull request is opened against `main`, then unit tests and linting run automatically and their status is posted to the PR within 5 minutes.

**AC-021:** Given the Databricks Workflows job for daily batch, when viewed in the Workflows UI, then it shows a `0 6 * * *` schedule and all five domain pipeline tasks.

**AC-022:** Given the SCD Type 2 Silver table for customers, when a retailer's `retail_segment` attribute changes, then exactly one row for that `retailer_id` has `is_current = true` and the prior row has a non-null `effective_end_date`.

**AC-023:** Given `gld_freshsip.sales_gross_margin_sku`, when any row with `gross_margin_pct < 30%` is queried, then `margin_alert_flag = true` for that row.

**AC-024:** Given the DQ framework, when any pipeline run detects a null rate > 1% for a required column, then the monitoring log entry has `run_status != 'OK'`.

**AC-025:** Given the GitHub Actions deployment workflow, when it runs after a merge to `main`, then the Databricks job definitions are updated without requiring manual intervention.

---

## 10. Dependencies

### Upstream Data Dependencies

- **Synthetic data generator (CPG-002)** must be completed before any Bronze ingestion pipeline can be developed or tested.
- **Databricks workspace and schema setup (CPG-001)** must be completed before any pipeline code can be deployed or executed.
- **Bronze pipelines** for each domain must be complete before the corresponding Silver pipeline can be developed.
- **Silver pipelines** for each domain must be complete before the corresponding Gold pipeline can be developed.
- **All Gold tables for a domain** must be populated before the corresponding dashboard page can be built (CPG-024 through CPG-026).

### Cross-Domain Dependencies

- `gld_freshsip.customers_concentration_risk` (KPI-C04) depends on `gld_freshsip.customers_top_retailers` (KPI-C01).
- `gld_freshsip.inventory_dsi` (KPI-I03) depends on sales velocity data from `slv_freshsip.sales_transactions` (Sales Silver).
- `gld_freshsip.inventory_turnover` (KPI-I02) depends on COGS data derived from `slv_freshsip.sales_transactions` and `slv_freshsip.ref_products` (Sales Silver).
- `gld_freshsip.production_traceability` (KPI-P04) depends on `slv_freshsip.shipments` (Distribution Silver) and `slv_freshsip.customers` (Customers Silver).

### External System Dependencies

- **Databricks Community Edition account:** Must be provisioned and accessible before CPG-001 can begin.
- **GitHub repository:** Must be initialized with agreed branch strategy before CPG-028 CI/CD can be configured.
- **Logistics partner CSV format agreement:** The column schema of the logistics partner CSV must be documented before CPG-016 Bronze ingestion can be designed.

### Team Dependencies

- The **Data Architect** must produce Silver and Gold schema definitions before the Data Engineer begins pipeline development.
- The **Product Owner** must sign off on all KPI formulas in `kpi-definitions.md` before any Gold pipeline is built.
- The **CEO** must review synthetic data output (CPG-002) before the board demo prep begins to confirm the data looks credible.

---

## 11. Assumptions

1. **Free tier availability:** Databricks Community Edition will remain available and functional throughout the 4-week project timeline. If the free tier is deprecated or rate-limited, the project will require a paid tier as a fallback.
2. **Unity Catalog unavailability:** Unity Catalog is assumed to be unavailable on the free tier for this release. The Hive metastore fallback (`brz_freshsip`, `slv_freshsip`, `gld_freshsip` databases) is the primary catalog strategy.
3. **Synthetic data sufficiency:** 13 months of synthetic data will be sufficient to demonstrate all KPI trends (YoY, MoM, seasonal patterns) for the board presentation without real SAP data.
4. **Single user workspace:** The Databricks workspace is accessed by the CEO and AI development agents only. No multi-user RBAC is required for this release.
5. **ERP invoice price = revenue price:** The `unit_price` field in source ERP and POS data represents the ERP invoice price and is used directly for revenue calculation without further adjustment.
6. **Standard cost from product reference:** `standard_cost_per_unit` in `slv_freshsip.ref_products` is the authoritative COGS basis for gross margin and inventory value calculations.
7. **Reorder points are pre-configured in synthetic data:** Initial `slv_freshsip.ref_reorder_points` values will be seeded from synthetic data using industry-standard CPG benchmarks. Real SAP reorder points will replace these when live data integration occurs.
8. **CSV schemas are stable:** The column schemas of synthetic CSV files (ERP, logistics) will not change during the development period. Schema evolution handling is deferred to the real data integration phase.
9. **Micro-batch as "near-real-time":** The board presentation will represent the Production domain as "near-real-time (5-minute refresh)" and not claim continuous streaming capability.
10. **Open questions from product-brief.md are resolved:** The following product-brief open questions are treated as resolved by the KPI definitions locked in this PRD: Q1 (revenue = net of returns, invoice price, excludes trade spend), Q2 (on-time = receipt date vs. ERP promised date), Q3 (CAC customer = retailer account), Q5 (reorder points seeded from synthetic benchmarks), Q6 (13 months synthetic history).

---

## 12. Risks and Mitigations

| Risk ID | Risk Description | Probability | Impact | Severity | Mitigation |
|---|---|---|---|---|---|
| R1 | SAP CSV schema inconsistency slows Silver layer development | High | High | Critical | Build schema validation and row rejection in Bronze; use mock clean data for dev; buffer time in sprint planning |
| R2 | CEO scope creep — new dashboard requirements added mid-sprint | High | High | Critical | Strict MoSCoW enforcement; any new request goes to backlog after sprint planning is locked |
| R3 | Free tier cluster unavailable during board demo | Medium | Critical | Critical | Maintain pre-loaded demo environment; warm cluster 30 min before demo; maintain static screenshot backup slides |
| R4 | 4-week timeline insufficient to deliver all 5 domains at full depth | High | Medium | High | Phased delivery — Sales + Inventory MVP must be demo-ready by end of Week 2; remaining domains are bonus |
| R5 | IoT streaming unstable on free tier | High | Medium | High | Use micro-batch trigger pattern; never promise sub-minute latency; document as "near-real-time (5-min)" |
| R6 | Synthetic data looks unrealistic — undermines board credibility | Medium | High | High | Generate realistic data with embedded narratives and seasonal patterns; CEO reviews data before board meeting |
| R7 | KPI definitions disputed post-build | Medium | High | High | KPI formulas are locked in `kpi-definitions.md` before any Gold pipeline is built; change requires Product Owner sign-off |
| R8 | Unity Catalog unavailable — complicates catalog strategy | Medium | Low | Medium | Hive metastore fallback is documented and ready; transparent to end users |
| R9 | PII or data governance concerns with retailer data | Low | Medium | Low | Synthetic data only; if real data is used, restrict workspace access to named users only |
| R10 | Historical backfill gap — no YoY data at launch | High | Low | Medium | Seed 13 months of synthetic history; clearly label as "simulated" until real data backfill |

---

## 13. Phased Delivery Plan

### Sprint Overview

| Sprint | Week | Focus | MVP Gate |
|---|---|---|---|
| Sprint 1 | Week 1 | Foundation + Sales domain end-to-end | Silver table `slv_freshsip.sales_transactions` populated; row count and null checks pass |
| Sprint 2 | Week 2 | Sales Gold + Inventory domain + Dashboard MVP | CEO can view daily revenue chart and inventory reorder alerts on live dashboard |
| Sprint 3 | Week 3 | Production, Distribution, Customers domains | All 5 domain Gold tables populated with synthetic data |
| Sprint 4 | Week 4 | Dashboard polish + Genie (stretch) + Board prep | CEO completes full demo walkthrough; screenshot backup slides created |

---

### Sprint 1 — Foundation and Sales (Week 1)

**Goal:** Establish the platform infrastructure and deliver end-to-end Sales pipeline from synthetic data generation through Silver layer.

**Stories in Sprint:**
- CPG-001: Infrastructure setup (8pts) — Must Have
- CPG-002: Synthetic data generation (5pts) — Must Have
- CPG-003: Bronze ingestion — Sales POS JSON (5pts) — Must Have
- CPG-004: Bronze ingestion — Sales ERP CSV (3pts) — Must Have
- CPG-005: Silver layer — Sales cleaning and validation (8pts) — Must Have
- CPG-028: CI/CD pipeline setup (8pts) — Should Have (partial: GitHub Actions config only)
- CPG-029: Data quality framework (5pts) — Must Have (core DQ checks for Sales Silver)

**Sprint 1 Total:** 42 story points
**Sprint 1 Exit Criteria:** `slv_freshsip.sales_transactions` is populated with 13 months of synthetic data; null rate < 1% for all required columns; no duplicate `transaction_id` values; DQ monitoring log has at least one successful run entry.

---

### Sprint 2 — Sales Gold, Inventory, and Dashboard MVP (Week 2)

**Goal:** Deliver all Sales Gold KPIs, complete the Inventory domain Bronze through Gold, and launch the first two dashboard pages. This sprint produces the board demo MVP.

**Stories in Sprint:**
- CPG-006: Gold — Daily Revenue KPI table (5pts) — Must Have
- CPG-007: Gold — Gross Margin by SKU (5pts) — Must Have
- CPG-008: Bronze ingestion — Inventory ERP (3pts) — Must Have
- CPG-009: Silver — Inventory stock levels and reorder reference (5pts) — Must Have
- CPG-010: Gold — Inventory Turnover Rate (5pts) — Should Have
- CPG-011: Gold — DSI and Reorder Alert Flag (5pts) — Must Have
- CPG-024: Dashboard — Sales and Inventory pages (8pts) — Must Have
- CPG-027: Databricks Workflows orchestration (5pts) — Must Have

**Sprint 2 Total:** 41 story points
**Sprint 2 Exit Criteria:** CEO can open the dashboard Sales page and see daily revenue chart loading in < 5 seconds; Inventory page shows reorder alert badge with correct count; all Sales and Inventory Gold tables populated; Workflows daily job scheduled.

---

### Sprint 3 — Production, Distribution, and Customers Domains (Week 3)

**Goal:** Deliver all three remaining domains (Production, Distribution, Customers) from Bronze through Gold, populating all 20 KPI Gold tables with synthetic data.

**Stories in Sprint:**
- CPG-012: Bronze ingestion — IoT sensors micro-batch (8pts) — Should Have
- CPG-013: Silver — Production batch records and QC (5pts) — Should Have
- CPG-014: Gold — Batch Yield Rate and Quality Check Pass Rate (5pts) — Should Have
- CPG-015: Gold — Downtime Hours and Batch Traceability (3pts) — Could Have
- CPG-016: Bronze ingestion — Logistics partner CSV (3pts) — Should Have
- CPG-017: Silver — Shipments and routes (5pts) — Should Have
- CPG-018: Gold — OTD% and Order Fulfillment Rate (5pts) — Should Have
- CPG-019: Gold — Cost Per Case and Worst Routes (3pts) — Could Have
- CPG-020: Bronze ingestion — Customer/Retailer ERP (3pts) — Should Have
- CPG-021: Silver — Retailer profiles SCD Type 2 (8pts) — Should Have
- CPG-022: Gold — Top 20 Retailers by Revenue (3pts) — Should Have
- CPG-023: Gold — CAC, Retention Rate, Revenue Concentration Risk (5pts) — Could Have

**Sprint 3 Total:** 56 story points
**Sprint 3 Exit Criteria:** All 5 domain Gold tables populated with synthetic data; all 20 KPIs return non-null values when queried; micro-batch Production pipeline running on 5-minute trigger.

---

### Sprint 4 — Dashboard Polish, Board Prep, and Stretch (Week 4)

**Goal:** Complete dashboard for all five domains, polish for board presentation, create screenshot backup slides, and deliver Genie space if capacity allows.

**Stories in Sprint:**
- CPG-025: Dashboard — Production and Distribution pages (5pts) — Should Have
- CPG-026: Dashboard — Customers page (3pts) — Should Have
- CPG-028: CI/CD pipeline — complete DABs deployment (8pts) — Should Have (complete remaining work)
- CPG-030: Genie AI/BI natural language space (8pts) — Could Have (stretch)
- Board presentation prep: screenshot export, demo walkthrough, backup slides — not a story; operational task

**Sprint 4 Total:** 24 story points (+ board prep)
**Sprint 4 Exit Criteria:** All 5 dashboard domain pages load within 5 seconds on pre-warmed cluster; CEO completes full demo walkthrough answering all 5 business questions in < 2 minutes each; static screenshot backup slides are ready; Genie space answers at least 3 CEO queries correctly (if completed).

---

## 14. Open Questions

| # | Question | Blocks | Status |
|---|---|---|---|
| OQ-01 | Who else needs dashboard access besides the CEO for the board meeting — board members, other VPs? | Access design; potential need for published public link | Open — requires CEO answer |
| OQ-02 | What alert delivery channel is preferred beyond the dashboard — email, Slack, or dashboard-only? | Alert framework design for Phase 2 | Open — deferred to Phase 2 |
| OQ-03 | Is there an existing Databricks workspace already provisioned, or does a new Community Edition account need to be created? | CPG-001 infrastructure setup start date | Open — requires CEO confirmation |
| OQ-04 | What is the board presentation date (specific calendar date)? This sets the final hard deadline for Sprint 4. | Sprint 4 end date and demo prep timeline | Open — requires CEO answer |
| OQ-05 | For Revenue Concentration Risk (KPI-C04), should the denominator be gross revenue or net revenue (after returns)? | KPI-C04 formula precision | Open — recommend net revenue consistent with all other revenue KPIs; awaiting CEO confirmation |
| OQ-06 | Are reactivated dormant accounts (inactive > 90 days, then active again) counted as new accounts for CAC? The current assumption is no. | CAC KPI-C02 denominator definition | Open — current assumption documented in Business Rule 4 of Section 5.5; confirm with CEO |
| OQ-07 | Should the Genie natural language space be restricted to Gold layer tables only, or can it access Silver tables for drill-down queries? | CPG-030 Genie configuration scope | Open — recommend Gold-only for board demo; deferred to Week 4 |
| OQ-08 | For the Distribution domain, what is the agreed column schema of the logistics partner CSV? A schema document from the logistics partner simulation is needed before CPG-016 can begin. | CPG-016 Bronze ingestion design | Open — Data Architect must define the synthetic CSV schema before CPG-016 development starts |

---

*This PRD is the authoritative requirements specification for Phase 2 of the FreshSip CPG Data Platform. It supersedes all informal requirement notes captured during brainstorming. Changes to any section after Data Architecture begins require Product Owner sign-off and a version increment.*
