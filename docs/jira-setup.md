# Jira Project Plan — FreshSip Beverages CPG Data Platform

**Generated:** 2026-04-05
**Last Updated:** 2026-04-05 (Phase 3 Solutioning complete)
**Project Key:** CPG
**Platform:** Jira Software (Scrum)

---

## Phase 3 Solutioning Update — 2026-04-05

**Status:** Architecture complete. Gate check: CONDITIONAL PASS. Sprint 1 cleared to start.

### Architecture Artifacts Produced
- `_bmad-output/architecture/architecture-overview.md` — High-level Mermaid architecture diagram
- `_bmad-output/architecture/schema-bronze.md` — 8 Bronze table DDLs
- `_bmad-output/architecture/schema-silver.md` — 11 Silver + 2 reference table DDLs with SCD Type 2
- `_bmad-output/architecture/schema-gold.md` — Star schema: 6 dims + 4 facts + 18 KPI tables with SQL
- `_bmad-output/architecture/data-quality-rules.md` — DQ rules by layer and table
- `_bmad-output/architecture/data-lineage.md` — Column-level lineage per domain
- `_bmad-output/architecture/diagrams/*.mmd` — 6 standalone Mermaid diagram files
- `_bmad-output/architecture/traceability-matrix.md` — KPI→story→table traceability
- `config/schemas/*.json` — 8 JSON Schema files for Bronze validation
- `_bmad-output/architecture/phase3-solutioning-gate-check.md` — Gate check report

### KPI Coverage: 14/20 Fully Covered
- Sales: 4/4 ✅ | Inventory: 3/4 ⚠️ | Production: 3/4 ⚠️ | Distribution: 3/4 ⚠️ | Customers: 3/4 ⚠️

### Tickets Requiring Updates (from architecture validation)

| Ticket | Update Required | Severity |
|---|---|---|
| CPG-012 | AC-1: Table name should be `brz_freshsip.iot_sensor_events_raw` (not `iot_production_raw`) | LOW |
| CPG-014 | AC-1: Column `yield_rate_pct` (not `batch_yield_rate_pct`); AC-3: `qc_pass_rate_pct` and `qc_warn_flag` | LOW |
| CPG-018 | AC-3: Column `fulfillment_warn_flag` (not `fulfillment_alert_flag`) | LOW |
| CPG-019 | AC-1: Column `cost_per_case` (not `cost_per_case_usd`) | LOW |
| CPG-021 | AC-1/4: SCD columns are `valid_from`/`valid_to` (not `effective_start_date`/`effective_end_date`) | LOW |
| CPG-010 | AC-3: Column `turnover_warn_flag` — to be renamed `turnover_alert_flag` per A4 action | LOW |
| CPG-029 | AC-1: `pipeline_dq_log` schema not yet defined — blocked pending GAP-14 resolution | MEDIUM |

### New Architecture Tickets (Recommended — Add to Backlog)

| Proposed ID | Title | Epic | Sprint | Points | Priority |
|---|---|---|---|---|---|
| CPG-031 | Fix: Add `batch_id` FK to `slv_freshsip.shipments` DDL and Bronze logistics schema | CPG-E04 | Pre-Sprint 3 | 2 | CRITICAL |
| CPG-032 | Fix: Define `slv_freshsip.pipeline_dq_log` schema and DQ monitoring table | CPG-E08 | Sprint 1 | 2 | HIGH |
| CPG-033 | Fix: Correct KPI-I02 SQL alias error and add warehouse_id to KPI-I03 DSI subquery | CPG-E02 | Pre-Sprint 3 | 1 | HIGH |
| CPG-034 | Fix: Standardize alert flag naming (`_warn_flag` → `_alert_flag`) across all Gold DDLs | CPG-E06 | Pre-Sprint 3 | 1 | HIGH |
| CPG-035 | Design: Stateful `consecutive_weeks_in_worst10` counter logic for KPI-D04 | CPG-E04 | Pre-Sprint 3 | 3 | HIGH |
| CPG-036 | Validate: Confirm ERP customer CSV contains spend allocation columns for KPI-C02 CAC | CPG-E05 | Sprint 2 | 1 | HIGH |
| CPG-037 | Fix: Implement prior-period rank lookback for KPI-C01 `rank_movement` column | CPG-E05 | Sprint 4 | 2 | MEDIUM |

---

## Section 1: Project Overview

| Field | Value |
|---|---|
| **Project Name** | FreshSip Beverages CPG Data Platform |
| **Project Key** | CPG |
| **Project Type** | Scrum |
| **Board Type** | Software Scrum Board |
| **Product Owner** | CEO (Reporter on all stories) |
| **Development Team** | AI Agent Team (Data Engineer, Architect, Deployer, QA) |
| **Sprint Length** | 1 week |
| **Total Sprints** | 4 |
| **Project Start** | 2026-04-07 |
| **Project End** | 2026-05-01 |
| **Goal** | Deliver a live Databricks AI/BI executive dashboard covering all 5 operational domains (Sales, Inventory, Production, Distribution, Customers), powered by a Medallion Architecture Bronze → Silver → Gold data platform, ready for board presentation. |

### Timeline Summary

| Sprint | Dates | Focus |
|---|---|---|
| Sprint 1 | 2026-04-07 to 2026-04-11 | Infrastructure, schemas, synthetic data, Bronze layer (Sales + Inventory) |
| Sprint 2 | 2026-04-14 to 2026-04-18 | Silver layer (all domains), DQ framework, Bronze (Production, Distribution, Customers) |
| Sprint 3 | 2026-04-21 to 2026-04-25 | Gold layer (Sales, Inventory, Production, Distribution) — all KPIs from core domains |
| Sprint 4 | 2026-04-28 to 2026-05-01 | Customers domain completion, dashboard pages, testing, deployment, board demo prep |

---

## Section 2: Epics

| Epic ID | Epic Name | Domain / Area | Sprint(s) | Total Story Points |
|---|---|---|---|---|
| CPG-E01 | Sales Data Pipeline | Sales | S1, S2, S3 | 26 |
| CPG-E02 | Inventory Data Pipeline | Inventory | S1, S2, S3 | 18 |
| CPG-E03 | Production Data Pipeline | Production | S2, S3 | 21 |
| CPG-E04 | Distribution Data Pipeline | Distribution | S2, S3 | 16 |
| CPG-E05 | Customers Data Pipeline | Customers | S2, S3, S4 | 19 |
| CPG-E06 | Infrastructure & DevOps | Infra | S1 | 21 |
| CPG-E07 | Dashboards & Visualization | Dashboard | S4 | 24 |
| CPG-E08 | Testing & Quality Assurance | QA | S1, S3, S4 | 8 |
| CPG-E09 | Deployment & Documentation | Deploy | S1, S4 | 13 |

> Note: CPG-E07 (Dashboards) maps to PRD epic CPG-E06; CPG-E08/E09 map to PRD epic CPG-E07 (Infrastructure and Operations). Epic IDs in this plan are Jira-facing names for organizational clarity.

---

## Section 3: Sprint Plan Overview

| Sprint | Dates | Focus | Story Points | Exit Criteria |
|---|---|---|---|---|
| Sprint 1 | 2026-04-07 to 2026-04-11 | Infrastructure, schemas, synthetic data, Bronze layer (Sales + Inventory), CI/CD, Orchestration setup, DQ framework core | ~34 pts | Hive metastore schemas created; synthetic data generated and loaded; `brz_freshsip.pos_transactions_raw`, `brz_freshsip.erp_sales_raw`, and `brz_freshsip.erp_inventory_raw` populated; GitHub Actions CI/CD workflow passing; DQ framework library deployed |
| Sprint 2 | 2026-04-14 to 2026-04-18 | Silver layer (Sales, Inventory, Production, Distribution), Bronze layer (Production IoT, Logistics, Customers) | ~37 pts | All Silver tables pass DQ checks; `slv_freshsip.sales_transactions`, `slv_freshsip.inventory_stock`, `slv_freshsip.production_batches`, `slv_freshsip.shipments` populated; no P0 bugs |
| Sprint 3 | 2026-04-21 to 2026-04-25 | Gold layer (Sales, Inventory, Production, Distribution) — 8 KPI tables, all 16 core KPIs queryable | ~36 pts | All Gold tables for Sales, Inventory, Production, and Distribution domains populated; all 16 core KPIs return non-null values for the full synthetic data range |
| Sprint 4 | 2026-04-28 to 2026-05-01 | Customers domain completion (Silver + Gold), dashboard pages, testing, deployment, board demo prep | ~40 pts (Sprint 4 is over target by ~6 pts; CPG-030 flagged as stretch — see risk notes) | Dashboard loads < 5s; CEO demo walkthrough passes; all 20 KPIs queryable; board deck exhibit ready; CPG-030 (Genie) is stretch and may slip |

---

## Section 4: Full Ticket List

---

### SPRINT 1

---

#### CPG-001: Databricks Workspace Setup — Schemas, Cluster Policy, DABs Connection

**Type:** Story
**Epic:** CPG-E06 — Infrastructure & DevOps
**Sprint:** Sprint 1
**Story Points:** 8
**Priority:** Highest
**Labels:** `infra`, `bronze`, `blocking`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Set up the foundational Databricks workspace environment that all pipeline development depends on. This includes creating the three Hive metastore databases (`brz_freshsip`, `slv_freshsip`, `gld_freshsip`), configuring a cluster policy for the project, and connecting the GitHub repository via Databricks Asset Bundles (DABs). Without this story, no pipeline code can be deployed or run. This is the single highest-priority story in the entire project and blocks every other story.

**Acceptance Criteria:**
- [ ] AC-1: Given the project repository, when the DABs deployment command is run (`databricks bundle deploy`), then the three Hive metastore databases (`brz_freshsip`, `slv_freshsip`, `gld_freshsip`) exist and are accessible via `SHOW DATABASES`.
- [ ] AC-2: Given the Databricks workspace, when a cluster is started using the project cluster policy, then it starts successfully within 5 minutes and remains active during pipeline execution without unexpected termination.
- [ ] AC-3: Given the GitHub repository, when a push is made to the `main` branch, then the GitHub Actions CI/CD workflow runs and exits with status code 0 (even if only a stub workflow is present at this stage).
- [ ] AC-4: Given the workspace, when any agent runs `SHOW DATABASES`, then all three layer databases (`brz_freshsip`, `slv_freshsip`, `gld_freshsip`) are present and empty (ready for data).

**Technical Notes:**
- Use Hive metastore as primary catalog (Unity Catalog upgrade deferred — Community Edition constraint)
- DABs config file: `config/databricks/databricks.yml`; target environments: `dev` and `prod`
- Cluster policy should enforce: auto-termination after 60 min idle, DBR 14.x LTS runtime, single-node allowed for dev
- Create `config/schemas/` DDL files documenting all layer naming conventions

**Dependencies:** None

---

#### CPG-002: Synthetic Data Generation — 13 Months, All 5 Domains, Realistic Patterns

**Type:** Story
**Epic:** CPG-E06 — Infrastructure & DevOps
**Sprint:** Sprint 1
**Story Points:** 5
**Priority:** Highest
**Labels:** `infra`, `synthetic-data`, `blocking`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Generate 13 months of realistic synthetic data for all five operational domains (Sales, Inventory, Production, Distribution, Customers) in their correct source formats (JSON for POS, CSV for ERP and logistics, Parquet/CSV for IoT sensors). The data must contain embedded seasonal patterns, realistic referential integrity across shared keys (retailer_id, sku_id, batch_id, shipment_id), and plausible business narratives — not random noise. Without this story, no Bronze or downstream pipeline can be tested and no dashboard KPIs will have data to display.

**Acceptance Criteria:**
- [ ] AC-1: Given the data generation script, when executed, then all output files cover a date range from 13 months prior to the current date (2025-03-01) through the current date (2026-04-05), with no gaps in monthly coverage.
- [ ] AC-2: Given the synthetic POS JSON files, when the Bronze ingestion pipeline runs, then the row count in `brz_freshsip.pos_transactions_raw` matches the expected synthetic row count within 1% tolerance.
- [ ] AC-3: Given the synthetic data, when any Gold KPI is computed for any trailing 12-month period, then the output contains non-null values for every month in that range (no silent month gaps).
- [ ] AC-4: Given the synthetic data, when revenue trend is plotted on the dashboard, then visible and plausible seasonal patterns are present (e.g., higher Q4 sales, lower Q1) — not a flat or purely random distribution.
- [ ] AC-5: Given all domain datasets, when joined on shared keys (retailer_id, sku_id, batch_id, shipment_id), then referential integrity holds for >= 95% of records.

**Technical Notes:**
- Output location: `data/synthetic/` with subdirectories per domain and file type
- Use Databricks AI Dev Kit `databricks-synthetic-data-gen` skill for generation patterns
- Embed at least 2 narrative events (e.g., a stockout event in month 7, a high-yield production week in month 4) for demonstration purposes
- File naming convention: `{domain}_{entity}_{YYYY-MM-DD}.{ext}` (e.g., `sales_pos_2026-04-01.json`)

**Dependencies:** CPG-001

---

#### CPG-003: Bronze Ingestion — Sales POS JSON (Hourly Micro-Batch)

**Type:** Story
**Epic:** CPG-E01 — Sales Data Pipeline
**Sprint:** Sprint 1
**Story Points:** 5
**Priority:** Highest
**Labels:** `sales`, `bronze`, `pipeline`, `mvp`, `blocking`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Bronze layer ingestion pipeline for hourly POS transaction JSON files into `brz_freshsip.pos_transactions_raw`. This pipeline is the first link in the revenue data chain: without it, no Sales Silver or Gold KPIs can be computed. The pipeline must be append-only with schema-on-read, handle malformed records gracefully via an error log, and run reliably on an hourly trigger cadence.

**Acceptance Criteria:**
- [ ] AC-1: Given a new hourly POS JSON file lands in the source directory, when the Bronze ingestion pipeline runs, then all records from that file appear in `brz_freshsip.pos_transactions_raw` within 65 minutes of file arrival (1-hour SLA with 5-minute buffer).
- [ ] AC-2: Given the Bronze pipeline, when it runs, then no schema transformations are applied — data is stored exactly as received (schema-on-read, append-only; no type casts, renames, or derived columns).
- [ ] AC-3: Given a JSON file with malformed records (missing required fields, bad JSON syntax), when the Bronze pipeline processes it, then valid records are ingested and malformed records are logged to `brz_freshsip.pipeline_error_log` with file name, line number, and error reason.
- [ ] AC-4: Given the Bronze table after ingestion, when the row count is compared to the source file record count (excluding malformed), then the counts match exactly — no records lost or duplicated.

**Technical Notes:**
- Target table: `brz_freshsip.pos_transactions_raw` (Delta format, append-only, partitioned by `ingestion_date`)
- Use notebook-based ingestion with PySpark `readStream` trigger `once` pattern (Community Edition fallback if SDP unavailable)
- Add `_ingestion_ts` and `_source_file` metadata columns to every Bronze record
- Error log table: `brz_freshsip.pipeline_error_log` shared across all Bronze pipelines

**Dependencies:** CPG-001, CPG-002

---

#### CPG-004: Bronze Ingestion — Sales ERP CSV (Daily Batch)

**Type:** Story
**Epic:** CPG-E01 — Sales Data Pipeline
**Sprint:** Sprint 1
**Story Points:** 3
**Priority:** High
**Labels:** `sales`, `bronze`, `pipeline`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Bronze layer ingestion pipeline for daily ERP sales order and returns CSV files into `brz_freshsip.erp_sales_raw` and `brz_freshsip.erp_returns_raw`. ERP data provides the authoritative invoice prices and return adjustments that net revenue calculations depend on. The pipeline must preserve the raw CSV structure exactly and raise a schema alert if the header row does not match the expected schema.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily ERP sales CSV file, when the Bronze ingestion pipeline runs, then all records appear in `brz_freshsip.erp_sales_raw` within 30 minutes of file availability (daily SLA).
- [ ] AC-2: Given the Bronze pipeline, when it runs, then the raw CSV structure is preserved with no column renames, type conversions, or derived columns applied to `erp_sales_raw`.
- [ ] AC-3: Given a CSV with a header row mismatch vs. the expected schema definition, when the pipeline runs, then it halts ingestion for that file, logs a schema alert to `brz_freshsip.pipeline_error_log`, and does not corrupt previously loaded data.
- [ ] AC-4: Given returns data in the daily ERP file, when it is ingested, then return records are stored in a separate table `brz_freshsip.erp_returns_raw`, distinguishable from forward sales records.

**Technical Notes:**
- Target tables: `brz_freshsip.erp_sales_raw` and `brz_freshsip.erp_returns_raw` (Delta, append-only, partitioned by `ingestion_date`)
- Use PySpark batch read with explicit `header=True, inferSchema=False` — no schema inference at Bronze layer
- Schema definition stored in `config/schemas/bronze/erp_sales_schema.json`
- Share error log table `brz_freshsip.pipeline_error_log` with CPG-003

**Dependencies:** CPG-001, CPG-002

---

#### CPG-008: Bronze Ingestion — Inventory ERP CSV (Daily Batch)

**Type:** Story
**Epic:** CPG-E02 — Inventory Data Pipeline
**Sprint:** Sprint 1
**Story Points:** 3
**Priority:** High
**Labels:** `inventory`, `bronze`, `pipeline`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Bronze layer ingestion pipeline for daily ERP inventory snapshot CSV files into `brz_freshsip.erp_inventory_raw`. This provides the raw warehouse stock data that flows through Silver into the DSI, reorder alert, and inventory turnover Gold KPIs. The pipeline must handle missing-file days gracefully without creating zero-row snapshots that could corrupt stock calculations.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily ERP inventory CSV, when the Bronze pipeline runs, then records appear in `brz_freshsip.erp_inventory_raw` within 30 minutes of file availability.
- [ ] AC-2: Given the Bronze table, when queried after ingestion, then each record contains a non-null `warehouse_id`, `sku_id`, `units_on_hand`, and `snapshot_date` in their raw source format (no type casting).
- [ ] AC-3: Given a day where no CSV file arrives (holiday or system outage), when the pipeline runs, then it logs a missing-file alert to `brz_freshsip.pipeline_error_log` and does not create a zero-row snapshot entry that would corrupt downstream stock level calculations.

**Technical Notes:**
- Target table: `brz_freshsip.erp_inventory_raw` (Delta, append-only, partitioned by `ingestion_date`)
- Implement missing-file detection by checking for expected file at configurable path before attempting read
- Schema definition: `config/schemas/bronze/erp_inventory_schema.json`
- Missing file alert should include: `expected_file_path`, `pipeline_name`, `alert_ts`, and `alert_reason = 'FILE_NOT_FOUND'`

**Dependencies:** CPG-001, CPG-002

---

#### CPG-027: Databricks Workflows Orchestration Setup

**Type:** Story
**Epic:** CPG-E09 — Deployment & Documentation
**Sprint:** Sprint 1
**Story Points:** 5
**Priority:** High
**Labels:** `infra`, `orchestration`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Configure Databricks Workflows job definitions for all three pipeline schedules: (1) daily batch at 06:00 UTC covering all five domain Silver and Gold pipelines in dependency order, (2) hourly POS refresh covering Sales Bronze through Gold, and (3) 5-minute micro-batch for Production IoT Bronze through Gold. This story creates the job skeleton and schedule definitions; the actual pipeline tasks will be wired in as downstream stories complete. Job configs must be stored in `config/databricks/` as code for DABs deployment.

**Acceptance Criteria:**
- [ ] AC-1: Given the Databricks Workflows configuration, when the daily batch job is viewed in the UI, then it shows a schedule of `0 6 * * *` (06:00 UTC daily) and the job definition includes task stubs for all five domain Silver and Gold pipelines in correct dependency order.
- [ ] AC-2: Given the hourly POS job, when it runs after CPG-003 and CPG-006 are completed, then it completes within 60 minutes and updates `gld_freshsip.sales_daily_revenue` before the next trigger fires.
- [ ] AC-3: Given the micro-batch Production job, when it runs at 5-minute intervals, then it processes all new IoT records since the last checkpoint and completes before the next trigger fires.
- [ ] AC-4: Given any job failure, when the Workflows UI is viewed, then the failed run is flagged with an error status and the error message is accessible in the run logs within 2 minutes of failure.

**Technical Notes:**
- Job config files: `config/databricks/jobs/daily_batch_job.yml`, `hourly_pos_job.yml`, `iot_microbatch_job.yml`
- Use Databricks AI Dev Kit `databricks-jobs` skill for job definition patterns
- Wire actual pipeline notebook/task references via DABs resource references — do not hardcode paths
- Add email/webhook alerting stub for job failure (even if notifications are not fully configured in this sprint)

**Dependencies:** CPG-001

---

#### CPG-028: CI/CD Pipeline — GitHub Actions + DABs Config

**Type:** Story
**Epic:** CPG-E09 — Deployment & Documentation
**Sprint:** Sprint 1
**Story Points:** 5
**Priority:** High
**Labels:** `infra`, `cicd`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Create a GitHub Actions CI/CD pipeline that runs unit tests and linting on every pull request to `main`, and deploys to Databricks via DABs on every merge. This ensures all code changes are automatically validated and deployed without manual intervention. The workflow must never expose Databricks API tokens in logs, using GitHub Secrets exclusively for credential management.

**Acceptance Criteria:**
- [ ] AC-1: Given a pull request to the `main` branch, when the PR is opened or updated, then the GitHub Actions workflow runs unit tests and linting checks within 5 minutes and posts a pass/fail status check to the PR (blocking merge on failure).
- [ ] AC-2: Given a merge to `main`, when the GitHub Actions deployment workflow runs, then the DABs `bundle deploy` command completes successfully and updates Databricks job and pipeline definitions without manual intervention.
- [ ] AC-3: Given a deployment that fails (e.g., schema conflict, syntax error in pipeline code), when the GitHub Actions workflow runs, then it exits with a non-zero status code and the error details are available in the workflow run log.
- [ ] AC-4: Given the CI/CD workflow, when it runs, then it never exposes Databricks API tokens or other secrets in workflow logs — all secrets accessed exclusively via GitHub Secrets.

**Technical Notes:**
- Workflow files: `.github/workflows/ci.yml` (PR validation), `.github/workflows/deploy.yml` (merge to main)
- Use Databricks AI Dev Kit `databricks-bundles` skill for DABs configuration patterns
- Required GitHub Secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
- Linting: `flake8` for Python; SQL linting via `sqlfluff` (or skip SQL lint if too complex for sprint scope)
- Unit test runner: `pytest tests/unit/`

**Dependencies:** CPG-001

---

#### CPG-029: Data Quality Framework — Reusable DQ Check Library

**Type:** Story
**Epic:** CPG-E08 — Testing & Quality Assurance
**Sprint:** Sprint 1
**Story Points:** 3
**Priority:** High
**Labels:** `dq`, `infra`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the reusable data quality check library that all Silver and Gold pipeline runs will call to perform null checks, duplicate key detection, range validation, and row rejection logging. This sprint delivers the core framework only — domain-specific DQ rules are wired in as each domain's Silver pipeline is built. The DQ log table `slv_freshsip.pipeline_dq_log` must be created and tested with a stub pipeline run.

**Acceptance Criteria:**
- [ ] AC-1: Given any Silver pipeline run that calls the DQ framework, when it completes, then a new row is written to `slv_freshsip.pipeline_dq_log` with: `pipeline_name`, `run_ts`, `table_name`, `rows_processed`, `rows_rejected`, `null_rate_by_column` (JSON map), `duplicate_count`, and `run_status`.
- [ ] AC-2: Given any table where the null rate for a required column exceeds 1%, when the DQ check function is called, then `run_status` is set to `'WARN'` or `'FAIL'` (never `'OK'`) in the monitoring log entry.
- [ ] AC-3: Given any table where duplicate primary keys are detected, when the DQ check runs, then `run_status = 'FAIL'` and the duplicate keys are recorded in the `dq_detail` column of the log entry.
- [ ] AC-4: Given the DQ framework module in `src/utils/dq_checks.py`, when its unit tests are run, then all tests pass with 100% coverage of the null check, duplicate check, and range validation functions.

**Technical Notes:**
- Module location: `src/utils/dq_checks.py` with public functions: `check_null_rates()`, `check_duplicate_keys()`, `check_range_validity()`, `log_dq_result()`
- DQ log table: `slv_freshsip.pipeline_dq_log` (Delta, append-only)
- Unit tests: `tests/unit/test_dq_checks.py`
- Design as a decorator or wrapper callable from any pipeline notebook — keep the API simple

**Dependencies:** CPG-001

---

### SPRINT 2

---

#### CPG-005: Silver Layer — Sales Transactions (Cleaning, Dedup, Business Rules)

**Type:** Story
**Epic:** CPG-E01 — Sales Data Pipeline
**Sprint:** Sprint 2
**Story Points:** 8
**Priority:** Highest
**Labels:** `sales`, `silver`, `pipeline`, `mvp`, `blocking`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Silver layer pipeline that cleans, deduplicates, type-casts, and validates Bronze sales data into the authoritative `slv_freshsip.sales_transactions` and `slv_freshsip.sales_returns` tables. This is the most critical Silver pipeline in the project: every downstream Sales Gold KPI — Daily Revenue, Gross Margin, and the Customer domain KPIs — reads from these tables. Business rule enforcement (valid price, valid quantity, valid category/channel, orphan return detection) must be comprehensive.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline run on Bronze sales tables, when it completes, then the null rate for all non-nullable columns (`transaction_id`, `sku_id`, `retailer_id`, `unit_price`, `quantity_sold`, `transaction_date`) in `slv_freshsip.sales_transactions` is less than 1%.
- [ ] AC-2: Given duplicate `transaction_id` values across Bronze loads, when the Silver pipeline runs, then `slv_freshsip.sales_transactions` contains exactly one record per `transaction_id` (dedup key = `transaction_id`, keep first by `_ingestion_ts`).
- [ ] AC-3: Given records with `unit_price <= 0` or `quantity_sold <= 0`, when the Silver pipeline runs, then those records are rejected to the DQ error log with reason `INVALID_PRICE_OR_QTY` and are not written to `slv_freshsip.sales_transactions`.
- [ ] AC-4: Given return records from `brz_freshsip.erp_returns_raw`, when the Silver pipeline runs, then they appear in `slv_freshsip.sales_returns` with correct `return_amount` and a valid reference back to `transaction_id` in `slv_freshsip.sales_transactions` (orphan returns logged, not processed).
- [ ] AC-5: Given the Silver pipeline run completion, when queried, then a DQ summary record is written to `slv_freshsip.pipeline_dq_log` including: run timestamp, rows processed, rows rejected, null rates per column, and duplicate count.

**Technical Notes:**
- Source tables: `brz_freshsip.pos_transactions_raw`, `brz_freshsip.erp_sales_raw`, `brz_freshsip.erp_returns_raw`
- Target tables: `slv_freshsip.sales_transactions` (Delta, partitioned by `transaction_date`), `slv_freshsip.sales_returns`
- Business rules from PRD Section 5.1: category must be in `['Carbonated Soft Drinks', 'Flavored Water', 'Energy Drinks', 'Juice Blends']`; channel must be in `['Retail', 'Wholesale', 'Direct-to-Consumer']`
- Type casts: `transaction_date → DATE`, `unit_price → DECIMAL(12,2)`, `quantity_sold → INTEGER`

**Dependencies:** CPG-003, CPG-004, CPG-029

---

#### CPG-009: Silver Layer — Inventory Stock Levels + Reorder Points Reference Table

**Type:** Story
**Epic:** CPG-E02 — Inventory Data Pipeline
**Sprint:** Sprint 2
**Story Points:** 5
**Priority:** High
**Labels:** `inventory`, `silver`, `pipeline`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Silver layer pipeline that cleans and conforms Bronze inventory snapshot data into `slv_freshsip.inventory_stock`, and populates the `slv_freshsip.ref_reorder_points` reference table with reorder thresholds per SKU per warehouse. The reorder points table is the authoritative trigger source for all stock alert KPIs. Records with negative `units_on_hand` must be rejected.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline run, when it completes, then `slv_freshsip.inventory_stock` contains one row per `(sku_id, warehouse_id, snapshot_date)` with no duplicate combinations.
- [ ] AC-2: Given records with `units_on_hand < 0`, when the Silver pipeline runs, then those records are rejected to the DQ log with reason code `INVALID_STOCK_LEVEL` and are not written to `slv_freshsip.inventory_stock`.
- [ ] AC-3: Given the reorder point reference table, when `slv_freshsip.ref_reorder_points` is queried, then it contains a non-null `reorder_point_units` value for every `(sku_id, warehouse_id)` combination present in `slv_freshsip.inventory_stock`.
- [ ] AC-4: Given the Silver pipeline run completion, when queried, then the pipeline monitoring table `slv_freshsip.pipeline_dq_log` is updated with row counts, rejection counts, and run timestamp.

**Technical Notes:**
- Source table: `brz_freshsip.erp_inventory_raw`
- Target tables: `slv_freshsip.inventory_stock` (Delta, partitioned by `snapshot_date`), `slv_freshsip.ref_reorder_points` (static reference, upserted)
- Reorder points can be seeded from synthetic data constants or a config CSV — define in `config/schemas/silver/reorder_points_seed.csv`
- Primary key for deduplication: `(sku_id, warehouse_id, snapshot_date)` — keep last record by `_ingestion_ts` if duplicates exist

**Dependencies:** CPG-008, CPG-029

---

#### CPG-012: Bronze Ingestion — Production IoT Sensors (Micro-Batch Every 5 Min)

**Type:** Story
**Epic:** CPG-E03 — Production Data Pipeline
**Sprint:** Sprint 2
**Story Points:** 8
**Priority:** High
**Labels:** `production`, `bronze`, `pipeline`, `streaming`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Bronze layer micro-batch ingestion pipeline for IoT sensor event data from production lines into `brz_freshsip.iot_production_raw`. This pipeline runs on a 5-minute trigger interval and must guarantee no event loss between consecutive windows (verified by sequence number continuity). The IoT pipeline is the most technically complex Bronze story due to the streaming pattern and sequence continuity requirement.

**Acceptance Criteria:**
- [ ] AC-1: Given new IoT sensor event records in the source directory, when the micro-batch pipeline trigger fires, then those records appear in `brz_freshsip.iot_production_raw` within 10 minutes of the trigger time (5-min trigger + 5-min processing SLA).
- [ ] AC-2: Given the Bronze pipeline, when it runs, then IoT records are appended without transformation (schema-on-read, append-only; no derived columns except `_ingestion_ts` and `_source_file`).
- [ ] AC-3: Given a production line generating events at 1-second intervals, when the micro-batch runs, then no events are dropped between consecutive trigger windows — verified by event `sequence_number` continuity check logged to `brz_freshsip.pipeline_error_log` on any gap.
- [ ] AC-4: Given a trigger window with zero new events, when the pipeline runs, then it completes without error and does not write empty records or a zero-row partition to the Bronze table.

**Technical Notes:**
- Target table: `brz_freshsip.iot_production_raw` (Delta, append-only, partitioned by `event_date`)
- Use PySpark Structured Streaming with `trigger(processingTime="5 minutes")` or trigger-once pattern for Community Edition
- Checkpoint location: `config/databricks/checkpoints/iot_production/` (store in DBFS or volume)
- Read Databricks `databricks-spark-structured-streaming` skill before implementing

**Dependencies:** CPG-001, CPG-002

---

#### CPG-013: Silver Layer — Production Batch Records and QC Results

**Type:** Story
**Epic:** CPG-E03 — Production Data Pipeline
**Sprint:** Sprint 2
**Story Points:** 5
**Priority:** High
**Labels:** `production`, `silver`, `pipeline`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Silver layer pipeline that aggregates IoT Bronze event records into consolidated production batch records in `slv_freshsip.production_batches`, and derives QC status and production events in `slv_freshsip.production_events`. Each batch must appear as exactly one row in Silver regardless of how many IoT events it spans. The over-yield flag and QC status validation rules must be enforced.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline run, when it completes, then `slv_freshsip.production_batches` contains exactly one row per `batch_id` with non-null `actual_output_cases`, `expected_output_cases`, `qc_status`, `production_line_id`, `batch_start_ts`, and `batch_end_ts`.
- [ ] AC-2: Given a `batch_id` with multiple IoT events, when the Silver pipeline aggregates them, then those events are collapsed into a single batch record with no `batch_id` appearing more than once in `slv_freshsip.production_batches`.
- [ ] AC-3: Given records where `actual_output_cases > expected_output_cases * 1.10` (over 10% over-yield, physically implausible), when the Silver pipeline runs, then those records are flagged with `dq_flag = 'YIELD_EXCEEDS_EXPECTED'` and logged to the DQ log.
- [ ] AC-4: Given batch records with `qc_status NOT IN ('PASS', 'FAIL', 'PENDING')`, when the Silver pipeline runs, then those records are rejected with reason code `INVALID_QC_STATUS` and do not appear in `slv_freshsip.production_batches`.

**Technical Notes:**
- Source table: `brz_freshsip.iot_production_raw`
- Target tables: `slv_freshsip.production_batches` (Delta, partitioned by `batch_date`), `slv_freshsip.production_events`
- Aggregation: group by `batch_id`, take `MIN(event_ts)` as `batch_start_ts`, `MAX(event_ts)` as `batch_end_ts`
- QC status is the last recorded `qc_status` event per `batch_id` by `event_ts`

**Dependencies:** CPG-012, CPG-029

---

#### CPG-016: Bronze Ingestion — Logistics Partner CSV (Daily Batch)

**Type:** Story
**Epic:** CPG-E04 — Distribution Data Pipeline
**Sprint:** Sprint 2
**Story Points:** 3
**Priority:** High
**Labels:** `distribution`, `bronze`, `pipeline`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Bronze layer ingestion pipeline for daily logistics partner shipment CSV files into `brz_freshsip.logistics_shipments_raw`. This is the sole source of truth for all distribution KPIs (OTD%, fulfillment rate, cost per case, worst routes). The pipeline must handle multi-carrier files and partition records by `ingestion_date` for efficient downstream processing.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily logistics CSV, when the Bronze pipeline runs, then records appear in `brz_freshsip.logistics_shipments_raw` within 30 minutes of file availability.
- [ ] AC-2: Given the Bronze table after ingestion, when queried, then all raw columns are present and unmodified (no type casts, renames, or derived columns — only `_ingestion_ts` and `_source_file` metadata added).
- [ ] AC-3: Given a logistics file containing records for multiple carriers, when the pipeline runs, then all carriers' records are ingested into the same table `brz_freshsip.logistics_shipments_raw`, partitioned by `ingestion_date` and queryable by `carrier_id`.

**Technical Notes:**
- Target table: `brz_freshsip.logistics_shipments_raw` (Delta, append-only, partitioned by `ingestion_date`)
- Schema definition: `config/schemas/bronze/logistics_shipments_schema.json`
- Multi-carrier handling: the synthetic data should include at least 3 carrier codes; all land in the same table
- Share `brz_freshsip.pipeline_error_log` for schema mismatch and missing-file alerts

**Dependencies:** CPG-001, CPG-002

---

#### CPG-017: Silver Layer — Shipments, Routes, Fulfillment Records

**Type:** Story
**Epic:** CPG-E04 — Distribution Data Pipeline
**Sprint:** Sprint 2
**Story Points:** 5
**Priority:** High
**Labels:** `distribution`, `silver`, `pipeline`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Silver layer pipeline that cleans and conforms Bronze logistics data into `slv_freshsip.shipments`, deriving the `is_fully_shipped` flag per order and validating delivery dates and logistics costs. This table is the single source of truth for all four Distribution KPIs (OTD%, fulfillment rate, cost per case, worst routes). The `is_fully_shipped` derivation and the missing delivery date flag are the two most important business rules in this pipeline.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline run, when it completes, then `slv_freshsip.shipments` contains one row per `shipment_id` with non-null `actual_delivery_date`, `promised_delivery_date`, `logistics_cost_usd`, `cases_delivered`, `order_id`, `channel`, `region`, and `route_id`.
- [ ] AC-2: Given an order with all line items fully shipped (`quantity_shipped >= quantity_ordered` for all lines), when the Silver pipeline runs, then that `order_id` has `is_fully_shipped = true` in `slv_freshsip.shipments`; any partially shipped order has `is_fully_shipped = false`.
- [ ] AC-3: Given records where `actual_delivery_date` is null for shipments with `ship_date` older than 30 days, when the Silver pipeline runs, then those records are flagged with `dq_flag = 'DELIVERY_DATE_MISSING'` and logged to the DQ log.
- [ ] AC-4: Given records where `logistics_cost_usd < 0`, when the Silver pipeline runs, then those records are rejected with reason code `INVALID_LOGISTICS_COST` and do not appear in `slv_freshsip.shipments`.

**Technical Notes:**
- Source table: `brz_freshsip.logistics_shipments_raw`
- Target table: `slv_freshsip.shipments` (Delta, partitioned by `ship_date`)
- Derive `is_fully_shipped` by grouping on `order_id` and checking all line items — requires window function or aggregation join
- Type casts: `ship_date → DATE`, `actual_delivery_date → DATE`, `promised_delivery_date → DATE`, `logistics_cost_usd → DECIMAL(12,2)`

**Dependencies:** CPG-016, CPG-029

---

#### CPG-020: Bronze Ingestion — Customer/Retailer ERP CSV (Daily Batch)

**Type:** Story
**Epic:** CPG-E05 — Customers Data Pipeline
**Sprint:** Sprint 2
**Story Points:** 3
**Priority:** Medium
**Labels:** `customers`, `bronze`, `pipeline`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Bronze layer ingestion pipeline for daily ERP customer and retailer profile CSV files into `brz_freshsip.erp_customers_raw`. Retailer attributes (name, segment, region, account activation date) are needed for Silver enrichment of sales transactions and the Customer Gold KPIs. This story starts customer domain ingestion in Sprint 2 so the Silver SCD Type 2 pipeline (CPG-021) has data to process in Sprint 4.

**Acceptance Criteria:**
- [ ] AC-1: Given a new daily ERP customer CSV, when the Bronze pipeline runs, then records appear in `brz_freshsip.erp_customers_raw` within 30 minutes of file availability.
- [ ] AC-2: Given the Bronze table after ingestion, when queried, then all records include a non-null `retailer_id` and `account_activation_date` in raw source format.
- [ ] AC-3: Given records with duplicate `retailer_id` values in the same daily file, when the pipeline runs, then all duplicate records are ingested into Bronze without deduplication (Bronze is append-only; Silver handles deduplication via SCD Type 2 logic).

**Technical Notes:**
- Target table: `brz_freshsip.erp_customers_raw` (Delta, append-only, partitioned by `ingestion_date`)
- Schema definition: `config/schemas/bronze/erp_customers_schema.json`
- Key columns to preserve in raw: `retailer_id`, `retailer_name`, `retail_segment`, `region`, `account_activation_date`, `credit_terms`, `satisfaction_score`
- No deduplication, no type casting at Bronze layer — strictly append-only

**Dependencies:** CPG-001, CPG-002

---

### SPRINT 3

---

#### CPG-006: Gold Layer — Daily Revenue KPI Table (`gld_freshsip.sales_daily_revenue`)

**Type:** Story
**Epic:** CPG-E01 — Sales Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 5
**Priority:** Highest
**Labels:** `sales`, `gold`, `kpi`, `mvp`, `blocking`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes the Daily Revenue KPI table from Silver sales data, aggregated by `(transaction_date, product_category, region, channel)`. This is the primary KPI on the executive dashboard — it powers the CEO's daily revenue view, MoM comparison, and YoY comparison. The pipeline must run hourly (aligned with POS ingestion) and the `last_updated_ts` column must reflect the freshness SLA.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.sales_daily_revenue` contains one row per `(transaction_date, product_category, region, channel)` combination with a non-null `net_revenue` column.
- [ ] AC-2: Given a specific `(transaction_date, product_category, region, channel)` combination, when `net_revenue` is manually verified against Silver data, then it equals `SUM(unit_price * quantity_sold) - SUM(return_amount)` for that combination within a tolerance of $0.01.
- [ ] AC-3: Given the Gold table, when data for the most recent completed day is queried, then the `last_updated_ts` column reflects a timestamp within 65 minutes of the current time (hourly SLA).
- [ ] AC-4: Given 13 months of synthetic data, when the Gold table is queried for any calendar month in that range, then a non-null and non-zero `net_revenue` value is returned for at least 3 of the 4 product categories.

**Technical Notes:**
- Source tables: `slv_freshsip.sales_transactions`, `slv_freshsip.sales_returns`
- Target table: `gld_freshsip.sales_daily_revenue` (Delta, partitioned by `transaction_date`, Z-ordered on `product_category, region`)
- Refresh pattern: full recalculation for current day; historical days are idempotently overwritten (MERGE on grain key)
- Also compute `gld_freshsip.sales_period_comparison` in this same pipeline for MoM/YoY KPIs (KPI-S02, KPI-S03)

**Dependencies:** CPG-005

---

#### CPG-007: Gold Layer — Gross Margin by SKU (`gld_freshsip.sales_gross_margin_sku`)

**Type:** Story
**Epic:** CPG-E01 — Sales Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 5
**Priority:** High
**Labels:** `sales`, `gold`, `kpi`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes Gross Margin by SKU from Silver sales data joined with the product cost reference table. The VP Sales uses this KPI to identify margin-negative SKUs and drive pricing and product mix decisions. The pipeline must set a `margin_alert_flag = true` for any SKU below 30% gross margin, and label partial-week rows with `is_partial_week = true`.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.sales_gross_margin_sku` contains one row per `(week_start_date, sku_id, product_category)` with non-null `gross_margin_pct` and `net_revenue` columns.
- [ ] AC-2: Given any SKU row, when `gross_margin_pct` is manually verified, then it equals `(net_revenue - cogs) / net_revenue * 100` where `cogs = standard_cost_per_unit * quantity_sold`, within a tolerance of 0.1 percentage points.
- [ ] AC-3: Given a SKU with `gross_margin_pct < 30%`, when the Gold table is queried, then that SKU row has `margin_alert_flag = true`.
- [ ] AC-4: Given the Gold table queried for the current week before the week is complete, then partial-week values are present and labeled `is_partial_week = true`.

**Technical Notes:**
- Source tables: `slv_freshsip.sales_transactions`, `slv_freshsip.sales_returns`, `slv_freshsip.ref_products`
- Target table: `gld_freshsip.sales_gross_margin_sku` (Delta, partitioned by `week_start_date`, Z-ordered on `sku_id`)
- `standard_cost_per_unit` must be joined from `slv_freshsip.ref_products` on `sku_id` — this reference table should be seeded as part of CPG-002 synthetic data
- Week grain: ISO week start (Monday); `week_start_date = DATE_TRUNC('week', transaction_date)`

**Dependencies:** CPG-005

---

#### CPG-010: Gold Layer — Inventory Turnover Rate (`gld_freshsip.inventory_turnover`)

**Type:** Story
**Epic:** CPG-E02 — Inventory Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 5
**Priority:** High
**Labels:** `inventory`, `gold`, `kpi`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes the Inventory Turnover Rate per warehouse over a trailing 30-day rolling window. This KPI helps the Supply Chain Manager assess whether each warehouse is holding too much or too little inventory relative to actual sales velocity. The pipeline must compute COGS from Silver sales and join with Silver inventory to derive turnover rate, and flag warehouses below the 0.5x/30-day threshold.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.inventory_turnover` contains one row per `(week_start_date, warehouse_id)` with a non-null `inventory_turnover_rate` column.
- [ ] AC-2: Given any warehouse row, when `inventory_turnover_rate` is manually verified, then it equals `SUM(cogs_30d) / AVG(inventory_value_30d)` for that warehouse within a tolerance of 0.01 (where `cogs_30d` = trailing 30-day COGS, `inventory_value_30d` = trailing 30-day average inventory value).
- [ ] AC-3: Given a warehouse with `inventory_turnover_rate < 0.5`, when the Gold table is queried, then that row has `turnover_alert_flag = true`.

**Technical Notes:**
- Source tables: `slv_freshsip.inventory_stock`, `slv_freshsip.sales_transactions`, `slv_freshsip.ref_products`
- Target table: `gld_freshsip.inventory_turnover` (Delta, partitioned by `week_start_date`)
- Rolling 30-day window: use `RANGE BETWEEN 29 PRECEDING AND CURRENT ROW` on ordered `snapshot_date`
- Refresh: weekly (Monday 05:00 UTC); full historical recalculation on first run, incremental thereafter

**Dependencies:** CPG-005, CPG-009

---

#### CPG-011: Gold Layer — DSI + Reorder Alert Flag (`gld_freshsip.inventory_dsi`, `gld_freshsip.inventory_stock_levels`)

**Type:** Story
**Epic:** CPG-E02 — Inventory Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 5
**Priority:** Highest
**Labels:** `inventory`, `gold`, `kpi`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes Days Sales of Inventory (DSI) per SKU per warehouse and the Reorder Alert Flag stock levels table. DSI tells the Supply Chain Manager how many days of inventory remain at current sales velocity. The reorder alert flag is one of the most operationally critical KPIs in the platform — it must be refreshed hourly and the `last_updated_ts` SLA must be verifiable.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.inventory_dsi` contains one row per `(snapshot_date, sku_id, warehouse_id)` with a non-null `dsi_days` column.
- [ ] AC-2: Given any SKU-warehouse combination, when `dsi_days` is manually verified, then it equals `units_on_hand / avg_daily_sales_units_30d` for that combination within a tolerance of 0.1 days.
- [ ] AC-3: Given `gld_freshsip.inventory_stock_levels`, when any row has `units_on_hand <= reorder_point_units`, then that row has `reorder_alert_flag = true`.
- [ ] AC-4: Given the Gold tables, when the most recent row for any SKU-warehouse is queried, then `last_updated_ts` reflects a timestamp within 65 minutes of the current time (hourly refresh SLA).

**Technical Notes:**
- Source tables: `slv_freshsip.inventory_stock`, `slv_freshsip.sales_transactions`, `slv_freshsip.ref_reorder_points`
- Target tables: `gld_freshsip.inventory_dsi` (partitioned by `snapshot_date`), `gld_freshsip.inventory_stock_levels` (current snapshot, MERGE upsert)
- `avg_daily_sales_units_30d = SUM(quantity_sold over 30 days) / 30` — requires cross-domain join of inventory and sales Silver tables
- Partition pruning: Z-order both tables on `sku_id, warehouse_id` for dashboard query performance

**Dependencies:** CPG-005, CPG-009

---

#### CPG-014: Gold Layer — Batch Yield Rate + Quality Check Pass Rate

**Type:** Story
**Epic:** CPG-E03 — Production Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 5
**Priority:** High
**Labels:** `production`, `gold`, `kpi`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes Batch Yield Rate per batch and Quality Check Pass Rate per production line per day from Silver production batch records. These KPIs enable VP Operations to monitor production efficiency and quality compliance in near real time. The pipeline must set alert flags for batches below 92% yield and lines below 96% daily QC pass rate.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.production_yield` contains one row per `batch_id` with a non-null `batch_yield_rate_pct` column.
- [ ] AC-2: Given any batch row, when `batch_yield_rate_pct` is manually verified, then it equals `actual_output_cases / expected_output_cases * 100` within a tolerance of 0.01 percentage points.
- [ ] AC-3: Given `gld_freshsip.production_quality`, when any row has `quality_pass_rate_pct < 96%` for a `(production_date, production_line_id)`, then that row has `quality_alert_flag = true`.
- [ ] AC-4: Given the Gold tables queried for the current day, when production batches were completed in the last 10 minutes, then those batches appear in the Gold table (micro-batch SLA: values reflect completions within past 10 minutes).

**Technical Notes:**
- Source table: `slv_freshsip.production_batches`
- Target tables: `gld_freshsip.production_yield` (per-batch grain, partitioned by `batch_date`), `gld_freshsip.production_quality` (daily by production line, partitioned by `production_date`)
- Both tables produced in same pipeline run; use a single PySpark job with two output writes
- `batch_yield_rate_pct` formula: `actual_output_cases / NULLIF(expected_output_cases, 0) * 100`

**Dependencies:** CPG-013

---

#### CPG-015: Gold Layer — Downtime Hours + Batch Traceability

**Type:** Story
**Epic:** CPG-E03 — Production Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 3
**Priority:** Medium
**Labels:** `production`, `gold`, `kpi`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes Downtime Hours by production line by day, and the Batch Traceability Index linking production batches to shipments and retailer deliveries. Downtime quantifies unplanned production losses for VP Operations reporting. Batch traceability enables rapid recall response by verifying the complete chain from batch → shipment → retailer for every completed batch.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.production_downtime` contains one row per `(production_date, production_line_id)` with a non-null `downtime_hours` column.
- [ ] AC-2: Given any downtime row, when `downtime_hours` is manually verified, then it equals `SUM((downtime_end_ts - downtime_start_ts) / 3600.0)` for all `DOWNTIME_UNPLANNED` events on that line on that date, within a tolerance of 0.01 hours.
- [ ] AC-3: Given `gld_freshsip.production_traceability`, when any `batch_id` is queried, then the result includes a valid `shipment_id` and `retailer_id` for all completed and passed batches where `batch_end_ts` is more than 48 hours prior.
- [ ] AC-4: Given `gld_freshsip.production_traceability`, when the traceability index is computed, then it equals the percentage of completed passed batches with a full batch → shipment → retailer chain, matching the KPI-P04 formula within 0.1 percentage points.

**Technical Notes:**
- Source tables: `slv_freshsip.production_batches`, `slv_freshsip.production_events`, `slv_freshsip.shipments`, `slv_freshsip.customers`
- Target tables: `gld_freshsip.production_downtime`, `gld_freshsip.production_traceability`
- Downtime sourced from `production_events` where `event_type = 'DOWNTIME_UNPLANNED'`
- Traceability index requires cross-domain join: production → distribution → customers

**Dependencies:** CPG-013, CPG-017

---

#### CPG-018: Gold Layer — On-Time Delivery % + Order Fulfillment Rate

**Type:** Story
**Epic:** CPG-E04 — Distribution Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 5
**Priority:** High
**Labels:** `distribution`, `gold`, `kpi`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes On-Time Delivery % by channel and region, and Order Fulfillment Rate by channel from Silver shipment data. These KPIs are the primary distribution performance metrics for VP Operations. The fulfillment alert flag must trigger for any channel day below 95% fulfillment rate. Both tables refresh daily at 04:00 UTC aligned with the logistics partner CSV delivery schedule.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.distribution_otd` contains one row per `(ship_date, channel, region)` with a non-null `otd_pct` column.
- [ ] AC-2: Given any OTD row, when `otd_pct` is manually verified, then it equals `COUNT(shipment_id WHERE actual_delivery_date <= promised_delivery_date) / COUNT(shipment_id) * 100` for that `(ship_date, channel, region)` within a tolerance of 0.01 percentage points.
- [ ] AC-3: Given `gld_freshsip.distribution_fulfillment`, when any row has `fulfillment_rate_pct < 95%`, then that row has `fulfillment_alert_flag = true`.
- [ ] AC-4: Given any fulfillment row, when `fulfillment_rate_pct` is manually verified, then it equals `COUNT(order_id WHERE is_fully_shipped = true) / COUNT(order_id) * 100` for that `(order_date, channel)` within a tolerance of 0.01 percentage points.

**Technical Notes:**
- Source table: `slv_freshsip.shipments`
- Target tables: `gld_freshsip.distribution_otd` (partitioned by `ship_date`), `gld_freshsip.distribution_fulfillment` (partitioned by `order_date`)
- OTD formula leverages `is_fully_shipped` flag derived in CPG-017 Silver pipeline
- Refresh schedule: daily at 04:00 UTC (earlier than main daily batch to align with logistics CSV)

**Dependencies:** CPG-017

---

#### CPG-019: Gold Layer — Cost Per Case + Worst-Performing Routes

**Type:** Story
**Epic:** CPG-E04 — Distribution Data Pipeline
**Sprint:** Sprint 3
**Story Points:** 3
**Priority:** Medium
**Labels:** `distribution`, `gold`, `kpi`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes Cost Per Case Delivered by region and route (weekly), and the Worst-Performing Routes ranked table (top 10 worst routes by composite OTD% + cost score). These KPIs help VP Operations identify which distribution routes to prioritize for corrective negotiation or rerouting. The worst routes table must use a `RANK()` window function and retain only the top 10.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.distribution_cost` contains one row per `(week_start_date, region, route_id)` with a non-null `cost_per_case_usd` column.
- [ ] AC-2: Given any cost row, when `cost_per_case_usd` is manually verified, then it equals `SUM(logistics_cost_usd) / SUM(cases_delivered)` for that combination within a tolerance of $0.01.
- [ ] AC-3: Given `gld_freshsip.distribution_route_performance`, when queried for the most recent complete week, then it contains exactly the top 10 worst routes ranked by `worst_route_rank` ascending (rank 1 = worst performance).
- [ ] AC-4: Given any route row in `gld_freshsip.distribution_route_performance`, when `route_otd_pct` is manually verified, then it equals the OTD% formula applied to all shipments on that route in that week within a tolerance of 0.01 percentage points.

**Technical Notes:**
- Source table: `slv_freshsip.shipments`
- Target tables: `gld_freshsip.distribution_cost` (partitioned by `week_start_date`), `gld_freshsip.distribution_route_performance` (top-10 weekly snapshot)
- `worst_route_rank`: `RANK() OVER (ORDER BY route_otd_pct ASC, route_cost_per_case DESC)` — rank ascending (worst OTD first)
- Refresh: weekly at 05:00 UTC Monday (recalculate prior full week)

**Dependencies:** CPG-017

---

### SPRINT 4

---

#### CPG-021: Silver Layer — Retailer Profiles SCD Type 2

**Type:** Story
**Epic:** CPG-E05 — Customers Data Pipeline
**Sprint:** Sprint 4
**Story Points:** 8
**Priority:** High
**Labels:** `customers`, `silver`, `pipeline`, `scd2`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Silver layer SCD Type 2 pipeline that loads Bronze customer data into `slv_freshsip.customers`, preserving historical changes to retailer attributes (segment reclassification, region reassignment). Exactly one row per `retailer_id` must have `is_current = true` at all times. This is the most technically complex Silver story in the project due to the SCD Type 2 merge logic. All downstream Customer Gold KPIs depend on this table.

**Acceptance Criteria:**
- [ ] AC-1: Given the Silver pipeline run on a day where a retailer's `retail_segment` has changed, when it completes, then `slv_freshsip.customers` has a new row with `effective_start_date = today`, and the prior row has `effective_end_date = yesterday` and `is_current = false`.
- [ ] AC-2: Given `slv_freshsip.customers`, when queried for any `retailer_id`, then exactly one row has `is_current = true` (no retailer has two active rows simultaneously).
- [ ] AC-3: Given the Silver pipeline run, when it completes, then the null rate for `retailer_id`, `retailer_name`, `region`, `retail_segment`, and `account_activation_date` is less than 1%.
- [ ] AC-4: Given a new retailer account appearing for the first time, when the Silver pipeline runs, then a new SCD record is created with `effective_start_date = account_activation_date` and `is_current = true`.

**Technical Notes:**
- Source table: `brz_freshsip.erp_customers_raw`
- Target table: `slv_freshsip.customers` (Delta, SCD Type 2, not partitioned — full table is relatively small; Z-order on `retailer_id`)
- Use Delta `MERGE INTO` with `WHEN MATCHED AND <hash changed> THEN UPDATE (expire old)` + `WHEN NOT MATCHED THEN INSERT (new row)` pattern
- Change detection: compare hash of `(retailer_name, retail_segment, region, credit_terms)` between incoming and current row

**Dependencies:** CPG-020

---

#### CPG-022: Gold Layer — Top 20 Retailers by Revenue (`gld_freshsip.customers_top_retailers`)

**Type:** Story
**Epic:** CPG-E05 — Customers Data Pipeline
**Sprint:** Sprint 4
**Story Points:** 3
**Priority:** High
**Labels:** `customers`, `gold`, `kpi`, `mvp`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes the Top 20 Retailers by Revenue table, refreshed weekly, with rank, net revenue, and percentage-of-total columns plus rank-change vs. prior week. This is a key board presentation KPI — the CEO uses it to answer "who are my most important accounts?" The pipeline must produce exactly 20 rows per week and compute `rank_change` relative to the previous week's ranking.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run, when it completes, then `gld_freshsip.customers_top_retailers` contains exactly 20 rows for the most recent complete week, ranked 1 through 20 by `net_revenue` descending.
- [ ] AC-2: Given any retailer row, when `retailer_net_revenue` is manually verified, then it equals `SUM(unit_price * quantity_sold) - SUM(return_amount)` for that `retailer_id` in the most recent complete week in Silver tables within a tolerance of $0.01.
- [ ] AC-3: Given the Gold table, when the sum of `pct_of_total_revenue` across all 20 rows is computed, then it is between 0% and 100% exclusive (never summing to more than 100% due to rounding errors).
- [ ] AC-4: Given a retailer appearing in the top 20 for the current week and the prior week, when both weekly rows are queried, then `rank_change = prior_rank - current_rank` (positive = improved rank, negative = declined rank).

**Technical Notes:**
- Source tables: `slv_freshsip.sales_transactions`, `slv_freshsip.sales_returns`, `slv_freshsip.customers`
- Target table: `gld_freshsip.customers_top_retailers` (partitioned by `week_start_date`, top 20 rows per week)
- `pct_of_total_revenue`: use `SUM() OVER ()` window function for denominator (all retailers' revenue, not just top 20)
- `rank_change`: lag the prior week's ranking using `LAG(revenue_rank) OVER (PARTITION BY retailer_id ORDER BY week_start_date)`

**Dependencies:** CPG-005, CPG-021

---

#### CPG-023: Gold Layer — CAC, Retention Rate, Revenue Concentration Risk

**Type:** Story
**Epic:** CPG-E05 — Customers Data Pipeline
**Sprint:** Sprint 4
**Story Points:** 5
**Priority:** Medium
**Labels:** `customers`, `gold`, `kpi`
**Assignee:** AI Agent — Data Engineer
**Reporter:** CEO

**Description:**
> Build the Gold layer pipeline that computes three Customer health KPIs: Customer Acquisition Cost (CAC) by retail segment monthly, Retailer Retention Rate by region monthly, and Revenue Concentration Risk (top-5 retailer % of total revenue) monthly. These KPIs give the CEO a portfolio health view and are required for the board presentation narrative on account diversification strategy.

**Acceptance Criteria:**
- [ ] AC-1: Given the Gold pipeline run on the first business day of a month, when it completes, then `gld_freshsip.customers_cac` contains one row per `(month, retail_segment)` covering the prior calendar month with a non-null `cac_usd` column.
- [ ] AC-2: Given any CAC row, when `cac_usd` is manually verified, then it equals `SUM(trade_spend_usd + broker_commission_usd + field_sales_cost_usd) / COUNT(DISTINCT new_account_id)` for that `(month, retail_segment)` within a tolerance of $1.00.
- [ ] AC-3: Given `gld_freshsip.customers_retention`, when any month's retention rate is manually verified, then it equals the percentage of `retailer_id` values active in the prior month that also had at least one transaction in the current month, within a tolerance of 0.1 percentage points.
- [ ] AC-4: Given `gld_freshsip.customers_concentration_risk`, when queried for the most recent month, then `top5_concentration_pct` equals the percentage of total revenue attributable to the top 5 retailers in `gld_freshsip.customers_top_retailers` for that month within a tolerance of 0.1 percentage points.

**Technical Notes:**
- Source tables: `slv_freshsip.customers`, `slv_freshsip.sales_transactions`, `slv_freshsip.sales_returns`, `gld_freshsip.customers_top_retailers`
- Target tables: `gld_freshsip.customers_cac`, `gld_freshsip.customers_retention`, `gld_freshsip.customers_concentration_risk`
- `sales_spend` table (`trade_spend_usd`, `broker_commission_usd`, `field_sales_cost_usd`) must be included in synthetic data generation (CPG-002)
- Monthly refresh: run on the 1st of each month via a dedicated monthly job (or parametrize the daily batch job)

**Dependencies:** CPG-005, CPG-021

---

#### CPG-024: Sales + Inventory Dashboard Pages (Databricks AI/BI)

**Type:** Story
**Epic:** CPG-E07 — Dashboards & Visualization
**Sprint:** Sprint 4
**Story Points:** 8
**Priority:** Highest
**Labels:** `sales`, `inventory`, `dashboard`, `mvp`, `blocking`
**Assignee:** AI Agent — Data Engineer (Deployer)
**Reporter:** CEO

**Description:**
> Build the Sales and Inventory pages of the Databricks AI/BI executive dashboard using Gold layer tables. The Sales page must display: daily revenue trend (line chart), MoM and YoY delta cards, and gross margin by SKU ranked table with alert flags. The Inventory page must display: stock level heat map (warehouse x SKU), DSI table with color coding, reorder alert badge, and inventory turnover chart. All charts must render within 5 seconds on a pre-warmed cluster.

**Acceptance Criteria:**
- [ ] AC-1: Given a pre-warmed Databricks cluster, when the dashboard Sales page is loaded, then all charts and widgets render within 5 seconds of page open with no loading spinners or blank widgets.
- [ ] AC-2: Given the Sales page, when it is viewed, then it displays: daily revenue trend (line chart, 30-day rolling), MoM revenue comparison delta card (green/red arrow), YoY revenue comparison delta card, and gross margin by SKU ranked table with `margin_alert_flag` visual indicator.
- [ ] AC-3: Given the Inventory page, when it is viewed, then it displays: current stock level heat map (warehouse x SKU grid, color = stock health), DSI table with red/yellow/green color coding, active reorder alert count badge, and inventory turnover bar chart by warehouse.
- [ ] AC-4: Given any KPI widget on the Sales or Inventory pages, when the underlying data is a known test value (from synthetic data), then the widget displays that value correctly with no rounding errors exceeding $1 or 0.1 percentage points.

**Technical Notes:**
- Use Databricks AI Dev Kit `databricks-aibi-dashboards` skill and MCP tool `mcp__databricks__create_or_update_dashboard`
- Dashboard definition stored as code in `src/dashboard/freshsip_executive_dashboard.json`
- Source queries: parameterized SQL against Gold tables only — never query Silver or Bronze from dashboard
- Optimize Gold table queries for < 1s execution: ensure correct partitioning and Z-ordering from CPG-006, CPG-007, CPG-010, CPG-011

**Dependencies:** CPG-006, CPG-007, CPG-010, CPG-011

---

#### CPG-025: Production + Distribution Dashboard Pages (Databricks AI/BI)

**Type:** Story
**Epic:** CPG-E07 — Dashboards & Visualization
**Sprint:** Sprint 4
**Story Points:** 5
**Priority:** High
**Labels:** `production`, `distribution`, `dashboard`
**Assignee:** AI Agent — Data Engineer (Deployer)
**Reporter:** CEO

**Description:**
> Build the Production and Distribution pages of the Databricks AI/BI executive dashboard. The Production page must display: batch yield rate KPI card and trend line, QC pass rate KPI card and production line heat map, and downtime hours bar chart. The Distribution page must display: OTD% KPI card and regional map, order fulfillment rate KPI card and channel chart, cost per case by region, and worst-performing routes ranked table. Batches below 92% yield must be highlighted with a visual alert.

**Acceptance Criteria:**
- [ ] AC-1: Given a pre-warmed cluster, when the Production dashboard page is loaded, then it renders within 5 seconds and displays: batch yield rate (KPI card + 30-day trend line), quality check pass rate (KPI card + production line heat map by week), and downtime hours (bar chart by production line, last 7 days).
- [ ] AC-2: Given the Distribution page, when it is viewed, then it displays: OTD% (KPI card + US regional map colored by OTD%), order fulfillment rate (KPI card + channel bar chart), cost per case (bar chart by region + target line at $4.50), and worst-performing routes (ranked table with route_id, region, OTD%, cost per case, weeks on worst list).
- [ ] AC-3: Given a batch yield rate below 92% in the underlying Gold data, when the Production page is viewed, then the affected batch row in the trend chart or table is highlighted with a red visual alert indicator.

**Technical Notes:**
- Extend the same dashboard created in CPG-024 with two additional pages (Production, Distribution)
- Use `mcp__databricks__create_or_update_dashboard` to update the existing dashboard object
- Regional map widget: use Databricks AI/BI native map visualization with US state-level granularity
- Worst routes table: sourced from `gld_freshsip.distribution_route_performance` (top 10 worst, weekly)

**Dependencies:** CPG-014, CPG-015, CPG-018, CPG-019

---

#### CPG-026: Customers Dashboard Page (Databricks AI/BI)

**Type:** Story
**Epic:** CPG-E07 — Dashboards & Visualization
**Sprint:** Sprint 4
**Story Points:** 3
**Priority:** High
**Labels:** `customers`, `dashboard`, `mvp`
**Assignee:** AI Agent — Data Engineer (Deployer)
**Reporter:** CEO

**Description:**
> Build the Customers page of the Databricks AI/BI executive dashboard showing top 20 retailers ranked table with rank-change indicators, revenue concentration donut chart (top 5 vs. rest vs. long tail), and monthly retention rate trend line. The CEO uses this page during board presentations to answer account concentration questions. Retailers with > 15% of total revenue must be highlighted with a concentration risk indicator.

**Acceptance Criteria:**
- [ ] AC-1: Given a pre-warmed cluster, when the Customers dashboard page is loaded, then it renders within 5 seconds and displays the top 20 retailers ranked table with rank-change up/down arrow indicators vs. prior week.
- [ ] AC-2: Given the Customers page, when it is viewed, then it also displays: a revenue concentration donut chart (top 5 retailers vs. retailers 6-20 vs. long tail), and a monthly retention rate trend line (12-month rolling).
- [ ] AC-3: Given a single retailer with > 15% of total revenue in the underlying Gold data, when the Customers page is viewed, then that retailer's row in the top 20 table is highlighted with a red concentration risk indicator badge.

**Technical Notes:**
- Extend the dashboard from CPG-024/CPG-025 with a fifth Customers page
- Source queries: `gld_freshsip.customers_top_retailers`, `gld_freshsip.customers_concentration_risk`, `gld_freshsip.customers_retention`
- Donut chart: top-5 vs. retailers 6-20 vs. all others — requires three bucket aggregation on `revenue_rank`
- Retention trend: 12-month rolling from `gld_freshsip.customers_retention` partitioned by `month`

**Dependencies:** CPG-022, CPG-023

---

#### CPG-030: Genie AI/BI Natural Language Query Space (STRETCH — Could Have)

**Type:** Story
**Epic:** CPG-E07 — Dashboards & Visualization
**Sprint:** Sprint 4
**Story Points:** 8
**Priority:** Low
**Labels:** `dashboard`, `genie`, `stretch`, `nlq`
**Assignee:** AI Agent — Data Engineer (Deployer)
**Reporter:** CEO

**Description:**
> Configure a Databricks Genie AI/BI natural language query space connected to all Gold layer tables, enabling the CEO to ask business questions in plain English without SQL knowledge. This is a stretch goal (Could Have) for Sprint 4 — it may slip if sprint capacity is consumed by critical path items or bug fixes. The Genie space should be linked from the main dashboard page as an "Ask a Question" entry point.

> **STRETCH GOAL NOTE:** Sprint 4 total is estimated at ~40 story points (6 points over the ~34-point target). If capacity runs short, this story is the first candidate to defer to a future sprint. All Must Have and Should Have stories in Sprint 4 take priority over CPG-030.

**Acceptance Criteria:**
- [ ] AC-1: Given the Genie space is configured and connected to Gold tables, when the CEO types "What was total revenue last week?", then Genie returns a correct numeric answer matching the value in `gld_freshsip.sales_daily_revenue` for the prior calendar week within a tolerance of $1.00.
- [ ] AC-2: Given the Genie space, when the CEO asks "Which warehouse has the lowest stock level right now?", then Genie returns the correct `warehouse_id` and `units_on_hand` matching the most recent row in `gld_freshsip.inventory_stock_levels`.
- [ ] AC-3: Given the Genie space, when the CEO asks "Who are my top 5 retailers by revenue this month?", then Genie returns the correct top 5 retailer names matching `gld_freshsip.customers_top_retailers` for the current month-to-date.
- [ ] AC-4: Given the Genie space, when any query is answered, then the response includes the SQL query used (for transparency) and the last-updated timestamp of the underlying Gold table queried.

**Technical Notes:**
- Use Databricks AI Dev Kit `databricks-genie` skill and MCP tool `mcp__databricks__create_or_update_genie`
- Connect Genie space to all 12 Gold tables: `gld_freshsip.*`
- Add table descriptions and column descriptions to each Gold table to improve Genie accuracy
- Genie space link should be embedded in the main dashboard page header as "Ask FreshSip AI"

**Dependencies:** CPG-006, CPG-011, CPG-022, CPG-024, CPG-025, CPG-026

---

## Section 5: Dependency Map

### Critical Path (Primary — Revenue and Board Demo)

```
CPG-001 (Infra Setup)
  └─► CPG-002 (Synthetic Data)
        ├─► CPG-003 (Bronze: POS JSON)
        │     └─► CPG-005 (Silver: Sales) ──────────────────────────────────┐
        └─► CPG-004 (Bronze: ERP Sales CSV)                                  │
              └─► CPG-005 (Silver: Sales)                                    │
                    ├─► CPG-006 (Gold: Daily Revenue) ──────────────────────►│
                    └─► CPG-007 (Gold: Gross Margin) ──────────────────────►│
                                                                              │
CPG-001 (Infra)                                                               │
  └─► CPG-002 (Synthetic Data)                                               │
        └─► CPG-008 (Bronze: Inventory CSV)                                  │
              └─► CPG-009 (Silver: Inventory)                                │
                    ├─► CPG-010 (Gold: Inv Turnover) ──────────────────────►│
                    └─► CPG-011 (Gold: DSI + Alerts) ──────────────────────►│
                                                                              ▼
                                                              CPG-024 (Sales + Inv Dashboard)
                                                                              └─► CPG-026 (CEO Demo Ready)
```

### Micro-Batch Path (Production IoT)

```
CPG-001 (Infra)
  └─► CPG-002 (Synthetic Data)
        └─► CPG-012 (Bronze: IoT Micro-Batch, 5 min)
              └─► CPG-013 (Silver: Production Batches)
                    ├─► CPG-014 (Gold: Yield + QC)
                    └─► CPG-015 (Gold: Downtime + Traceability)*
                          └─► CPG-025 (Production Dashboard)

* CPG-015 also depends on CPG-017 (Silver: Shipments) for traceability cross-domain join
```

### Distribution Path

```
CPG-001 (Infra)
  └─► CPG-002 (Synthetic Data)
        └─► CPG-016 (Bronze: Logistics CSV)
              └─► CPG-017 (Silver: Shipments)
                    ├─► CPG-018 (Gold: OTD + Fulfillment)
                    └─► CPG-019 (Gold: Cost/Case + Routes)
                          └─► CPG-025 (Distribution Dashboard)
```

### Customer Domain Path

```
CPG-001 (Infra)
  └─► CPG-002 (Synthetic Data)
        └─► CPG-020 (Bronze: Customers CSV) [Sprint 2]
              └─► CPG-021 (Silver: SCD Type 2) [Sprint 4]
                    ├─► CPG-022 (Gold: Top 20 Retailers)
                    └─► CPG-023 (Gold: CAC, Retention, Concentration)
                          └─► CPG-026 (Customers Dashboard)
```

### Orchestration and Infrastructure Path

```
CPG-001 (Infra)
  ├─► CPG-027 (Workflows Orchestration) [Sprint 1 — wired to pipelines as they complete]
  ├─► CPG-028 (CI/CD GitHub Actions + DABs) [Sprint 1]
  └─► CPG-029 (DQ Framework) [Sprint 1 — consumed by all Silver pipelines]
        └─► CPG-005 / CPG-009 / CPG-013 / CPG-017 / CPG-021 (all Silver pipelines)
```

---

## Section 6: Velocity and Capacity Planning

### Sprint Velocity Target

| Sprint | Target Points | Actual Assignment | Buffer / Notes |
|---|---|---|---|
| Sprint 1 | 34 pts | 34 pts | Fits within target |
| Sprint 2 | 36 pts | 37 pts | 1 point over — acceptable within 5% buffer |
| Sprint 3 | 36 pts | 36 pts | On target |
| Sprint 4 | 34 pts | 40 pts | **6 pts over — CPG-030 (8 pts, stretch) is the flex item** |
| **Total** | **140 pts** | **147 pts** | CPG-030 accounts for all overage; remove it to hit 139 pts |

### Capacity Model

- **Team:** 1 AI Agent (Data Engineer) with Architect and Deployer support for specific stories
- **Buffer policy:** 20% of sprint capacity reserved for bug fixes, rework, and unexpected complexity
- **Effective capacity:** ~34 pts per sprint (42 raw pts × 80% = ~34 pts committed)
- **Sprint 4 Risk:** Flagged — if CPG-021 (8 pts, SCD Type 2) has unexpected complexity carryover, CPG-030 (Genie, 8 pts) should be deferred. CPG-024 through CPG-026 (dashboard stories, 16 pts total) are non-negotiable for the board demo.

### Sprint 3 Density Risk (Most Likely Slip Items)

Sprint 3 carries all 8 Gold table stories across 4 domains in a single week. The two stories most likely to slip or require extra time:

1. **CPG-011 (DSI + Reorder Alert, 5 pts)** — Requires a cross-domain join of Silver inventory and Silver sales with a 30-day rolling window aggregation. If Silver data quality issues surface during this sprint, this computation is sensitive to missing days in the `snapshot_date` series.

2. **CPG-015 (Downtime + Traceability, 3 pts)** — Depends on both CPG-013 (Production Silver) AND CPG-017 (Distribution Silver) being complete and clean. If either Silver table has DQ issues, the traceability cross-domain join will fail. Mitigation: treat CPG-015 as Could Have and deprioritize it if Sprint 3 is under pressure — the board demo can proceed without batch traceability.

### Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Community Edition rate limits (API, cluster restarts) slow pipeline development | High | Medium | Use trigger-once streaming patterns; pre-warm cluster before demo |
| SCD Type 2 (CPG-021) merge logic bugs discovered in Sprint 4 | Medium | High | Add integration test `tests/integration/test_customers_scd.py` in Sprint 4 |
| Sprint 3 Gold pipeline cross-domain joins produce null-heavy output | Medium | High | Run DQ check after each Gold pipeline; escalate to backlog if null rate > 5% |
| CPG-030 (Genie) setup exceeds time budget in Sprint 4 | High | Low | Explicitly tagged as stretch; deferred to next sprint with no impact on board demo |
| Dashboard render time exceeds 5s SLA on cold cluster | Medium | High | Pre-warm cluster for demo; Z-order all Gold tables on dashboard query dimensions |

---

## Section 7: Definition of Ready and Definition of Done

### Definition of Ready

A story is ready to enter a sprint when ALL of the following are true:

- [ ] User story is written with a clear "As a ... I want ... So that ..." format
- [ ] At least 2 (ideally 3+) acceptance criteria are defined in Given/When/Then format
- [ ] All upstream dependencies are completed or confirmed to be in-progress in the same sprint
- [ ] Source tables and schemas are defined (Bronze table name, Silver DDL, or Gold grain documented)
- [ ] Story is estimated in Fibonacci points by the team (1, 2, 3, 5, 8, or 13)
- [ ] Technical Notes section identifies the key implementation approach and target table names
- [ ] Priority is set (Highest/High/Medium/Low) and aligns with MoSCoW classification in the PRD

### Definition of Done

A story is done (can be marked **Closed** in Jira) when ALL of the following are true:

- [ ] All acceptance criteria are verifiable and passing (manual or automated verification)
- [ ] Code committed to GitHub on a feature branch with a pull request open against `main`
- [ ] PR reviewed and approved by the Code Reviewer agent (no P0 or P1 issues outstanding)
- [ ] Unit tests written in `tests/unit/` and passing with no failures in CI
- [ ] For pipeline stories: data quality checks passing (DQ log shows `run_status = 'OK'` or `'WARN'` — not `'FAIL'`)
- [ ] For pipeline stories: target table populated with data (SELECT COUNT(*) > 0 confirmed)
- [ ] For dashboard stories: dashboard page renders within 5 seconds on a pre-warmed cluster
- [ ] Commit message follows the project convention: `[LAYER] type: description` (e.g., `[GOLD] feat: add daily revenue KPI table`)
- [ ] No hardcoded secrets, credentials, or workspace-specific paths in committed code
- [ ] Story linked to its Epic in Jira; sprint field is set; story points are confirmed

---

## Section 8: Labels Reference

| Label | Use Cases |
|---|---|
| `bronze` | Bronze layer ingestion pipeline stories (CPG-003, CPG-004, CPG-008, CPG-012, CPG-016, CPG-020) |
| `silver` | Silver layer transformation and validation pipeline stories (CPG-005, CPG-009, CPG-013, CPG-017, CPG-021) |
| `gold` | Gold layer aggregation and KPI pipeline stories (CPG-006, CPG-007, CPG-010, CPG-011, CPG-014, CPG-015, CPG-018, CPG-019, CPG-022, CPG-023) |
| `dashboard` | Dashboard and visualization stories (CPG-024, CPG-025, CPG-026, CPG-030) |
| `infra` | Infrastructure, workspace setup, DevOps, CI/CD stories (CPG-001, CPG-027, CPG-028) |
| `dq` | Data quality framework stories (CPG-029) |
| `synthetic-data` | Synthetic data generation (CPG-002) |
| `orchestration` | Workflow and job scheduling stories (CPG-027) |
| `cicd` | CI/CD pipeline stories (CPG-028) |
| `scd2` | SCD Type 2 dimension stories (CPG-021) |
| `streaming` | Streaming / micro-batch ingestion stories (CPG-012) |
| `nlq` | Natural language query / Genie stories (CPG-030) |
| `sales` | Stories that produce or consume Sales domain data |
| `inventory` | Stories that produce or consume Inventory domain data |
| `production` | Stories that produce or consume Production domain data |
| `distribution` | Stories that produce or consume Distribution domain data |
| `customers` | Stories that produce or consume Customers domain data |
| `kpi` | Stories that produce Gold KPI tables |
| `pipeline` | All pipeline implementation stories (Bronze, Silver, Gold) |
| `mvp` | Must-have for board demo — these stories cannot slip |
| `stretch` | Could Have — drops if sprint capacity runs out (CPG-030) |
| `blocking` | Story is on the critical path; other stories depend on it completing on time |

---

## Appendix A: Story Point Summary by Sprint

| Story | Title | Epic | Sprint | Points |
|---|---|---|---|---|
| CPG-001 | Databricks Workspace Setup | E06 — Infra | S1 | 8 |
| CPG-002 | Synthetic Data Generation | E06 — Infra | S1 | 5 |
| CPG-003 | Bronze: Sales POS JSON | E01 — Sales | S1 | 5 |
| CPG-004 | Bronze: Sales ERP CSV | E01 — Sales | S1 | 3 |
| CPG-008 | Bronze: Inventory ERP CSV | E02 — Inventory | S1 | 3 |
| CPG-027 | Workflows Orchestration | E09 — Deploy | S1 | 5 |
| CPG-028 | CI/CD GitHub Actions + DABs | E09 — Deploy | S1 | 5 |
| CPG-029 | DQ Framework Library | E08 — QA | S1 | 3 |
| **Sprint 1 Total** | | | | **37** |
| CPG-005 | Silver: Sales Transactions | E01 — Sales | S2 | 8 |
| CPG-009 | Silver: Inventory Stock + Reorder | E02 — Inventory | S2 | 5 |
| CPG-012 | Bronze: IoT Sensors Micro-Batch | E03 — Production | S2 | 8 |
| CPG-013 | Silver: Production Batches + QC | E03 — Production | S2 | 5 |
| CPG-016 | Bronze: Logistics CSV | E04 — Distribution | S2 | 3 |
| CPG-017 | Silver: Shipments + Routes | E04 — Distribution | S2 | 5 |
| CPG-020 | Bronze: Customers CSV | E05 — Customers | S2 | 3 |
| **Sprint 2 Total** | | | | **37** |
| CPG-006 | Gold: Daily Revenue | E01 — Sales | S3 | 5 |
| CPG-007 | Gold: Gross Margin by SKU | E01 — Sales | S3 | 5 |
| CPG-010 | Gold: Inventory Turnover | E02 — Inventory | S3 | 5 |
| CPG-011 | Gold: DSI + Reorder Alerts | E02 — Inventory | S3 | 5 |
| CPG-014 | Gold: Batch Yield + QC Rate | E03 — Production | S3 | 5 |
| CPG-015 | Gold: Downtime + Traceability | E03 — Production | S3 | 3 |
| CPG-018 | Gold: OTD + Fulfillment Rate | E04 — Distribution | S3 | 5 |
| CPG-019 | Gold: Cost/Case + Routes | E04 — Distribution | S3 | 3 |
| **Sprint 3 Total** | | | | **36** |
| CPG-021 | Silver: Customers SCD Type 2 | E05 — Customers | S4 | 8 |
| CPG-022 | Gold: Top 20 Retailers | E05 — Customers | S4 | 3 |
| CPG-023 | Gold: CAC + Retention + Risk | E05 — Customers | S4 | 5 |
| CPG-024 | Dashboard: Sales + Inventory | E07 — Dashboard | S4 | 8 |
| CPG-025 | Dashboard: Production + Distribution | E07 — Dashboard | S4 | 5 |
| CPG-026 | Dashboard: Customers | E07 — Dashboard | S4 | 3 |
| CPG-030 | Genie NLQ Space *(STRETCH)* | E07 — Dashboard | S4 | 8 |
| **Sprint 4 Total (incl. stretch)** | | | | **40** |
| **Sprint 4 Total (excl. stretch)** | | | | **32** |
| **Project Total (incl. stretch)** | | | | **150** |
| **Project Total (excl. stretch)** | | | | **142** |

---

## Appendix B: KPI to Story Traceability

| KPI ID | KPI Name | Domain | Gold Table Story | Dashboard Story |
|---|---|---|---|---|
| KPI-S01 | Daily Revenue | Sales | CPG-006 | CPG-024 |
| KPI-S02 | Revenue MoM % | Sales | CPG-006 (same pipeline) | CPG-024 |
| KPI-S03 | Revenue YoY % | Sales | CPG-006 (same pipeline) | CPG-024 |
| KPI-S04 | Gross Margin by SKU | Sales | CPG-007 | CPG-024 |
| KPI-I01 | Current Stock Level | Inventory | CPG-011 | CPG-024 |
| KPI-I02 | Inventory Turnover Rate | Inventory | CPG-010 | CPG-024 |
| KPI-I03 | Days Sales of Inventory (DSI) | Inventory | CPG-011 | CPG-024 |
| KPI-I04 | Reorder Alert Flag | Inventory | CPG-011 | CPG-024 |
| KPI-P01 | Batch Yield Rate | Production | CPG-014 | CPG-025 |
| KPI-P02 | Quality Check Pass Rate | Production | CPG-014 | CPG-025 |
| KPI-P03 | Downtime Hours | Production | CPG-015 | CPG-025 |
| KPI-P04 | Batch Traceability Index | Production | CPG-015 | CPG-025 |
| KPI-D01 | On-Time Delivery % | Distribution | CPG-018 | CPG-025 |
| KPI-D02 | Order Fulfillment Rate | Distribution | CPG-018 | CPG-025 |
| KPI-D03 | Cost Per Case Delivered | Distribution | CPG-019 | CPG-025 |
| KPI-D04 | Worst-Performing Routes | Distribution | CPG-019 | CPG-025 |
| KPI-C01 | Top 20 Retailers by Revenue | Customers | CPG-022 | CPG-026 |
| KPI-C02 | Customer Acquisition Cost | Customers | CPG-023 | CPG-026 |
| KPI-C03 | Retailer Retention Rate | Customers | CPG-023 | CPG-026 |
| KPI-C04 | Revenue Concentration Risk | Customers | CPG-023 | CPG-026 |
