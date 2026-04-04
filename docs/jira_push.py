"""
FreshSip Beverages — Jira Project Push Script
Creates 4 sprints, 9 epics, and 30 stories in the SCRUM project.

Credentials are loaded from docs/.env (never committed — listed in .gitignore).
"""

import os
import sys
import requests
import time
from pathlib import Path
from requests.auth import HTTPBasicAuth

# ── Load .env (no external deps) ─────────────────────────────────────────────
def _load_env(env_path: Path) -> dict:
    if not env_path.exists():
        sys.exit(f"ERROR: .env file not found at {env_path}")
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env

_env = _load_env(Path(__file__).parent / ".env")

def _require(key: str) -> str:
    val = _env.get(key) or os.environ.get(key, "")
    if not val:
        sys.exit(f"ERROR: {key} is missing from .env")
    return val

# ── Config ────────────────────────────────────────────────────────────────────
JIRA_URL    = _require("JIRA_URL").rstrip("/")
EMAIL       = _require("JIRA_EMAIL")
API_TOKEN   = _require("JIRA_API_TOKEN")
PROJECT_KEY = "SCRUM"
BOARD_ID    = 1

AUTH    = HTTPBasicAuth(EMAIL, API_TOKEN)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

# Issue type IDs (from API discovery)
EPIC_TYPE_ID = "10001"
TASK_TYPE_ID = "10003"

# Custom fields
FIELD_STORY_POINTS = "customfield_10016"
FIELD_SPRINT       = "customfield_10020"

# ── Helpers ───────────────────────────────────────────────────────────────────
def api(method, path, **kwargs):
    url = f"{JIRA_URL}{path}"
    resp = getattr(requests, method)(url, auth=AUTH, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json() if resp.text else {}

def create_sprint(name, start, end):
    print(f"  Creating sprint: {name}")
    data = {
        "name": name,
        "startDate": f"{start}T09:00:00.000Z",
        "endDate":   f"{end}T18:00:00.000Z",
        "originBoardId": BOARD_ID,
    }
    result = api("post", "/rest/agile/1.0/sprint", json=data)
    return result["id"]

def create_epic(summary, description, labels):
    print(f"  Creating epic: {summary}")
    data = {
        "fields": {
            "project":     {"key": PROJECT_KEY},
            "issuetype":   {"id": EPIC_TYPE_ID},
            "summary":     summary,
            "description": {
                "type": "doc", "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}]
            },
            "labels": labels,
        }
    }
    result = api("post", "/rest/api/3/issue", json=data)
    return result["id"], result["key"]

def create_story(summary, description, acceptance_criteria, story_points,
                 sprint_id, epic_id, labels, priority="Medium"):
    print(f"    Creating story: {summary[:60]}...")
    ac_text = "\n".join(acceptance_criteria)
    full_desc = f"{description}\n\nAcceptance Criteria:\n{ac_text}"
    data = {
        "fields": {
            "project":   {"key": PROJECT_KEY},
            "issuetype": {"id": TASK_TYPE_ID},
            "summary":   summary,
            "description": {
                "type": "doc", "version": 1,
                "content": [{"type": "paragraph", "content": [{"type": "text", "text": full_desc}]}]
            },
            "labels": labels,
            "priority": {"name": priority},
            FIELD_STORY_POINTS: story_points,
            FIELD_SPRINT: sprint_id,
            "parent": {"id": epic_id},
        }
    }
    result = api("post", "/rest/api/3/issue", json=data)
    return result["key"]


# ── Data ──────────────────────────────────────────────────────────────────────

SPRINTS = [
    ("S1: Infra & Bronze",   "2026-04-07", "2026-04-11"),
    ("S2: Silver & DQ",      "2026-04-14", "2026-04-18"),
    ("S3: Gold & KPIs",      "2026-04-21", "2026-04-25"),
    ("S4: Dash & Deploy",    "2026-04-28", "2026-05-01"),
]

EPICS = [
    ("E01", "CPG-E01: Sales Data Pipeline",           "End-to-end pipeline for Sales domain: POS + ERP ingestion through Bronze, Silver cleaning, and Gold KPI tables (Daily Revenue, Gross Margin).",           ["sales", "pipeline"]),
    ("E02", "CPG-E02: Inventory Data Pipeline",        "End-to-end pipeline for Inventory domain: ERP stock data through Bronze, Silver stock levels, and Gold KPI tables (Turnover Rate, DSI, Reorder Alerts).", ["inventory", "pipeline"]),
    ("E03", "CPG-E03: Production Data Pipeline",       "End-to-end pipeline for Production domain: IoT sensor micro-batch through Bronze, Silver batch records, and Gold KPI tables (Yield Rate, QC Pass Rate).",  ["production", "pipeline"]),
    ("E04", "CPG-E04: Distribution Data Pipeline",     "End-to-end pipeline for Distribution domain: logistics CSV through Bronze, Silver shipments, and Gold KPI tables (OTD%, Fulfillment Rate, Cost Per Case).", ["distribution", "pipeline"]),
    ("E05", "CPG-E05: Customers Data Pipeline",        "End-to-end pipeline for Customers domain: retailer ERP data through Bronze, Silver SCD2 profiles, and Gold KPI tables (Top 20, CAC, Retention Rate).",   ["customers", "pipeline"]),
    ("E06", "CPG-E06: Infrastructure & DevOps",        "Databricks workspace setup, Hive metastore schemas, cluster policies, CI/CD with GitHub Actions + DABs, Databricks Workflows orchestration.",            ["infra", "devops"]),
    ("E07", "CPG-E07: Dashboards & Visualization",     "Databricks AI/BI dashboards for all 5 domains plus Genie AI natural language query space (stretch goal).",                                                 ["dashboard", "visualization"]),
    ("E08", "CPG-E08: Testing & Quality Assurance",    "Reusable data quality framework, unit tests for transformation logic, integration tests against Delta tables.",                                            ["dq", "testing"]),
    ("E09", "CPG-E09: Deployment & Documentation",     "Board demo environment prep, deployment validation, architecture docs, runbooks, and CEO dashboard walkthrough guide.",                                    ["deployment", "docs"]),
]

# Stories: (cpg_id, summary, description, acceptance_criteria, story_points, sprint_index(0-3), epic_key, labels, priority)
STORIES = [
    # ── SPRINT 1 ──────────────────────────────────────────────────────────────
    (
        "CPG-001",
        "CPG-001: Databricks Workspace Setup & Hive Metastore Schema Creation",
        "Set up the Databricks workspace with the three Hive metastore databases (brz_freshsip, slv_freshsip, gld_freshsip) as the Community Edition fallback for Unity Catalog. Configure cluster policy and verify connectivity for all pipeline agents.",
        [
            "AC-1: Given a Databricks workspace, when schemas are created, then brz_freshsip, slv_freshsip, and gld_freshsip databases exist and are queryable.",
            "AC-2: Given the cluster policy, when a job runs, then it uses the configured cluster size and auto-termination settings.",
            "AC-3: Given the workspace, when a test notebook runs, then it completes successfully with no permission errors.",
        ],
        8, 0, "E06", ["infra", "databricks", "mvp", "blocking"], "Highest"
    ),
    (
        "CPG-002",
        "CPG-002: Synthetic Data Generation (13 Months, All 5 Domains)",
        "Generate realistic synthetic data covering 13 months of history for all 5 domains (Sales, Inventory, Production, Distribution, Customers). Data must embed realistic narratives: a West Coast warehouse approaching stockout, a production batch yield decline, an East region outperforming. Seed all Bronze tables.",
        [
            "AC-1: Given the synthetic data generator, when run, then it produces 13 months of data for all 5 domain Bronze tables.",
            "AC-2: Given Sales data, when queried, then daily revenue follows realistic seasonal patterns (summer peaks for beverages).",
            "AC-3: Given Inventory data, when queried, then at least one warehouse SKU has stock below reorder point to trigger an alert.",
            "AC-4: Given Production data, when queried, then at least one batch shows a yield rate decline trend over 3 consecutive batches.",
        ],
        5, 0, "E06", ["infra", "synthetic-data", "mvp", "blocking"], "Highest"
    ),
    (
        "CPG-003",
        "CPG-003: Bronze Ingestion — Sales POS JSON (Hourly Micro-Batch)",
        "Implement the Bronze ingestion pipeline for Sales POS data arriving as hourly JSON files. Use notebook-based ingestion (Community Edition fallback for Auto Loader). Write append-only to brz_freshsip.pos_transactions_raw with schema-on-read. Log rejected rows.",
        [
            "AC-1: Given hourly POS JSON files, when the pipeline runs, then records are appended to brz_freshsip.pos_transactions_raw.",
            "AC-2: Given malformed JSON records, when ingested, then they are logged to a rejection table and pipeline does not fail.",
            "AC-3: Given the Bronze table, when queried, then it contains columns: transaction_id, retailer_id, product_id, quantity_sold, unit_price, transaction_timestamp, region, channel.",
        ],
        5, 0, "E01", ["sales", "bronze", "pipeline", "mvp", "blocking"], "Highest"
    ),
    (
        "CPG-004",
        "CPG-004: Bronze Ingestion — Sales ERP CSV (Daily Batch)",
        "Implement the Bronze ingestion pipeline for Sales ERP data arriving as daily CSV exports from SAP. Write append-only to brz_freshsip.erp_sales_raw. Include schema validation and row-level rejection logging.",
        [
            "AC-1: Given daily ERP CSV files, when the pipeline runs, then records are appended to brz_freshsip.erp_sales_raw.",
            "AC-2: Given a CSV with missing required columns, when ingested, then the pipeline logs the schema error and halts gracefully.",
            "AC-3: Given the Bronze table, when queried, then it contains columns: order_id, order_date, product_id, quantity, invoice_price, return_flag, region, channel.",
        ],
        3, 0, "E01", ["sales", "bronze", "pipeline", "mvp"], "High"
    ),
    (
        "CPG-008",
        "CPG-008: Bronze Ingestion — Inventory ERP CSV (Daily Batch)",
        "Implement Bronze ingestion for Inventory stock level data from daily ERP CSV exports. Write append-only to brz_freshsip.inventory_stock_raw. Include ingestion timestamp for lineage tracking.",
        [
            "AC-1: Given daily inventory CSV files, when the pipeline runs, then records are appended to brz_freshsip.inventory_stock_raw.",
            "AC-2: Given the Bronze table, when queried, then it contains columns: warehouse_id, sku_id, stock_on_hand, unit_cost, snapshot_date, ingested_at.",
            "AC-3: Given duplicate snapshot records, when ingested, then all are appended (dedup is a Silver layer responsibility).",
        ],
        3, 0, "E02", ["inventory", "bronze", "pipeline", "mvp"], "High"
    ),
    (
        "CPG-027",
        "CPG-027: Databricks Workflows Orchestration Setup",
        "Define and deploy Databricks Workflow job definitions for: (1) daily batch pipeline (Bronze→Silver→Gold, runs at 05:00 UTC), and (2) IoT micro-batch trigger (every 5 minutes for Production domain). Use Databricks Jobs API or DABs config.",
        [
            "AC-1: Given the daily batch workflow, when triggered at 05:00 UTC, then it runs Bronze→Silver→Gold jobs in sequence.",
            "AC-2: Given the micro-batch workflow, when triggered every 5 minutes, then it processes Production IoT data within 10 minutes of arrival.",
            "AC-3: Given a job failure, when it occurs, then the workflow sends an alert notification and halts downstream jobs.",
        ],
        5, 0, "E06", ["infra", "orchestration", "mvp"], "High"
    ),
    (
        "CPG-028",
        "CPG-028: CI/CD Pipeline — GitHub Actions + Databricks Asset Bundles",
        "Set up GitHub Actions workflow and Databricks Asset Bundles (DABs) configuration for automated deployment. On merge to main: validate notebooks, run unit tests, deploy pipeline definitions to Databricks workspace.",
        [
            "AC-1: Given a PR merged to main, when CI runs, then unit tests execute and must pass before deployment.",
            "AC-2: Given a successful CI run, when DABs deploy, then pipeline definitions are updated in the Databricks workspace.",
            "AC-3: Given a test failure, when CI runs, then deployment is blocked and the PR author is notified.",
        ],
        5, 0, "E06", ["infra", "cicd", "devops"], "Medium"
    ),
    (
        "CPG-029",
        "CPG-029: Data Quality Framework — Reusable DQ Check Library",
        "Build a reusable Python/PySpark data quality library (src/utils/dq_checks.py) with functions for: null rate checking, range validation, duplicate key detection, and referential integrity. All Silver pipelines must call this library.",
        [
            "AC-1: Given a DataFrame and a DQ config, when dq_checks.run() is called, then it returns a DQ report with pass/fail per check.",
            "AC-2: Given a null rate > 1% on a required column, when DQ runs, then the pipeline raises a DataQualityException.",
            "AC-3: Given duplicate primary keys, when DQ runs, then duplicates are counted and logged to a quarantine table.",
        ],
        3, 0, "E08", ["dq", "infra", "mvp", "blocking"], "High"
    ),

    # ── SPRINT 2 ──────────────────────────────────────────────────────────────
    (
        "CPG-005",
        "CPG-005: Silver Layer — Sales Transactions (Cleaning, Dedup, Validation)",
        "Build the Silver transformation pipeline for Sales data. Merge POS and ERP Bronze tables, deduplicate on transaction_id, cast types, apply business rules (net revenue = invoice_price * quantity - returns), and validate against DQ framework. Write to slv_freshsip.sales_transactions as Delta Lake.",
        [
            "AC-1: Given brz_freshsip.pos_transactions_raw and brz_freshsip.erp_sales_raw, when Silver pipeline runs, then slv_freshsip.sales_transactions is populated with zero duplicate transaction_ids.",
            "AC-2: Given the DQ framework, when Silver pipeline runs, then null rate on required columns is < 1%.",
            "AC-3: Given a return record, when processed, then net_revenue = (unit_price * quantity_sold) - return_amount.",
            "AC-4: Given Silver table, when queried, then columns include: transaction_id, product_id, sku_id, retailer_id, transaction_date, net_revenue, quantity_sold, product_category, region, channel, is_return.",
        ],
        8, 1, "E01", ["sales", "silver", "pipeline", "mvp", "blocking"], "Highest"
    ),
    (
        "CPG-009",
        "CPG-009: Silver Layer — Inventory Stock Levels & Reorder Reference Table",
        "Build Silver pipeline for Inventory: deduplicate daily snapshots, cast types, compute derived fields (days_of_supply_snapshot), and join with reorder points reference. Seed slv_freshsip.ref_reorder_points from synthetic data. Write to slv_freshsip.inventory_stock as Delta Lake.",
        [
            "AC-1: Given brz_freshsip.inventory_stock_raw, when Silver pipeline runs, then slv_freshsip.inventory_stock has one record per warehouse_id + sku_id + snapshot_date (no duplicates).",
            "AC-2: Given slv_freshsip.ref_reorder_points, when queried, then all SKUs in inventory_stock have a corresponding reorder_point value.",
            "AC-3: Given stock_on_hand < reorder_point for a SKU, when queried from Silver, then is_below_reorder = true.",
        ],
        5, 1, "E02", ["inventory", "silver", "pipeline", "mvp", "blocking"], "Highest"
    ),
    (
        "CPG-012",
        "CPG-012: Bronze Ingestion — IoT Production Sensors (Micro-Batch Every 5 Min)",
        "Implement Bronze micro-batch ingestion for IoT sensor data from production lines. Use Spark Structured Streaming with trigger(processingTime='5 minutes'). Write append-only to brz_freshsip.iot_production_raw. Handle late-arriving data with 10-minute watermark.",
        [
            "AC-1: Given IoT sensor JSON files arriving every 5 minutes, when the micro-batch pipeline runs, then records appear in brz_freshsip.iot_production_raw within 10 minutes of generation.",
            "AC-2: Given the Bronze table, when queried, then columns include: batch_id, production_line_id, sensor_timestamp, actual_output_cases, expected_output_cases, qc_status, downtime_minutes.",
            "AC-3: Given a late record (> 10 min late), when it arrives, then it is included in the next micro-batch window.",
        ],
        8, 1, "E03", ["production", "bronze", "streaming", "mvp"], "High"
    ),
    (
        "CPG-013",
        "CPG-013: Silver Layer — Production Batch Records & QC Results",
        "Build Silver pipeline aggregating IoT micro-batch data into complete batch records. One row per batch_id with: actual vs expected output, QC pass/fail, total downtime. Write to slv_freshsip.production_batches as Delta Lake with MERGE (upsert) on batch_id.",
        [
            "AC-1: Given brz_freshsip.iot_production_raw, when Silver pipeline runs, then slv_freshsip.production_batches has exactly one row per batch_id.",
            "AC-2: Given a batch with multiple QC sensor readings, when aggregated, then qc_pass = true only if all readings pass.",
            "AC-3: Given a batch completion event, when Silver processes it, then batch_status = 'complete' and yield_rate is computed.",
        ],
        5, 1, "E03", ["production", "silver", "pipeline"], "High"
    ),
    (
        "CPG-016",
        "CPG-016: Bronze Ingestion — Logistics Partner CSV (Daily Batch)",
        "Implement Bronze ingestion for daily logistics CSV from third-party logistics partner. Write append-only to brz_freshsip.logistics_raw. Include schema drift detection — alert if columns change from expected schema.",
        [
            "AC-1: Given daily logistics CSV, when pipeline runs, then records are appended to brz_freshsip.logistics_raw.",
            "AC-2: Given the Bronze table, when queried, then columns include: shipment_id, order_id, origin_warehouse, destination, ship_date, promised_delivery_date, actual_delivery_date, cases_shipped, logistics_cost.",
            "AC-3: Given a CSV with unexpected column names, when ingested, then a schema drift alert is logged and the pipeline halts.",
        ],
        3, 1, "E04", ["distribution", "bronze", "pipeline"], "Medium"
    ),
    (
        "CPG-017",
        "CPG-017: Silver Layer — Shipments, Routes & Fulfillment Records",
        "Build Silver pipeline joining logistics Bronze with ERP orders to compute fulfillment status. Write to slv_freshsip.shipments. Compute: is_on_time (actual_delivery_date <= promised_delivery_date), is_fully_shipped (cases_shipped >= cases_ordered).",
        [
            "AC-1: Given brz_freshsip.logistics_raw joined with ERP order data, when Silver runs, then slv_freshsip.shipments has one row per shipment_id.",
            "AC-2: Given a shipment where actual_delivery_date <= promised_delivery_date, when queried, then is_on_time = true.",
            "AC-3: Given a partial shipment, when queried, then is_fully_shipped = false and shortfall_cases = cases_ordered - cases_shipped.",
        ],
        5, 1, "E04", ["distribution", "silver", "pipeline"], "High"
    ),
    (
        "CPG-020",
        "CPG-020: Bronze Ingestion — Customer/Retailer ERP (Daily Batch)",
        "Implement Bronze ingestion for Customer/Retailer master data from daily ERP exports. Write append-only to brz_freshsip.customers_raw. Capture full snapshot each day for SCD2 processing in Silver.",
        [
            "AC-1: Given daily customer ERP CSV, when pipeline runs, then records are appended to brz_freshsip.customers_raw with ingested_date.",
            "AC-2: Given the Bronze table, when queried, then columns include: retailer_id, retailer_name, segment, region, state, credit_terms, account_status, snapshot_date.",
            "AC-3: Given a retailer record that changed vs. prior day, when ingested, then both old and new snapshots exist in Bronze.",
        ],
        3, 1, "E05", ["customers", "bronze", "pipeline"], "Medium"
    ),

    # ── SPRINT 3 ──────────────────────────────────────────────────────────────
    (
        "CPG-006",
        "CPG-006: Gold Layer — Daily Revenue KPI Table",
        "Build Gold aggregation for Daily Revenue KPI. Read from slv_freshsip.sales_transactions and aggregate to gld_freshsip.sales_daily_revenue at grain: date × product_category × region × channel. Partition by transaction_date and Z-order by region, product_category.",
        [
            "AC-1: Given slv_freshsip.sales_transactions, when Gold pipeline runs, then gld_freshsip.sales_daily_revenue has one row per date × product_category × region × channel.",
            "AC-2: Given spot-check query for a known date, when revenue is computed, then total_net_revenue = SUM(unit_price * quantity_sold) - SUM(return_amount) matches hand calculation.",
            "AC-3: Given the Gold table, when dashboard queries run, then query completes in < 5 seconds on a pre-warmed cluster.",
        ],
        5, 2, "E01", ["sales", "gold", "kpi", "mvp", "blocking"], "Highest"
    ),
    (
        "CPG-007",
        "CPG-007: Gold Layer — Gross Margin by SKU",
        "Build Gold aggregation for Gross Margin by SKU. Join slv_freshsip.sales_transactions with slv_freshsip.products (COGS per SKU). Write to gld_freshsip.sales_gross_margin_sku at grain: week × sku_id. Formula: (net_revenue - cogs) / net_revenue * 100.",
        [
            "AC-1: Given sales transactions joined with product COGS, when Gold pipeline runs, then gld_freshsip.sales_gross_margin_sku is populated.",
            "AC-2: Given a SKU with known net_revenue and cogs, when gross_margin_pct is computed, then it equals (net_revenue - cogs) / net_revenue * 100.",
            "AC-3: Given the Gold table, when queried for bottom 10 SKUs by margin, then results return in < 3 seconds.",
        ],
        5, 2, "E01", ["sales", "gold", "kpi", "mvp"], "High"
    ),
    (
        "CPG-010",
        "CPG-010: Gold Layer — Inventory Turnover Rate",
        "Build Gold aggregation for Inventory Turnover Rate. Compute 30-day COGS and average inventory value per warehouse. Write to gld_freshsip.inventory_turnover at grain: week × warehouse_id. Formula: SUM(cogs_30d) / AVG(inventory_value).",
        [
            "AC-1: Given 30 days of inventory snapshots and COGS data, when Gold pipeline runs, then gld_freshsip.inventory_turnover is populated.",
            "AC-2: Given a warehouse with known COGS and inventory value, when turnover_rate is computed, then it matches COGS / AVG(inventory_value).",
            "AC-3: Given warehouses with turnover < 2.0 (warn threshold), when queried, then they are flagged with alert_level = 'warn'.",
        ],
        5, 2, "E02", ["inventory", "gold", "kpi", "mvp"], "High"
    ),
    (
        "CPG-011",
        "CPG-011: Gold Layer — DSI + Reorder Alert Flag",
        "Build Gold table for Days Sales of Inventory and Reorder Alerts. Join slv_freshsip.inventory_stock with 30-day average daily sales from slv_freshsip.sales_transactions. Write to gld_freshsip.inventory_dsi_alerts. Formula: DSI = AVG(stock_units) / AVG(daily_sales_units).",
        [
            "AC-1: Given current stock and 30-day avg daily sales, when Gold pipeline runs, then gld_freshsip.inventory_dsi_alerts has one row per sku_id × warehouse_id × snapshot_date.",
            "AC-2: Given a SKU with DSI < 7 days, when queried, then reorder_alert = true and alert_level = 'critical'.",
            "AC-3: Given the cross-domain join (inventory + sales), when pipeline runs, then it completes within the 30-minute batch window.",
        ],
        5, 2, "E02", ["inventory", "gold", "kpi", "mvp"], "High"
    ),
    (
        "CPG-014",
        "CPG-014: Gold Layer — Batch Yield Rate + Quality Check Pass Rate",
        "Build Gold aggregation for Production KPIs. Read from slv_freshsip.production_batches. Write to gld_freshsip.production_kpis at grain: date × production_line_id × product_id. Compute: yield_rate = actual_output / expected_output * 100, qc_pass_rate = passed_batches / total_batches * 100.",
        [
            "AC-1: Given slv_freshsip.production_batches, when Gold pipeline runs, then gld_freshsip.production_kpis is populated.",
            "AC-2: Given a batch with actual_output = 950 and expected_output = 1000, when yield_rate is computed, then it equals 95.0.",
            "AC-3: Given a production line with qc_pass_rate < 90%, when queried, then alert_level = 'warn'.",
        ],
        5, 2, "E03", ["production", "gold", "kpi"], "High"
    ),
    (
        "CPG-015",
        "CPG-015: Gold Layer — Downtime Hours + Batch Traceability",
        "Build Gold table for Production Downtime and Batch Traceability. Aggregate downtime_minutes by production_line per day. Build traceability linkage: batch_id → shipment_id → retailer_id. Write to gld_freshsip.production_downtime and gld_freshsip.batch_traceability.",
        [
            "AC-1: Given iot data with downtime_minutes, when Gold runs, then gld_freshsip.production_downtime has total_downtime_hours per line per day.",
            "AC-2: Given a batch_id, when traceability table is queried, then it returns the associated shipment_id(s) and retailer_id(s).",
            "AC-3: Given a production line with > 4 hours downtime in a day, when queried, then alert_level = 'critical'.",
        ],
        3, 2, "E03", ["production", "gold", "kpi"], "Medium"
    ),
    (
        "CPG-018",
        "CPG-018: Gold Layer — On-Time Delivery % + Order Fulfillment Rate",
        "Build Gold aggregation for Distribution KPIs. Read from slv_freshsip.shipments. Write to gld_freshsip.distribution_kpis at grain: date × channel × region. Compute: otd_pct = SUM(is_on_time) / COUNT(shipment_id) * 100, fulfillment_rate = SUM(is_fully_shipped) / COUNT(order_id) * 100.",
        [
            "AC-1: Given slv_freshsip.shipments, when Gold pipeline runs, then gld_freshsip.distribution_kpis is populated.",
            "AC-2: Given a channel with 80 on-time out of 100 shipments, when otd_pct is computed, then it equals 80.0.",
            "AC-3: Given OTD% < 85% for a region, when queried, then alert_level = 'warn'.",
        ],
        5, 2, "E04", ["distribution", "gold", "kpi"], "High"
    ),
    (
        "CPG-019",
        "CPG-019: Gold Layer — Cost Per Case Delivered + Worst-Performing Routes",
        "Build Gold table for Distribution cost efficiency KPIs. Compute cost_per_case = logistics_cost / cases_delivered per region. Rank routes by OTD% ascending to identify worst performers. Write to gld_freshsip.distribution_cost_efficiency and gld_freshsip.route_performance.",
        [
            "AC-1: Given shipment cost and case volume data, when Gold runs, then gld_freshsip.distribution_cost_efficiency has cost_per_case per region per week.",
            "AC-2: Given route performance data, when gld_freshsip.route_performance is queried, then routes are ranked by otd_pct ascending (worst first).",
            "AC-3: Given the top 10 worst routes, when queried, then route_rank 1–10 are returned with route_id, otd_pct, and total_shipments.",
        ],
        3, 2, "E04", ["distribution", "gold", "kpi"], "Medium"
    ),

    # ── SPRINT 4 ──────────────────────────────────────────────────────────────
    (
        "CPG-021",
        "CPG-021: Silver Layer — Retailer Profiles with SCD Type 2",
        "Build Silver pipeline for Customer/Retailer master data with SCD Type 2 logic. Track changes to retailer segment, region, credit_terms, and account_status over time. Write to slv_freshsip.customers with columns: effective_date, expiry_date, is_current.",
        [
            "AC-1: Given a retailer whose segment changed between two daily snapshots, when Silver pipeline runs, then two rows exist in slv_freshsip.customers: one with is_current=false (prior), one with is_current=true (new).",
            "AC-2: Given a retailer with no changes, when Silver pipeline runs, then only one row exists with is_current=true.",
            "AC-3: Given slv_freshsip.customers, when queried for active retailers, then filtering WHERE is_current=true returns exactly the current state of each retailer.",
        ],
        8, 3, "E05", ["customers", "silver", "scd2", "pipeline"], "High"
    ),
    (
        "CPG-022",
        "CPG-022: Gold Layer — Top 20 Retailers by Revenue",
        "Build Gold aggregation for Top Retailers KPI. Join slv_freshsip.sales_transactions with slv_freshsip.customers to compute revenue per retailer. Write to gld_freshsip.customer_revenue_rank at grain: month × retailer_id. Include revenue_rank and pct_of_total_revenue.",
        [
            "AC-1: Given monthly sales data, when Gold runs, then gld_freshsip.customer_revenue_rank has revenue_rank 1–N per month.",
            "AC-2: Given total monthly revenue, when pct_of_total_revenue is computed for top 5 retailers, then it equals their combined revenue / total * 100.",
            "AC-3: Given the Gold table, when dashboard queries top 20 retailers, then results load in < 3 seconds.",
        ],
        3, 3, "E05", ["customers", "gold", "kpi"], "High"
    ),
    (
        "CPG-023",
        "CPG-023: Gold Layer — CAC, Retailer Retention Rate & Revenue Concentration Risk",
        "Build Gold table for Customer health KPIs. Compute CAC = (trade_spend + broker_commissions + field_sales_cost) / new_accounts per segment per month. Retention = active_this_period / active_last_period * 100. Concentration Risk = top5_revenue / total_revenue * 100.",
        [
            "AC-1: Given spend and new account data, when CAC is computed, then it equals total_acquisition_spend / COUNT(DISTINCT new_account_id).",
            "AC-2: Given retailer activity across two periods, when retention_rate is computed, then it equals retained_retailers / prior_period_active * 100.",
            "AC-3: Given top 5 retailer revenues, when concentration_risk_pct is computed, then it equals their sum / total company revenue * 100.",
        ],
        5, 3, "E05", ["customers", "gold", "kpi"], "High"
    ),
    (
        "CPG-024",
        "CPG-024: Sales + Inventory Dashboard Pages (Databricks AI/BI)",
        "Build Sales and Inventory dashboard pages in Databricks AI/BI Dashboards. Sales page: Daily Revenue trend chart, MoM/YoY KPI cards, Top SKUs table, Gross Margin by category. Inventory page: Stock level heatmap by warehouse, DSI gauge, Reorder Alert table. Must load in < 5 seconds.",
        [
            "AC-1: Given gld_freshsip.sales_daily_revenue, when Sales dashboard page loads, then daily revenue line chart renders with data for the last 90 days.",
            "AC-2: Given gld_freshsip.inventory_dsi_alerts, when Inventory page loads, then SKUs with reorder_alert=true appear highlighted in the alert table.",
            "AC-3: Given a pre-warmed cluster, when either dashboard page loads, then it renders in < 5 seconds.",
        ],
        8, 3, "E07", ["dashboard", "sales", "inventory", "mvp"], "Highest"
    ),
    (
        "CPG-025",
        "CPG-025: Production + Distribution Dashboard Pages",
        "Build Production and Distribution dashboard pages in Databricks AI/BI. Production page: Batch Yield Rate trend, QC Pass Rate gauge, Downtime Hours bar chart. Distribution page: OTD% by region map, Fulfillment Rate trend, Worst Routes table, Cost Per Case by region.",
        [
            "AC-1: Given gld_freshsip.production_kpis, when Production page loads, then batch yield rate sparkline renders for the last 30 batches.",
            "AC-2: Given gld_freshsip.distribution_kpis, when Distribution page loads, then OTD% by region is displayed as a bar chart ranked by performance.",
            "AC-3: Given gld_freshsip.route_performance, when Worst Routes widget loads, then top 10 worst routes by OTD% are shown in a sortable table.",
        ],
        5, 3, "E07", ["dashboard", "production", "distribution"], "High"
    ),
    (
        "CPG-026",
        "CPG-026: Customers Dashboard Page",
        "Build Customers dashboard page in Databricks AI/BI. Show: Top 20 Retailers leaderboard by revenue with trend sparklines, Revenue Concentration Risk donut chart, Retailer Retention Rate trend, CAC by segment bar chart.",
        [
            "AC-1: Given gld_freshsip.customer_revenue_rank, when Customers page loads, then top 20 retailers are shown in ranked order with monthly revenue.",
            "AC-2: Given revenue concentration data, when donut chart renders, then it shows top 5 vs rest split with correct percentages.",
            "AC-3: Given CAC by segment data, when bar chart loads, then each retail segment has a distinct bar with dollar value label.",
        ],
        3, 3, "E07", ["dashboard", "customers"], "Medium"
    ),
    (
        "CPG-030",
        "CPG-030: Genie AI/BI Natural Language Query Space (STRETCH — Could Have)",
        "STRETCH GOAL: Configure a Databricks Genie Space on the Gold layer tables to enable natural language CEO queries. Target queries: 'Which warehouse is closest to stockout?', 'What was yield rate on the last 5 batches?', 'Who are my top retailers in Texas this month?'. This story may slip to a future sprint if Sprint 4 capacity is exceeded.",
        [
            "AC-1: Given the Genie Space configured on Gold tables, when CEO asks 'What was total revenue last week?', then Genie returns the correct figure from gld_freshsip.sales_daily_revenue.",
            "AC-2: Given the Genie Space, when CEO asks 'Which warehouse has the lowest days of supply for energy drinks?', then Genie returns the correct warehouse from gld_freshsip.inventory_dsi_alerts.",
            "AC-3: Given 5 test queries, when run against Genie, then at least 3 return correct answers without manual SQL intervention.",
        ],
        8, 3, "E07", ["dashboard", "genie", "stretch", "ai"], "Low"
    ),
]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("FreshSip Beverages — Jira Project Push")
    print("=" * 60)

    # 1. Create sprints
    print("\n[1/3] Creating sprints...")
    sprint_ids = []
    for name, start, end in SPRINTS:
        sid = create_sprint(name, start, end)
        sprint_ids.append(sid)
        print(f"       Sprint ID {sid}: {name}")
        time.sleep(0.3)

    # 2. Create epics
    print("\n[2/3] Creating epics...")
    epic_map = {}  # e.g. "E01" -> epic_id
    for ekey, summary, description, labels in EPICS:
        epic_id, epic_jira_key = create_epic(summary, description, labels)
        epic_map[ekey] = epic_id
        print(f"       {epic_jira_key}: {summary[:55]}...")
        time.sleep(0.3)

    # 3. Create stories
    print("\n[3/3] Creating stories...")
    created = []
    for (cpg_id, summary, description, acs, pts, sprint_idx, epic_key, labels, priority) in STORIES:
        sprint_id = sprint_ids[sprint_idx]
        epic_id   = epic_map[epic_key]
        jira_key  = create_story(summary, description, acs, pts, sprint_id, epic_id, labels, priority)
        created.append((cpg_id, jira_key, summary[:50]))
        time.sleep(0.4)

    # Summary
    print("\n" + "=" * 60)
    print(f"DONE — Created {len(sprint_ids)} sprints, {len(epic_map)} epics, {len(created)} stories")
    print("=" * 60)
    print("\nStory mapping:")
    for cpg_id, jira_key, title in created:
        print(f"  {cpg_id} → {jira_key}  {title}")

if __name__ == "__main__":
    main()
