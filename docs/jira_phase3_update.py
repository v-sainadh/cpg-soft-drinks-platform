"""
FreshSip Beverages — Jira Phase 3 Architecture Update Script

What this does:
  1. Finds existing stories by summary prefix (CPG-XXX) to get their Jira issue keys
  2. Posts architecture correction comments to 6 existing stories
  3. Creates 7 new architecture fix tickets (CPG-031 through CPG-037)

Credentials loaded from docs/.env (never committed).
"""

import os
import sys
import time
import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth

# ── Load .env ─────────────────────────────────────────────────────────────────
def _load_env(env_path: Path) -> dict:
    if not env_path.exists():
        sys.exit(f"ERROR: .env not found at {env_path}")
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
        sys.exit(f"ERROR: {key} missing from .env")
    return val

# ── Config ────────────────────────────────────────────────────────────────────
JIRA_URL    = _require("JIRA_URL").rstrip("/")
EMAIL       = _require("JIRA_EMAIL")
API_TOKEN   = _require("JIRA_API_TOKEN")
PROJECT_KEY = "SCRUM"
BOARD_ID    = 1

AUTH    = HTTPBasicAuth(EMAIL, API_TOKEN)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

EPIC_TYPE_ID = "10001"
TASK_TYPE_ID = "10003"
FIELD_STORY_POINTS = "customfield_10016"
FIELD_SPRINT       = "customfield_10020"

# ── Helpers ───────────────────────────────────────────────────────────────────
def api(method, path, **kwargs):
    url = f"{JIRA_URL}{path}"
    resp = getattr(requests, method)(url, auth=AUTH, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text[:400]}")
        resp.raise_for_status()
    return resp.json() if resp.text else {}

def find_issue_by_summary_prefix(prefix: str) -> str | None:
    """Search for an issue whose summary starts with the given prefix."""
    jql = f'project = {PROJECT_KEY} AND summary ~ "{prefix}" ORDER BY created ASC'
    result = api("post", "/rest/api/3/search/jql", json={"jql": jql, "maxResults": 5, "fields": ["summary", "key"]})
    for issue in result.get("issues", []):
        if issue["fields"]["summary"].startswith(prefix):
            return issue["key"]
    return None

def post_comment(issue_key: str, body: str):
    print(f"  Commenting on {issue_key}")
    data = {
        "body": {
            "type": "doc", "version": 1,
            "content": [{"type": "paragraph", "content": [{"type": "text", "text": body}]}]
        }
    }
    api("post", f"/rest/api/3/issue/{issue_key}/comment", json=data)

def add_label(issue_key: str, label: str):
    api("put", f"/rest/api/3/issue/{issue_key}", json={"fields": {"labels": [label]}})

def get_sprint_id(sprint_name_fragment: str) -> int | None:
    """Find a sprint ID by partial name match."""
    result = api("get", f"/rest/agile/1.0/board/{BOARD_ID}/sprint", params={"maxResults": 20})
    for sprint in result.get("values", []):
        if sprint_name_fragment.lower() in sprint["name"].lower():
            return sprint["id"]
    return None

def find_epic_key(epic_summary_prefix: str) -> str | None:
    return find_issue_by_summary_prefix(epic_summary_prefix)

def create_story(summary, description, acceptance_criteria, story_points,
                 sprint_id, epic_key, labels, priority="Medium"):
    print(f"  Creating story: {summary[:70]}...")
    ac_text = "\n".join(f"- {ac}" for ac in acceptance_criteria)
    full_desc = f"{description}\n\nAcceptance Criteria:\n{ac_text}"
    fields = {
        "project":   {"key": PROJECT_KEY},
        "issuetype": {"id": TASK_TYPE_ID},
        "summary":   summary,
        "description": {
            "type": "doc", "version": 1,
            "content": [{"type": "paragraph", "content": [{"type": "text", "text": full_desc}]}]
        },
        "labels":   labels,
        "priority": {"name": priority},
        FIELD_STORY_POINTS: story_points,
    }
    if sprint_id:
        fields[FIELD_SPRINT] = sprint_id
    if epic_key:
        fields["parent"] = {"key": epic_key}
    result = api("post", "/rest/api/3/issue", json={"fields": fields})
    return result["key"]

# ── Phase 3 correction comments on existing stories ───────────────────────────

CORRECTIONS = [
    (
        "CPG-012",
        "[Phase 3 Architecture] Table name correction: "
        "The architecture schema defines the Bronze IoT table as brz_freshsip.iot_sensor_events_raw "
        "(not iot_production_raw as referenced in AC-1). "
        "Please use brz_freshsip.iot_sensor_events_raw in all implementation work."
    ),
    (
        "CPG-014",
        "[Phase 3 Architecture] Column name corrections from schema validation: "
        "(1) AC-1: column is yield_rate_pct (not batch_yield_rate_pct). "
        "(2) AC-3: column is qc_pass_rate_pct (not quality_pass_rate_pct) and alert column is qc_warn_flag (not quality_alert_flag). "
        "Implement using schema-gold.md column names as the authoritative reference."
    ),
    (
        "CPG-018",
        "[Phase 3 Architecture] Column name correction: "
        "AC-3 references fulfillment_alert_flag but the architecture schema defines this column as fulfillment_warn_flag. "
        "Implement as fulfillment_warn_flag per schema-gold.md."
    ),
    (
        "CPG-019",
        "[Phase 3 Architecture] Column name correction: "
        "AC-1 references cost_per_case_usd but the architecture schema defines this column as cost_per_case (no _usd suffix). "
        "Implement as cost_per_case per schema-gold.md. "
        "Also note: the stateful consecutive_weeks_in_worst10 counter for KPI-D04 now uses a LEFT JOIN against the prior week's "
        "Gold partition — see CPG-035 for the implementation design."
    ),
    (
        "CPG-021",
        "[Phase 3 Architecture] SCD Type 2 column name correction: "
        "AC-1 and AC-4 reference effective_start_date / effective_end_date, but the architecture schema uses "
        "valid_from / valid_to (industry-standard SCD2 naming). "
        "Implement as valid_from / valid_to per schema-silver.md."
    ),
    (
        "CPG-029",
        "[Phase 3 Architecture] Missing schema — action required: "
        "AC-1 references slv_freshsip.pipeline_dq_log but this table has no DDL defined in schema-silver.md. "
        "CPG-032 has been raised to define this schema before Sprint 1 ends. "
        "Do not implement the DQ log table until CPG-032 provides the authoritative DDL."
    ),
]

# ── New Phase 3 architecture fix tickets ──────────────────────────────────────

NEW_TICKETS = [
    {
        "id":          "CPG-031",
        "summary":     "CPG-031: [ARCH FIX] Add batch_id FK to slv_freshsip.shipments for KPI-P04 Traceability",
        "description": (
            "CRITICAL gap identified during Phase 3 architecture validation. "
            "The KPI-P04 Batch Traceability SQL joins production_batches to shipments using batch_id = order_id, "
            "but slv_freshsip.shipments has no batch_id column. "
            "This column has now been added to schema-silver.md (nullable STRING FK to production_batches.batch_id). "
            "The Bronze logistics JSON schema (config/schemas/bronze_logistics_shipments.json) has been updated to "
            "document batch_id as an optional source field. "
            "The Data Engineer must: (1) ensure the logistics CSV synthetic data includes batch_id values, "
            "(2) extract batch_id in the Silver shipments pipeline, "
            "(3) validate the KPI-P04 traceability join produces non-empty results."
        ),
        "acceptance_criteria": [
            "AC-1: Given slv_freshsip.shipments, when schema is inspected, then a nullable batch_id STRING column is present.",
            "AC-2: Given synthetic logistics CSV data, when Bronze ingestion runs, then batch_id values are present in at least 80% of records.",
            "AC-3: Given slv_freshsip.shipments joined to slv_freshsip.production_batches on batch_id, when KPI-P04 query runs, then gld_freshsip.production_traceability returns non-empty rows.",
            "AC-4: Given a specific batch_id, when production_traceability is queried, then it returns the associated shipment_id and retailer_id.",
        ],
        "story_points": 2,
        "sprint":       "S2",
        "epic":         "CPG-E04",
        "labels":       ["distribution", "silver", "arch-fix", "kpi-p04", "blocking"],
        "priority":     "Highest",
    },
    {
        "id":          "CPG-032",
        "summary":     "CPG-032: [ARCH FIX] Define slv_freshsip.pipeline_dq_log schema and DDL",
        "description": (
            "The DQ monitoring table slv_freshsip.pipeline_dq_log is referenced in multiple PRD acceptance criteria "
            "(CPG-005 AC-5, CPG-009 AC-4, CPG-029 AC-1) but has no DDL defined in schema-silver.md. "
            "This is a HIGH severity gap — without a schema definition, the Data Engineer cannot implement the DQ log "
            "consistently across pipelines. "
            "Deliver: DDL for slv_freshsip.pipeline_dq_log including columns: "
            "run_id, pipeline_name, table_name, run_timestamp, rows_processed, rows_rejected, null_rates (MAP), "
            "dq_check_results (ARRAY<STRUCT>), overall_status (PASS/WARN/FAIL)."
        ),
        "acceptance_criteria": [
            "AC-1: Given schema-silver.md, when the pipeline_dq_log section is read, then a complete CREATE TABLE DDL is present.",
            "AC-2: Given the DDL, when executed in Databricks, then the table is created without errors in slv_freshsip.",
            "AC-3: Given a Silver pipeline run, when it completes, then one row per run is written to pipeline_dq_log with non-null run_id and overall_status.",
            "AC-4: Given pipeline_dq_log, when queried for a failed run, then rows_rejected > 0 and overall_status IN ('WARN', 'FAIL').",
        ],
        "story_points": 2,
        "sprint":       "S1",
        "epic":         "CPG-E08",
        "labels":       ["dq", "silver", "arch-fix", "blocking"],
        "priority":     "High",
    },
    {
        "id":          "CPG-033",
        "summary":     "CPG-033: [ARCH FIX] Correct KPI-I02 SQL alias and KPI-I03 DSI warehouse dimension",
        "description": (
            "Two SQL errors identified in schema-gold.md during Phase 3 validation — both fixed in the schema, "
            "Data Engineer must implement corrected logic:\n\n"
            "Fix 1 — KPI-I02 Inventory Turnover: The cogs_30d CTE referenced s.transaction_date "
            "where the alias for sales_transactions is t. Fixed to t.transaction_date.\n\n"
            "Fix 2 — KPI-I03 DSI: The avg_daily_sales CTE grouped only by sku_id, ignoring warehouse_id. "
            "This caused all warehouses holding the same SKU to receive the same average daily sales figure "
            "regardless of warehouse-level sales mix. Fixed to GROUP BY (sku_id, warehouse_id) "
            "with a JOIN to inventory_stock on both sku_id and warehouse_id. "
            "The final LEFT JOIN is now on i.sku_id = s.sku_id AND i.warehouse_id = s.warehouse_id."
        ),
        "acceptance_criteria": [
            "AC-1: Given the KPI-I02 Gold pipeline, when run against synthetic data, then it completes without SQL runtime errors.",
            "AC-2: Given two warehouses holding the same SKU with different sales volumes, when KPI-I03 is computed, then dsi_days differs between the two warehouses.",
            "AC-3: Given a warehouse with zero sales for a SKU, when KPI-I03 runs, then dsi_days is NULL (not a division-by-zero error).",
        ],
        "story_points": 1,
        "sprint":       "S3",
        "epic":         "CPG-E02",
        "labels":       ["inventory", "gold", "arch-fix", "kpi-i02", "kpi-i03"],
        "priority":     "High",
    },
    {
        "id":          "CPG-034",
        "summary":     "CPG-034: [ARCH FIX] Standardize alert flag naming across all Gold KPI tables",
        "description": (
            "Phase 3 validation found inconsistent alert column naming between PRD acceptance criteria and Gold schema: "
            "some columns are _warn_flag, others _alert_flag. "
            "Decision: standardize all to _alert_flag for consistency with PRD wording. "
            "Affected columns: turnover_warn_flag → turnover_alert_flag (inventory_turnover), "
            "fulfillment_warn_flag → fulfillment_alert_flag (distribution_fulfillment), "
            "dsi_warn_flag → dsi_alert_flag (inventory_dsi), "
            "route_warn_flag → route_alert_flag (distribution_route_performance), "
            "qc_warn_flag → qc_alert_flag (production_quality). "
            "Update schema-gold.md DDLs and all computation SQL to use _alert_flag suffix."
        ),
        "acceptance_criteria": [
            "AC-1: Given all Gold KPI DDLs in schema-gold.md, when searched for _warn_flag, then zero occurrences are found.",
            "AC-2: Given the Gold tables in Databricks after deployment, when schema is inspected, then all alert columns use the _alert_flag suffix.",
            "AC-3: Given the dashboard queries referencing alert columns, then they use _alert_flag names and return correct TRUE/FALSE values.",
        ],
        "story_points": 1,
        "sprint":       "S3",
        "epic":         "CPG-E06",
        "labels":       ["gold", "arch-fix", "naming"],
        "priority":     "High",
    },
    {
        "id":          "CPG-035",
        "summary":     "CPG-035: [ARCH FIX] Implement stateful consecutive_weeks_in_worst10 counter for KPI-D04",
        "description": (
            "KPI-D04 Worst-Performing Routes requires a consecutive_weeks_in_worst10 counter to fire the critical alert "
            "when a route appears in the worst-10 list for 3+ consecutive weeks. "
            "The original SQL hardcoded this to 0, making the critical alert permanently inactive. "
            "The schema-gold.md has been updated with the correct stateful pattern:\n\n"
            "Implementation: the weekly INSERT reads the prior week's partition from gld_freshsip.distribution_route_performance "
            "via LEFT JOIN on (route_id, week_start_date = DATE_ADD(current_monday, -7)). "
            "COALESCE(prior.consecutive_weeks_in_worst10, 0) + 1 gives: "
            "1 for routes newly entering worst-10, N+1 for routes already tracked, "
            "and routes that drop out of worst-10 naturally get no row written (counter resets). "
            "route_critical_flag = TRUE when OTD < 70% OR consecutive_weeks >= 3."
        ),
        "acceptance_criteria": [
            "AC-1: Given a route in the worst-10 for week N, when week N+1 runs, then consecutive_weeks_in_worst10 = 2.",
            "AC-2: Given a route in the worst-10 for 3 consecutive weeks with OTD > 70%, when queried, then route_critical_flag = true.",
            "AC-3: Given a route that drops out of worst-10 in week N+1, when queried for week N+2, then it has no row in that week's partition (counter implicitly reset).",
            "AC-4: Given a brand-new worst-10 route (first week), when queried, then consecutive_weeks_in_worst10 = 1 and route_critical_flag is determined by OTD threshold only.",
        ],
        "story_points": 3,
        "sprint":       "S3",
        "epic":         "CPG-E04",
        "labels":       ["distribution", "gold", "arch-fix", "kpi-d04", "stateful"],
        "priority":     "High",
    },
    {
        "id":          "CPG-036",
        "summary":     "CPG-036: [ARCH FIX] Validate ERP customer CSV includes spend columns for KPI-C02 CAC",
        "description": (
            "KPI-C02 Customer Acquisition Cost depends on slv_freshsip.sales_spend, which is derived from "
            "the ERP customer CSV (brz_freshsip.erp_customers_raw). "
            "The architecture assumes the customer CSV contains trade_spend_usd, broker_commission_usd, "
            "and field_sales_cost_usd columns — but no business rule or data contract confirms this. "
            "If these columns are absent, CAC will compute as $0 silently with no pipeline error. "
            "Action: (1) Confirm spend columns are present in synthetic erp_customers CSV. "
            "(2) Add a Bronze DQ rule: WARN if SUM(trade_spend_usd) = 0 for any ingestion period. "
            "(3) Document the spend column contract in config/schemas/bronze_erp_customers.json."
        ),
        "acceptance_criteria": [
            "AC-1: Given config/schemas/bronze_erp_customers.json, when reviewed, then trade_spend_usd, broker_commission_usd, and field_sales_cost_usd are documented as properties.",
            "AC-2: Given synthetic erp_customers CSV, when ingested, then slv_freshsip.sales_spend contains non-zero values for at least one retailer per period.",
            "AC-3: Given a Bronze DQ check on erp_customers_raw, when total spend for a period is $0, then a WARNING is logged to pipeline_dq_log.",
            "AC-4: Given slv_freshsip.sales_spend joined with new_accounts in the CAC computation, when gld_freshsip.customers_cac is queried, then cac_usd > 0 for at least one segment.",
        ],
        "story_points": 1,
        "sprint":       "S2",
        "epic":         "CPG-E05",
        "labels":       ["customers", "gold", "arch-fix", "kpi-c02", "dq"],
        "priority":     "High",
    },
    {
        "id":          "CPG-037",
        "summary":     "CPG-037: [ARCH FIX] Implement prior-period rank lookback for KPI-C01 rank_movement",
        "description": (
            "KPI-C01 Top 20 Retailers requires a rank_movement column showing week-over-week rank change "
            "(positive = improved, negative = declined). "
            "The current Gold SQL sets rank_movement = NULL because prior-period rank is not computed. "
            "CPG-022 AC-4 requires this column. "
            "Implementation: LEFT JOIN gld_freshsip.customers_top_retailers prior "
            "ON retailer_id = prior.retailer_id AND prior.period_start_date = DATE_ADD(current_week_start, -7). "
            "rank_movement = prior.retailer_rank - current_retailer_rank "
            "(positive = moved up the rankings, negative = moved down)."
        ),
        "acceptance_criteria": [
            "AC-1: Given a retailer ranked #5 last week and #3 this week, when KPI-C01 runs, then rank_movement = 2.",
            "AC-2: Given a retailer not in last week's top 20, when they enter this week, then rank_movement = NULL (no prior rank to compare).",
            "AC-3: Given the Gold table, when queried for the current week, then rank_movement is non-null for all retailers who appeared in both the current and prior week's top 20.",
        ],
        "story_points": 2,
        "sprint":       "S4",
        "epic":         "CPG-E05",
        "labels":       ["customers", "gold", "arch-fix", "kpi-c01"],
        "priority":     "Medium",
    },
]

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("FreshSip Phase 3 — Jira Architecture Update")
    print("=" * 60)

    # Step 1: Post correction comments to existing stories
    print("\n[1/3] Posting architecture correction comments to existing stories...")
    for cpg_id, comment in CORRECTIONS:
        prefix = f"{cpg_id}:"
        key = find_issue_by_summary_prefix(prefix)
        if key:
            post_comment(key, comment)
            time.sleep(0.3)
        else:
            print(f"  WARNING: Could not find issue with summary prefix '{prefix}' — skipping comment")

    # Step 2: Resolve sprint IDs
    print("\n[2/3] Resolving sprint IDs...")
    sprint_map = {}
    for frag in ("S1", "S2", "S3", "S4"):
        sid = get_sprint_id(frag)
        sprint_map[frag] = sid
        status = sid if sid else "NOT FOUND"
        print(f"  {frag} → sprint_id={status}")

    # Step 3: Create new fix tickets
    print("\n[3/3] Creating Phase 3 architecture fix tickets...")
    epic_key_cache = {}
    created = []
    for t in NEW_TICKETS:
        # Resolve epic key
        epic_code = t["epic"]  # e.g. "CPG-E04"
        if epic_code not in epic_key_cache:
            key = find_issue_by_summary_prefix(f"{epic_code}:")
            epic_key_cache[epic_code] = key
            if not key:
                print(f"  WARNING: Epic {epic_code} not found — story will be created without epic link")
        epic_key = epic_key_cache.get(epic_code)

        sprint_id = sprint_map.get(t["sprint"])

        new_key = create_story(
            summary=t["summary"],
            description=t["description"],
            acceptance_criteria=t["acceptance_criteria"],
            story_points=t["story_points"],
            sprint_id=sprint_id,
            epic_key=epic_key,
            labels=t["labels"],
            priority=t["priority"],
        )
        created.append((t["id"], new_key))
        time.sleep(0.4)

    print("\n" + "=" * 60)
    print("Phase 3 update complete.")
    print(f"  Comments posted: {len(CORRECTIONS)}")
    print(f"  New tickets created: {len(created)}")
    for cpg_id, jira_key in created:
        print(f"    {cpg_id} → {jira_key}")
    print("=" * 60)

if __name__ == "__main__":
    main()
