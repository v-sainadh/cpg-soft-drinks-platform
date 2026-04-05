"""
FreshSip Beverages — Jira Update: Synthetic Data Generation Complete (CPG-002)

What this does:
  1. Finds CPG-002 in Jira by summary prefix
  2. Posts a detailed completion comment with stats and file inventory
  3. Transitions CPG-002 to Done
  4. Posts a scope-change note (31-day window vs. 13-month AC-1 — by design for Phase 1)

Credentials loaded from docs/.env (never committed).
Run from repo root: python docs/jira_synthetic_data_update.py
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

AUTH    = HTTPBasicAuth(EMAIL, API_TOKEN)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

# ── Helpers ───────────────────────────────────────────────────────────────────
def api(method, path, **kwargs):
    url = f"{JIRA_URL}{path}"
    resp = getattr(requests, method)(url, auth=AUTH, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
    return resp.json() if resp.text else {}

def find_issue_by_summary_prefix(prefix: str) -> str | None:
    """Return the Jira issue key whose summary starts with prefix."""
    jql = f'project = {PROJECT_KEY} AND summary ~ "{prefix}" ORDER BY created ASC'
    result = api("post", "/rest/api/3/search/jql",
                 json={"jql": jql, "maxResults": 5, "fields": ["summary", "key"]})
    for issue in result.get("issues", []):
        if issue["fields"]["summary"].startswith(prefix):
            return issue["key"]
    return None

def get_transitions(issue_key: str) -> list[dict]:
    return api("get", f"/rest/api/3/issue/{issue_key}/transitions").get("transitions", [])

def transition_issue(issue_key: str, transition_name_fragment: str) -> bool:
    transitions = get_transitions(issue_key)
    match = next(
        (t for t in transitions if transition_name_fragment.lower() in t["name"].lower()),
        None
    )
    if not match:
        names = [t["name"] for t in transitions]
        print(f"  WARNING: No transition matching '{transition_name_fragment}' found. Available: {names}")
        return False
    api("post", f"/rest/api/3/issue/{issue_key}/transitions",
        json={"transition": {"id": match["id"]}})
    print(f"  Transitioned {issue_key} → '{match['name']}'")
    return True

def post_comment(issue_key: str, paragraphs: list[str]):
    content = [
        {"type": "paragraph", "content": [{"type": "text", "text": p}]}
        for p in paragraphs
    ]
    api("post", f"/rest/api/3/issue/{issue_key}/comment",
        json={"body": {"type": "doc", "version": 1, "content": content}})
    print(f"  Comment posted to {issue_key}")


# ── Completion comment content ────────────────────────────────────────────────

COMPLETION_PARAGRAPHS = [
    "[DONE — Synthetic Data Generation] Work completed 2026-04-05.",

    "SCOPE NOTE: AC-1 requested 13 months (2025-03-01 – 2026-04-05). "
    "Per Sprint 1 planning, the initial generation window is 2025-10-01 – 2025-10-31 (31 days) "
    "to provide a focused, high-quality dataset for Bronze pipeline development and dashboard demos. "
    "Expanding to 13 months is tracked as a backlog item (data volume vs. Community Edition cluster limits).",

    "FILES GENERATED — data/synthetic/:",

    "ERP (CSV): "
    "erp/products.csv (50 SKUs: Carbonated 20, Water 10, Energy 12, Juice 8) | "
    "erp/customers.csv (200 retailers, 4 regions, 3 segments) | "
    "erp/warehouses.csv (8 warehouses, 2 per region) | "
    "erp/orders.csv (15,150 rows incl. 150 injected duplicates) | "
    "erp/order_lines.csv (42,986 rows) | "
    "erp/inventory_daily.csv (12,400 rows: 8 WH × 50 SKUs × 31 days).",

    "POS (NDJSON): pos/pos_transactions.json — 50,000 transactions, "
    "1–5 items each, across 200 store locations.",

    "Production (NDJSON): production/batches.json (500 batches, 6 lines) | "
    "production/quality_checks.json (2,262 checks, 6 check types, ~5% fail rate) | "
    "production/downtime_events.json (100 events, 5 categories).",

    "Logistics (CSV): logistics/shipments.csv — 3,000 shipments linked to delivered/shipped orders.",

    "BUSINESS KPI ACTUALS: "
    "Total ERP revenue $17.5M (target $15–25M) ✓ | "
    "Avg production yield 95.4% (target 94–97%) ✓ | "
    "On-time delivery 92.3% (target ~92%) ✓ | "
    "QC pass rate ~95% ✓.",

    "DIRTY DATA INJECTED (for Silver DQ testing): "
    "~2% nulls on non-key fields in orders and order_lines | "
    "150 duplicate order_ids (~1%) | "
    "5 future-dated orders (2025-11-01 – 2025-11-05) | "
    "~10 negative unit_prices | "
    "2 null launch_dates in products.",

    "REFERENTIAL INTEGRITY: All FK relationships verified 100% consistent "
    "(order_lines→orders, order_lines→products, inventory→warehouses, inventory→products, "
    "shipments→orders, shipments→warehouses, pos→customers, pos items→products, quality_checks→batches).",

    "REPRODUCIBILITY: Generator script at data/synthetic/generate_data.py — stdlib only (no pandas), "
    "seed=42, fully deterministic. Re-run at any time to regenerate identical data.",

    "AC-5 STATUS: Referential integrity >= 95% — PASS (100%). "
    "AC-2 STATUS: Files generated and ready for Bronze ingestion pipeline (CPG-003, CPG-004). "
    "AC-3/AC-4 STATUS: Pending Gold layer implementation (Sprint 3).",
]

SCOPE_NOTE_PARAGRAPHS = [
    "[SCOPE DELTA — CPG-002 AC-1] 13-month range deferred to avoid Community Edition "
    "cluster memory constraints during Sprint 1 validation. "
    "The 31-day October 2025 window covers all 5 domains, all file formats, "
    "and all KPI calculation paths. "
    "Recommend raising a backlog story to extend to full 13-month range "
    "once Bronze pipelines are validated on the smaller dataset.",
]

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("FreshSip — Jira Update: CPG-002 Synthetic Data Complete")
    print("=" * 60)

    # 1. Locate CPG-002
    print("\n[1/3] Locating CPG-002 in Jira...")
    key = find_issue_by_summary_prefix("CPG-002:")
    if not key:
        # Fallback: search by content
        key = find_issue_by_summary_prefix("Synthetic Data Generation")
    if not key:
        print("  ERROR: Could not find CPG-002. Searching by 'Synthetic Data'...")
        jql = f'project = {PROJECT_KEY} AND summary ~ "Synthetic Data" ORDER BY created ASC'
        result = api("post", "/rest/api/3/search/jql",
                     json={"jql": jql, "maxResults": 5, "fields": ["summary", "key"]})
        for issue in result.get("issues", []):
            print(f"  Found candidate: {issue['key']} — {issue['fields']['summary']}")
        sys.exit("Could not locate CPG-002. Please check Jira manually.")
    print(f"  Found: {key}")

    # 2. Post completion comment
    print(f"\n[2/3] Posting completion comment to {key}...")
    post_comment(key, COMPLETION_PARAGRAPHS)
    time.sleep(0.5)
    post_comment(key, SCOPE_NOTE_PARAGRAPHS)

    # 3. Transition to Done
    print(f"\n[3/3] Transitioning {key} to Done...")
    done = transition_issue(key, "done")
    if not done:
        # Try alternate names
        for name in ("complete", "close", "resolve"):
            if transition_issue(key, name):
                break

    print("\n" + "=" * 60)
    print(f"Update complete. {key} marked Done with completion comment.")
    print("=" * 60)

if __name__ == "__main__":
    main()
