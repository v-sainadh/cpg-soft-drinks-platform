#!/usr/bin/env python3
"""
FreshSip Beverages — Jira CLI Utility for the Scrum Master Agent
=================================================================
A single-file CLI that the SM agent calls via Bash to interact with Jira.

Usage:
  python docs/jira_utils.py find   "CPG-002"
  python docs/jira_utils.py info   SCRUM-15
  python docs/jira_utils.py comment SCRUM-15 "Sprint update: pipeline complete."
  python docs/jira_utils.py close  SCRUM-15
  python docs/jira_utils.py status SCRUM-15 "In Progress"
  python docs/jira_utils.py sprint-list
  python docs/jira_utils.py board-list
  python docs/jira_utils.py create  --summary "CPG-042: ..." --description "..." \\
                                     --sprint S1 --points 3 --priority High \\
                                     --labels bronze,blocking --epic CPG-E01
  python docs/jira_utils.py done-report          # List all Done stories for current sprint
  python docs/jira_utils.py sprint-status S1     # Show all stories and statuses for sprint

Credentials: docs/.env (JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN)
"""

import json
import os
import sys
import argparse
import time
import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth

# ── Credentials ───────────────────────────────────────────────────────────────

def _load_env(env_path: Path) -> dict:
    if not env_path.exists():
        sys.exit(f"ERROR: .env not found at {env_path}\n"
                 f"Create docs/.env with JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN")
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env

_ENV_PATH = Path(__file__).parent / ".env"
_env      = _load_env(_ENV_PATH)

def _require(key: str) -> str:
    val = _env.get(key) or os.environ.get(key, "")
    if not val:
        sys.exit(f"ERROR: {key} missing from docs/.env")
    return val

JIRA_URL    = _require("JIRA_URL").rstrip("/")
EMAIL       = _require("JIRA_EMAIL")
API_TOKEN   = _require("JIRA_API_TOKEN")
PROJECT_KEY = _env.get("JIRA_PROJECT_KEY", "SCRUM")
BOARD_ID    = int(_env.get("JIRA_BOARD_ID", "1"))

AUTH    = HTTPBasicAuth(EMAIL, API_TOKEN)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

TASK_TYPE_ID       = "10003"
FIELD_STORY_POINTS = "customfield_10016"
FIELD_SPRINT       = "customfield_10020"

# ── Core API wrapper ──────────────────────────────────────────────────────────

def _api(method: str, path: str, **kwargs) -> dict:
    url  = f"{JIRA_URL}{path}"
    resp = getattr(requests, method)(url, auth=AUTH, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"[Jira API ERROR] {resp.status_code}: {resp.text[:600]}", file=sys.stderr)
        resp.raise_for_status()
    return resp.json() if resp.text else {}

# ── Helpers ───────────────────────────────────────────────────────────────────

def find_issue(identifier: str) -> dict | None:
    """
    Find a Jira issue by:
      - Jira key (e.g. SCRUM-15)
      - CPG ID prefix (e.g. CPG-002 → finds issue whose summary starts with 'CPG-002:')
      - Summary fragment (falls back to JQL ~ search)
    Returns the full issue dict, or None if not found.
    """
    # If it looks like a Jira key (PROJECT-NNN), fetch directly
    if "-" in identifier and identifier.split("-")[0].isupper() and identifier.split("-")[-1].isdigit():
        try:
            return _api("get", f"/rest/api/3/issue/{identifier}")
        except Exception:
            pass

    # Otherwise search by summary prefix
    jql    = f'project = {PROJECT_KEY} AND summary ~ "{identifier}" ORDER BY created ASC'
    result = _api("post", "/rest/api/3/search/jql",
                  json={"jql": jql, "maxResults": 10,
                        "fields": ["summary", "key", "status", "assignee", "priority",
                                   "customfield_10016", "customfield_10020", "labels"]})
    for issue in result.get("issues", []):
        summ = issue["fields"].get("summary", "")
        if summ.startswith(identifier) or identifier.lower() in summ.lower():
            return issue
    return None


def get_transitions(issue_key: str) -> list[dict]:
    return _api("get", f"/rest/api/3/issue/{issue_key}/transitions").get("transitions", [])


def do_transition(issue_key: str, name_fragment: str) -> str:
    """Transition an issue to a status matching name_fragment. Returns new status name."""
    transitions = get_transitions(issue_key)
    match = next(
        (t for t in transitions if name_fragment.lower() in t["name"].lower()),
        None
    )
    if not match:
        available = [t["name"] for t in transitions]
        raise ValueError(f"No transition matching '{name_fragment}'. Available: {available}")
    _api("post", f"/rest/api/3/issue/{issue_key}/transitions",
         json={"transition": {"id": match["id"]}})
    return match["name"]


def post_comment(issue_key: str, text: str):
    """Post a plain-text comment to an issue."""
    _api("post", f"/rest/api/3/issue/{issue_key}/comment",
         json={"body": {
             "type": "doc", "version": 1,
             "content": [{"type": "paragraph",
                          "content": [{"type": "text", "text": text}]}]
         }})


def get_sprint_id(sprint_fragment: str) -> int | None:
    result = _api("get", f"/rest/agile/1.0/board/{BOARD_ID}/sprint",
                  params={"maxResults": 20})
    for sprint in result.get("values", []):
        if sprint_fragment.lower() in sprint["name"].lower():
            return sprint["id"]
    return None


def get_epic_key(epic_code: str) -> str | None:
    """Find epic Jira key from CPG epic code like CPG-E01."""
    issue = find_issue(f"{epic_code}:")
    return issue["key"] if issue else None


def _fmt_issue(issue: dict) -> str:
    f      = issue["fields"]
    key    = issue["key"]
    summ   = f.get("summary", "")
    status = f.get("status", {}).get("name", "?")
    pts    = f.get("customfield_10016") or "-"
    pri    = f.get("priority", {}).get("name", "?")
    labels = ", ".join(f.get("labels", [])) or "-"
    return (f"  {key}  [{status}]  {pts}pts  {pri}\n"
            f"  Summary: {summ}\n"
            f"  Labels:  {labels}")


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_find(args):
    issue = find_issue(args.identifier)
    if not issue:
        print(f"NOT FOUND: '{args.identifier}'")
        sys.exit(1)
    print(_fmt_issue(issue))
    print(f"\n  Jira key: {issue['key']}")


def cmd_info(args):
    issue = find_issue(args.key)
    if not issue:
        print(f"NOT FOUND: '{args.key}'")
        sys.exit(1)
    f      = issue["fields"]
    status = f.get("status", {}).get("name", "?")
    assignee = (f.get("assignee") or {}).get("displayName", "Unassigned")
    sprint_raw = f.get("customfield_10020")
    sprint_name = "-"
    if isinstance(sprint_raw, list) and sprint_raw:
        sprint_name = sprint_raw[-1].get("name", "-")
    print(json.dumps({
        "key":      issue["key"],
        "summary":  f.get("summary"),
        "status":   status,
        "assignee": assignee,
        "points":   f.get("customfield_10016"),
        "priority": (f.get("priority") or {}).get("name"),
        "labels":   f.get("labels", []),
        "sprint":   sprint_name,
    }, indent=2))


def cmd_comment(args):
    issue = find_issue(args.key)
    if not issue:
        print(f"NOT FOUND: '{args.key}'")
        sys.exit(1)
    real_key = issue["key"]
    post_comment(real_key, args.message)
    print(f"✓ Comment posted to {real_key}")


def cmd_close(args):
    issue = find_issue(args.key)
    if not issue:
        print(f"NOT FOUND: '{args.key}'")
        sys.exit(1)
    real_key = issue["key"]
    # Try "Done" first, then fallbacks
    for name in ("Done", "Complete", "Closed", "Resolve"):
        try:
            result = do_transition(real_key, name)
            print(f"✓ {real_key} → '{result}'")
            return
        except ValueError:
            continue
    transitions = get_transitions(real_key)
    print(f"Could not find a closing transition. Available: {[t['name'] for t in transitions]}")
    sys.exit(1)


def cmd_status(args):
    """Transition issue to a given status name."""
    issue = find_issue(args.key)
    if not issue:
        print(f"NOT FOUND: '{args.key}'")
        sys.exit(1)
    real_key = issue["key"]
    result   = do_transition(real_key, args.transition)
    print(f"✓ {real_key} → '{result}'")


def cmd_sprint_list(args):
    result = _api("get", f"/rest/agile/1.0/board/{BOARD_ID}/sprint",
                  params={"maxResults": 20})
    for s in result.get("values", []):
        print(f"  {s['id']:>4}  {s['state']:12}  {s['name']}")


def cmd_board_list(args):
    result = _api("get", "/rest/agile/1.0/board", params={"maxResults": 20})
    for b in result.get("values", []):
        print(f"  {b['id']:>4}  {b['name']}")


def cmd_create(args):
    sprint_id = get_sprint_id(args.sprint) if args.sprint else None
    epic_key  = get_epic_key(args.epic) if args.epic else None

    labels  = [l.strip() for l in args.labels.split(",")] if args.labels else []
    ac_text = "\n".join(f"- {ac}" for ac in (args.ac or []))
    full_desc = f"{args.description or ''}\n\nAcceptance Criteria:\n{ac_text}" if ac_text else (args.description or "")

    fields = {
        "project":             {"key": PROJECT_KEY},
        "issuetype":           {"id": TASK_TYPE_ID},
        "summary":             args.summary,
        "description": {
            "type": "doc", "version": 1,
            "content": [{"type": "paragraph",
                         "content": [{"type": "text", "text": full_desc}]}]
        },
        "labels":   labels,
        "priority": {"name": args.priority or "Medium"},
    }
    if args.points:
        fields[FIELD_STORY_POINTS] = float(args.points)
    if sprint_id:
        fields[FIELD_SPRINT] = sprint_id
    if epic_key:
        fields["parent"] = {"key": epic_key}

    result = _api("post", "/rest/api/3/issue", json={"fields": fields})
    print(f"✓ Created: {result['key']}  —  {args.summary[:60]}")
    return result["key"]


def cmd_sprint_status(args):
    """List all stories in a sprint with their current statuses."""
    sprint_id = get_sprint_id(args.sprint)
    if not sprint_id:
        print(f"Sprint '{args.sprint}' not found.")
        sys.exit(1)

    result = _api("get", f"/rest/agile/1.0/sprint/{sprint_id}/issue",
                  params={"maxResults": 100,
                          "fields": "summary,status,priority,customfield_10016,assignee,labels"})
    issues = result.get("issues", [])
    if not issues:
        print(f"No issues found in sprint '{args.sprint}' (id={sprint_id})")
        return

    # Group by status
    by_status: dict[str, list] = {}
    for issue in issues:
        st = issue["fields"]["status"]["name"]
        by_status.setdefault(st, []).append(issue)

    total = len(issues)
    done_ct = len(by_status.get("Done", []))
    print(f"\nSprint: {args.sprint} (id={sprint_id})  |  {done_ct}/{total} Done")
    print("=" * 60)
    for status, grp in sorted(by_status.items()):
        print(f"\n[{status}] ({len(grp)} issues)")
        for issue in grp:
            f    = issue["fields"]
            pts  = f.get("customfield_10016") or "-"
            summ = f.get("summary", "")[:70]
            print(f"  {issue['key']:12}  {pts}pts  {summ}")


def cmd_done_report(args):
    """Show all Done stories across the project with a summary."""
    jql = f'project = {PROJECT_KEY} AND status = Done ORDER BY updated DESC'
    result = _api("post", "/rest/api/3/search/jql",
                  json={"jql": jql, "maxResults": 50,
                        "fields": ["summary", "key", "status", "customfield_10016",
                                   "customfield_10020", "labels", "updated"]})
    issues = result.get("issues", [])
    total_pts = 0
    print(f"\nDone Stories — {PROJECT_KEY} ({len(issues)} issues)")
    print("=" * 70)
    for issue in issues:
        f    = issue["fields"]
        pts  = f.get("customfield_10016") or 0
        total_pts += pts or 0
        sprint_raw  = f.get("customfield_10020")
        sprint_name = "-"
        if isinstance(sprint_raw, list) and sprint_raw:
            sprint_name = sprint_raw[-1].get("name", "-")
        updated = issue["fields"].get("updated", "")[:10]
        print(f"  {issue['key']:12}  {str(pts):>3}pts  [{sprint_name:6}]  {updated}  "
              f"{f.get('summary', '')[:50]}")
    print(f"\n  Total story points completed: {total_pts}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="FreshSip Jira CLI — used by the Scrum Master agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # find
    p = sub.add_parser("find", help="Find an issue by CPG ID or summary fragment")
    p.add_argument("identifier")

    # info
    p = sub.add_parser("info", help="Get full details of an issue")
    p.add_argument("key")

    # comment
    p = sub.add_parser("comment", help="Post a comment to an issue")
    p.add_argument("key")
    p.add_argument("message")

    # close
    p = sub.add_parser("close", help="Transition an issue to Done")
    p.add_argument("key")

    # status
    p = sub.add_parser("status", help="Transition an issue to a named status")
    p.add_argument("key")
    p.add_argument("transition", help="Status name fragment (e.g. 'In Progress', 'Done', 'Review')")

    # sprint-list
    sub.add_parser("sprint-list", help="List all sprints on the board")

    # board-list
    sub.add_parser("board-list", help="List all boards")

    # create
    p = sub.add_parser("create", help="Create a new story")
    p.add_argument("--summary",     required=True)
    p.add_argument("--description", default="")
    p.add_argument("--sprint",      default=None, help="Sprint name fragment (e.g. S1, S2)")
    p.add_argument("--points",      type=float, default=None)
    p.add_argument("--priority",    default="Medium")
    p.add_argument("--labels",      default="", help="Comma-separated labels")
    p.add_argument("--epic",        default=None, help="Epic CPG code (e.g. CPG-E01)")
    p.add_argument("--ac",          action="append", help="Acceptance criterion (repeatable)")

    # sprint-status
    p = sub.add_parser("sprint-status", help="Show all issues in a sprint with statuses")
    p.add_argument("sprint", help="Sprint name fragment (e.g. S1, Sprint 1)")

    # done-report
    sub.add_parser("done-report", help="List all Done stories across the project")

    args = parser.parse_args()
    dispatch = {
        "find":          cmd_find,
        "info":          cmd_info,
        "comment":       cmd_comment,
        "close":         cmd_close,
        "status":        cmd_status,
        "sprint-list":   cmd_sprint_list,
        "board-list":    cmd_board_list,
        "create":        cmd_create,
        "sprint-status": cmd_sprint_status,
        "done-report":   cmd_done_report,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
