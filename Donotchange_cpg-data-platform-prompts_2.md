# CPG Soft Drinks Data Platform — Prompt Sequence

## Build a Medallion Architecture Dashboard Using BMad Method + Claude Code Agent Teams + Databricks AI Dev Kit

> **Domain:** Consumer Packaged Goods — Soft Drinks (FreshSip Beverages)
> **Architecture:** Medallion (Bronze → Silver → Gold)
> **Stack:** Databricks (with AI Dev Kit) · GitHub · Jira
> **Methodology:** BMad Method V6 (Breakthrough Method for Agile AI-Driven Development)
> **Multi-Agent:** Claude Code Agent Teams (experimental)
> **Deployment:** Databricks AI Dev Kit (MCP Server + Skills for direct workspace execution)
> **Agents:** Orchestrator · Product Owner · Data Architect · Data Engineer · Deployer

---

## How to Use This Document

This document contains **10 sequential prompts**. Each prompt is a self-contained step.

**Rules:**

1. Run each prompt in a **fresh Claude Code chat** (start a new session for each step — BMad best practice).
2. Copy-paste the prompt **exactly as written** inside the code blocks.
3. Wait for Claude to finish before moving to the next step.
4. Some steps produce artifacts (files). **Do not skip steps** — later prompts depend on earlier outputs.
5. Steps marked with 🤖 use Agent Teams (multiple parallel Claude instances). Steps marked with 👤 are single-session.

**Time estimate:** 3–5 hours total for all 10 steps.

---

## Pre-Requisites (Do These Manually First)

### A. Install Claude Code

```bash
npm install -g @anthropic-ai/claude-code
```

### B. Enable Agent Teams

Add to `~/.claude/settings.json`:

```json
{
  "env": {
    "CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS": "1"
  }
}
```

Or add `export CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` to your shell profile and restart your terminal.

### C. Create GitHub Repository

```bash
mkdir cpg-soft-drinks-platform
cd cpg-soft-drinks-platform
git init
git checkout -b main
gh repo create cpg-soft-drinks-platform --public --source=. --push
```

### D. Set Up Jira

1. Go to https://www.atlassian.com/software/jira/free and create a free account.
2. Create a new Scrum project called **CPG Data Platform** with key `CPG`.
3. Note your Jira domain (e.g., `yourname.atlassian.net`).
4. Create an API token at https://id.atlassian.com/manage-profile/security/api-tokens.
5. Save these — you'll need them in Step 5.
https://naninadhv.atlassian.net?continue=https%3A%2F%2Fnaninadhv.atlassian.net%2Fwelcome%2Fsoftware&atlOrigin=eyJpIjoiZmFlYTQ0MGM2MGE1NDM0Njk1ZWI4NjBkMDk5NmJlMzIiLCJwIjoiaiJ9

### E. Set Up Databricks

1. Go to https://community.cloud.databricks.com/ and register (Community Edition), OR use a full Databricks workspace if you have one.
before that create a uv venv, activate it, and the install using uv pip install databricks-cli
2. Install the Databricks CLI:
   ```bash
   pip install databricks-cli
   ```
3. Configure authentication:
   ```bash
   databricks configure --token
   # Enter your workspace URL and Personal Access Token
   ```
4. Verify it works:
   ```bash
   databricks workspace list /
   ```

### F. Install Databricks AI Dev Kit

This is the critical integration piece. Run from your project root:

```bash
cd cpg-soft-drinks-platform

# Full installation (skills + MCP server)
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)
```

When the installer prompts:
- Select **Claude Code** as the IDE
- Use your **DEFAULT** Databricks CLI profile (or specify the profile name)
- Choose **project scope** (installs into this project only)

This will:
- Install Databricks skills to `.claude/skills/` (patterns for SDP, Jobs, Dashboards, etc.)
- Set up the MCP server in `.claude/mcp.json` (so Claude can execute Databricks operations directly)
- Create a Python virtual environment at `~/.ai-dev-kit/.venv/`

Verify installation:
```bash
# Check skills were installed
ls .claude/skills/ca

# Check MCP config exists
cat .claude/mcp.json
```

You should see skills like `databricks-sdp-writer`, `databricks-bundles`, `databricks-dashboards`, `databricks-jobs`, etc.

### G. Install Node.js v20+ (for BMad Method)

```bash
node -v  # Must show v20 or higher
```

---

## STEP 1 — Install BMad Method 👤

> **What this does:** Installs the BMad Method framework into your project — agents, workflows, slash commands, and the `.bmad-core` folder structure. This coexists with the Databricks AI Dev Kit skills already installed.

**Do this directly in your terminal (not Claude Code):**

```bash
cd cpg-soft-drinks-platform
npx bmad-method install
```

When prompted:
- Select **BMad Method** as the module
- Select **Claude Code** as the IDE
- Accept defaults for everything else

After installation finishes, open Claude Code and verify:

```bash
cd cpg-soft-drinks-platform
claude
```

Then paste this prompt:

```
Verify my project setup by listing:
1. The full directory structure (2 levels deep)
2. All BMad agents available (list files in the bmad agents directory)
3. All Databricks AI Dev Kit skills installed (list files in .claude/skills/)
4. The MCP configuration (show .claude/mcp.json)
5. Run: bmad-help what should I do first?

Confirm that BOTH BMad Method AND Databricks AI Dev Kit are properly installed side by side.
```

---

## STEP 2 — Project Context & CLAUDE.md Setup 👤

> **What this does:** Creates the master project context file and updates CLAUDE.md so all agents understand the CPG domain, Medallion architecture, team structure, AND how to use the Databricks AI Dev Kit for deployment.

Start a **new Claude Code session** and paste:

```
I'm building a CPG (Consumer Packaged Goods) data platform for a fictional soft drinks company called "FreshSip Beverages". I need you to create two critical files that will guide all AI agents throughout this project.

## File 1: _bmad-output/project-context.md

Create this file with the following sections:

### Project Overview
- Company: "FreshSip Beverages" (fictional soft drinks manufacturer)
- Products: Carbonated soft drinks, flavored water, energy drinks, juice blends
- Markets: Operates across 12 US states, 3 distribution channels (Retail, Wholesale, Direct-to-Consumer)
- Goal: Build an end-to-end data platform using Medallion Architecture on Databricks to power a real-time executive dashboard tracking sales, inventory, production, and distribution KPIs

### Technical Stack
- Cloud Platform: Databricks (with AI Dev Kit for deployment)
- Architecture: Medallion (Bronze → Silver → Gold)
- Languages: Python (PySpark), SQL
- Pipeline Framework: Spark Declarative Pipelines (SDP) — the Databricks AI Dev Kit has skills for this
- Orchestration: Databricks Workflows/Jobs — the AI Dev Kit MCP server can create these directly
- Dashboards: Databricks AI/BI Dashboards — the AI Dev Kit MCP server can create these directly
- Version Control: GitHub
- Project Management: Jira (Scrum, project key: CPG)
- CI/CD: GitHub Actions + Databricks Asset Bundles (DABs)
- Deployment: Via Databricks AI Dev Kit MCP Server (Claude can execute Databricks operations directly)

### Deployment Architecture (Databricks AI Dev Kit)
- The project uses the Databricks AI Dev Kit which provides:
  - **Skills** (.claude/skills/): Teach Claude Databricks patterns (SDP, Jobs, Dashboards, Unity Catalog)
  - **MCP Server** (.claude/mcp.json): Lets Claude execute Databricks operations directly — create pipelines, run jobs, build dashboards, manage catalogs
- Deployment flow: Code is written locally → pushed to GitHub → deployed to Databricks via MCP tools or DABs
- Claude can directly: create/update SDP pipelines, schedule jobs, build AI/BI dashboards, query tables, manage Unity Catalog schemas
- For the Community Edition: some features (Unity Catalog, SDP) may be limited — fall back to notebooks + Hive metastore

### Architecture Decisions
- Bronze Layer: Raw ingestion from CSV/JSON sources (simulating ERP, POS, IoT sensors). Schema-on-read. Append-only. No transformations. Use Auto Loader pattern if SDP available, else notebook-based ingestion.
- Silver Layer: Cleaned, validated, deduplicated, standardized. Schema-on-write. Delta Lake format. SCD Type 2 for dimensional data.
- Gold Layer: Business-level aggregations, star schema, pre-computed KPIs. Optimized for dashboard queries.
- Catalog: Unity Catalog if available, otherwise Hive metastore with database-per-layer (brz_freshsip, slv_freshsip, gld_freshsip)

### Data Domains
1. Sales: POS transactions, order lines, returns, promotions
2. Inventory: Warehouse stock levels, reorder points, shelf life tracking
3. Production: Batch records, quality checks, yield rates, downtime events
4. Distribution: Shipments, delivery routes, fulfillment rates, logistics costs
5. Customers: Retailer profiles, segments, credit terms, satisfaction scores
6. Products: SKU master, formulations, packaging, pricing tiers

### Team Structure (Agent Roles)
- Orchestrator: Oversees handovers, status updates, task assignment, result validation, artifact archiving
- Product Owner: Creates BRDs from business requirements, defines KPIs, acceptance criteria
- Data Architect: Designs data models, defines schemas per layer, creates architecture diagrams, specifies data quality rules
- Data Engineer: Builds pipelines (ingestion, transformation, validation), writes PySpark/SQL code, deploys to Databricks using AI Dev Kit, creates workflows and dashboards, fixes bugs
- Code Reviewer: Reviews all code against quality standards before deployment
- Deployer: Uses Databricks AI Dev Kit MCP tools to deploy pipelines, create jobs, build dashboards

### Quality Standards
- All code must include docstrings and inline comments
- Every pipeline must have data quality checks (null checks, range validation, referential integrity)
- Naming convention: snake_case for tables/columns, prefixed by layer (brz_, slv_, gld_)
- All changes via Pull Requests with code review
- Unit tests for all transformation logic
- Cost optimization: minimize shuffles, use partition pruning, cache strategically

### KPI Definitions (Gold Layer Targets)
1. Daily Revenue by Product Category and Region
2. Inventory Turnover Rate by Warehouse
3. Production Yield Rate by Batch
4. Order Fulfillment Rate by Distribution Channel
5. Customer Acquisition Cost by Segment
6. Gross Margin by SKU
7. Days Sales of Inventory (DSI)
8. On-Time Delivery Percentage

## File 2: CLAUDE.md (update the existing one)

Append to the existing CLAUDE.md (preserve whatever BMad already put there) with this project-specific section:

## CPG Data Platform — Agent Instructions

### Domain Context
This is a data engineering project for FreshSip Beverages (soft drinks CPG company). All agents must read _bmad-output/project-context.md before starting any task.

### Architecture
Medallion Architecture: Bronze (raw) → Silver (clean) → Gold (aggregated)
Target platform: Databricks. All code must be PySpark or Databricks SQL compatible.

### Databricks AI Dev Kit Integration
This project uses the Databricks AI Dev Kit. Key details:
- Skills are in .claude/skills/ — read relevant skills before writing Databricks-specific code
- MCP Server is configured in .claude/mcp.json — use MCP tools to execute Databricks operations directly
- Always prefer Databricks-native patterns taught by the skills over generic PySpark
- Deployment workflow: Write code locally → test locally → deploy to Databricks via MCP tools or push to GitHub for DABs deployment
- For Community Edition limitations: fall back to notebook-based approaches when SDP or Unity Catalog is unavailable

### Agent Team Structure
When using Agent Teams, the following roles apply:
- Team Lead (Orchestrator): Coordinates work, validates outputs, manages handovers. Does NOT write pipeline code.
- Product Owner Teammate: Focuses on BRDs, KPIs, acceptance criteria. Reads from _bmad-output/ artifacts.
- Architect Teammate: Designs schemas, data models, architecture docs. Writes to _bmad-output/architecture/.
- Data Engineer Teammate: Writes pipeline code, tests, deployment configs. Works in src/ and tests/. Uses Databricks AI Dev Kit skills.
- Deployer Teammate: Uses Databricks MCP tools to deploy code, create jobs, build dashboards.

### Subagent Definitions
Subagents are defined in .claude/agents/ for reuse across teams and solo sessions:
- product-owner: BRD and KPI specialist
- data-architect: Schema and model designer
- data-engineer: Pipeline builder (uses AI Dev Kit skills)
- code-reviewer: Reviews PRs for quality standards
- deployer: Deploys to Databricks via MCP tools

### File Organization
cpg-soft-drinks-platform/
├── CLAUDE.md
├── _bmad-output/              # BMad artifacts
│   ├── project-context.md
│   ├── requirements/
│   ├── architecture/
│   └── stories/
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── utils/
│   └── dashboard/
├── tests/
│   ├── unit/
│   └── integration/
├── data/
│   └── synthetic/
├── config/
│   ├── databricks/
│   └── schemas/
├── notebooks/                 # Databricks notebook exports
├── .claude/
│   ├── agents/
│   ├── skills/                # Databricks AI Dev Kit skills
│   └── mcp.json               # Databricks MCP server config
├── .github/
│   └── workflows/
└── docs/

### Commit Convention
All commits: [LAYER] type: description
Examples: [BRONZE] feat: add POS transaction ingestion, [GOLD] fix: correct revenue aggregation logic

Now create both files. For CLAUDE.md, preserve whatever BMad already put there and append the project-specific section. Create all necessary directories.
```

---

## STEP 3 — Define Subagents 👤

> **What this does:** Creates reusable subagent definitions in `.claude/agents/`. These serve as both standalone subagents AND Agent Team teammates. The Data Engineer and Deployer agents are specifically instructed to use Databricks AI Dev Kit.

Start a **new Claude Code session** and paste:

```
Create subagent definition files for Claude Code in .claude/agents/. These are used both as standalone subagents AND as Agent Team teammates.

Read _bmad-output/project-context.md for full project context first.

Create these 5 files:

## 1. .claude/agents/product-owner.md

Role: Product Owner for FreshSip Beverages data platform.
Responsibilities: Read raw business requirements → create BRDs, define KPIs with formulas/data sources/granularity/targets, write acceptance criteria, prioritize with MoSCoW, create user stories.
Standards: BRDs must include Executive Summary, Business Objectives, Scope, Requirements, KPI Definitions, Acceptance Criteria, Dependencies, Assumptions, Out of Scope. KPIs must specify Name, Formula, Data Source tables/columns, Granularity, Target, Alert Threshold. Never make architecture decisions.
Artifacts: _bmad-output/requirements/BRD-{number}-{name}.md, kpi-definitions.md, user-stories.md
Context: Always read project-context.md and existing BRDs first.

## 2. .claude/agents/data-architect.md

Role: Data Architect for FreshSip Beverages.
Responsibilities: Translate BRDs into data models, design Bronze/Silver/Gold schemas, define data quality rules per layer, create ER diagrams (Mermaid), specify SCD strategies and partitioning, design Gold star schema, document lineage.
Standards: Bronze = schema-on-read, append-only, metadata columns. Silver = schema-on-write, Delta, SCD Type 2, surrogate keys. Gold = star schema, pre-aggregated. Naming: brz_/slv_/gld_ prefixes. Every table needs PK, audit columns, partition key. Quality rules: rule_name, type, column, expression, severity.
Artifacts: architecture-overview.md, schema-bronze/silver/gold.md, data-quality-rules.md, data-lineage.md, config/schemas/*.json
Context: Always read project-context.md and all BRDs first.

## 3. .claude/agents/data-engineer.md

Role: Data Engineer for FreshSip Beverages. Builds pipelines and deploys to Databricks.
CRITICAL: Before writing ANY Databricks code, read the relevant AI Dev Kit skills in .claude/skills/ (SDP, Jobs, Dashboards, Python SDK, Unity Catalog). Always prefer skill-taught patterns over generic PySpark.
Responsibilities: Build ingestion (Bronze), transformation (Silver), aggregation (Gold) pipelines. Implement quality checks. Write unit tests. Use Databricks AI Dev Kit patterns. Create notebook exports.
Standards: Use SDP patterns where available. Docstrings on every function. Project structure: src/bronze/silver/gold/utils/. Every pipeline needs logging, error handling, idempotency, quality checks. Delta merge for upserts. Config-driven (no hardcoded values). 3+ test cases per function.
Pipeline template: Read AI Dev Kit skill → imports + logger → config loading → quality functions → transform functions → main pipeline (read→validate→transform→validate→write) → entry point with error handling.
Artifacts: src/bronze|silver|gold/{domain}.py, src/utils/*.py, src/dashboard/*.sql, tests/unit/*.py, notebooks/*.py
Context: Read project-context.md, architecture docs, AI Dev Kit skills, assigned story, existing code.

## 4. .claude/agents/code-reviewer.md

Role: Code reviewer. Checks: correctness vs spec, Databricks patterns (uses AI Dev Kit skills?), quality checks present, error handling, performance (no unnecessary shuffles/collect), idempotency, test coverage, docstrings, naming conventions, no hardcoded values, no credentials in code.
Output: Per file ✅/❌ with line refs. Severity: 🔴 Blocker / 🟡 Warning / 🔵 Suggestion. Concrete fix per issue.
Context: project-context.md + architecture docs.

## 5. .claude/agents/deployer.md

Role: Deployment agent. Uses Databricks AI Dev Kit MCP tools (configured in .claude/mcp.json) to deploy directly to the workspace.
CRITICAL: Read AI Dev Kit skills (Jobs, Dashboards, SDP) before any deployment.
Responsibilities: Create databases/schemas via MCP, convert scripts to notebook format, upload to workspace, create Databricks Jobs/Workflows via MCP, build AI/BI dashboards via MCP, validate post-deployment, document rollback procedures.
Deployment sequence: Schema setup → data upload → Bronze deploy → Silver deploy → Gold deploy → Jobs → Dashboards → smoke test.
Fallback: If MCP operations fail (Community Edition), document the manual alternative steps.
Artifacts: config/databricks/*.json, notebooks/*.py, docs/deployment/*.md
Context: project-context.md, architecture docs, AI Dev Kit skills, src/ code.

Create all 5 files with comprehensive, well-structured markdown. Each file should be detailed enough that a subagent reading it knows exactly what to do, what standards to follow, and what to produce.
```

---

## STEP 4 — BMad Phase 1: Discovery & Product Brief 👤

> **What this does:** Uses BMad's Phase 1 to explore the problem space and generate a product brief.

Start a **new Claude Code session** and paste:

```
I want to run BMad Phase 1 (Discovery/Analysis) for my CPG soft drinks data platform.

Read _bmad-output/project-context.md first.

Here is the raw business requirement from the CEO of FreshSip Beverages:

---
BUSINESS REQUIREMENT (from CEO):

"We're flying blind. I need a dashboard that tells me, in real-time or near-real-time, how our business is performing. Specifically:

1. SALES: What are we selling, where, and how much revenue? By product category (carbonated, water, energy, juice), region, and channel (retail, wholesale, DTC). Daily trends, compare to last month/year.

2. INVENTORY: Which warehouses are overstocked or running low? Inventory turnover rate, days of supply per product. Alerts when below reorder point.

3. PRODUCTION: Yield rate per batch? Downtime? Quality check pass/fail rates. Batch traceability for failures.

4. DISTRIBUTION: On-time delivery? Fulfillment rate? Costliest routes? Cost per case delivered by region.

5. CUSTOMERS: Top 20 retailers by revenue? Customer acquisition cost by segment? Retention trends?

Data sources: ERP/SAP (CSV daily), POS from retailers (JSON hourly), IoT production sensors (streaming JSON), logistics partner API (CSV daily).

Budget is tight — use free Databricks tier. 4-week timeline. Team is me + AI agents. This must be a board presentation showcase."
---

Now:
1. Run brainstorming (bmad-brainstorming) — explore problem space, identify risks, hidden requirements, opportunities.
2. Create a Product Brief: opportunity, target users (CEO, VP Sales, VP Ops, Supply Chain Manager), key metrics, technical constraints, risks, success criteria.
3. Save all outputs to _bmad-output/ following BMad conventions.
4. Run bmad-help to tell me what to do next.
```

---

## STEP 5 — BMad Phase 2: PRD + KPIs + Jira Plan 🤖

> **What this does:** Agent Teams parallelize Phase 2. Product Owner creates the PRD while another teammate builds the Jira structure.

Start a **new Claude Code session** and paste:

```
BMad Phase 2 (Planning). I need a detailed PRD and Jira project plan.

Read all context: _bmad-output/project-context.md and everything in _bmad-output/ from Phase 1.

Create an agent team with 2 teammates:

Teammate 1 — Product Owner (use product-owner agent type from .claude/agents/product-owner.md):
- Run BMad create-prd workflow (bmad-create-prd)
- Input: Phase 1 product brief + CEO requirement
- PRD must cover all 5 data domains
- Include detailed KPI definitions with exact formulas, specifying Gold layer tables/columns
- Define user stories per domain
- Save PRD to _bmad-output/ per BMad conventions
- Create KPI registry at _bmad-output/requirements/kpi-definitions.md with per-KPI: Name, Business Question, Formula (SQL), Source Tables, Granularity, Target, Alert Threshold, Dashboard Widget Type

Teammate 2 — Jira Project Planner:
- Create docs/jira-setup.md containing:
  - Epics: one per data domain + Infrastructure + Dashboard + Deployment
  - User stories per epic (coordinate with Teammate 1 for alignment)
  - Story points (Fibonacci: 1,2,3,5,8,13)
  - 4 one-week sprints:
    - Sprint 1: Infrastructure + Databricks setup + schemas + synthetic data + Bronze layer
    - Sprint 2: Silver layer + quality framework
    - Sprint 3: Gold layer + star schema + KPIs
    - Sprint 4: Dashboards + testing + optimization + deployment + docs
  - Each story as a Jira-ready ticket: Title, Description, Acceptance Criteria, Story Points, Sprint, Labels, Dependencies

Team Lead: Coordinate between teammates (Teammate 2 needs stories from Teammate 1). Validate PRD covers all CEO requirements. Ensure KPI formulas are precise. Status report when done.
```

---

## STEP 6 — BMad Phase 3: Architecture & Data Models 🤖

> **What this does:** Agent Teams create the full technical architecture. Architect designs schemas while Product Owner validates requirement coverage.

Start a **new Claude Code session** and paste:

```
BMad Phase 3 (Solutioning). Complete data architecture design needed.

Read: _bmad-output/project-context.md, all requirements in _bmad-output/requirements/, and Databricks AI Dev Kit skills in .claude/skills/ (especially SDP, Jobs, Dashboards).

Run BMad architecture workflow (bmad-create-architecture).

Create an agent team with 2 teammates:

Teammate 1 — Data Architect (use data-architect agent type):

Read Databricks AI Dev Kit skills first. Then design:

1. _bmad-output/architecture/architecture-overview.md — High-level Mermaid diagram (Sources→Bronze→Silver→Gold→Dashboard), tech choices with rationale, Databricks topology, Community Edition fallbacks

2. _bmad-output/architecture/schema-bronze.md — One table per source per domain. Columns: all source + _ingested_at, _source_file, _batch_id. Partitioned by ingestion_date. Delta Lake. Include CREATE TABLE DDLs.

3. _bmad-output/architecture/schema-silver.md — Cleaned, typed, deduped tables. SCD Type 2 for products, customers, warehouses (surrogate_key, valid_from, valid_to, is_current). 5+ quality rules per table. Include DDLs.

4. _bmad-output/architecture/schema-gold.md — Star schema:
   - Facts: fact_sales, fact_inventory_snapshot, fact_production_batch, fact_shipment, fact_customer_metrics
   - Dims: dim_product, dim_customer, dim_warehouse, dim_date, dim_region, dim_channel
   - KPI tables: kpi_daily_revenue, kpi_inventory_turnover, kpi_production_yield, kpi_fulfillment_rate, kpi_customer_metrics
   - Include exact SQL/PySpark computation logic per KPI. Include DDLs.

5. _bmad-output/architecture/data-quality-rules.md — By layer and table. Format: rule_id|table|column|rule_type|expression|severity|description. Min 5 per Silver, 3 per Gold table.

6. _bmad-output/architecture/data-lineage.md — Mermaid diagrams, column-level lineage Bronze→Silver→Gold, one per domain.

7. config/schemas/ — JSON Schema files for Bronze layer source validation.

Teammate 2 — Product Owner (use product-owner agent type):
- Validate every KPI from PRD is computable from Gold tables
- Verify all data domains covered
- Create traceability matrix: _bmad-output/architecture/traceability-matrix.md (requirement → architecture component → Gold table → KPI)
- Flag gaps to Architect

Team Lead: Ensure Architect reads requirements AND Databricks skills first. Route validation issues. After both finish, run bmad-solutioning-gate-check. Verify implementability on Databricks. Readiness report.
```

---

## STEP 7 — Synthetic Data Generation 👤

> **What this does:** Creates realistic synthetic CSV/JSON data simulating 90 days of FreshSip operations.

Start a **new Claude Code session** and paste:

```
Generate synthetic data for FreshSip Beverages data platform.

Read: _bmad-output/architecture/schema-bronze.md, architecture-overview.md, _bmad-output/requirements/kpi-definitions.md

Generate files in data/synthetic/ simulating 90 days (2025-10-01 to 2025-12-31). Data must be internally consistent (FK matches, plausible quantities).

### ERP Data (CSV):
- data/synthetic/erp/products.csv — 50 SKUs: Carbonated(20), Water(10), Energy(12), Juice(8). Cols: sku_id, product_name, category, sub_category, unit_price, cost_price, pack_size, shelf_life_days, launch_date, status
- data/synthetic/erp/customers.csv — 200 retailers. Cols: customer_id, name, segment(Enterprise/Mid-Market/SMB), region(NE/SE/MW/W), channel(Retail/Wholesale/DTC), credit_terms_days, acquisition_date, status
- data/synthetic/erp/warehouses.csv — 8 warehouses, 4 regions. Cols: warehouse_id, name, region, capacity_cases, address, manager
- data/synthetic/erp/orders.csv — ~15,000 orders. Cols: order_id, customer_id, order_date, ship_date, delivery_date, status, total_amount
- data/synthetic/erp/order_lines.csv — ~45,000 lines. Cols: order_line_id, order_id, sku_id, quantity, unit_price, discount_pct, line_total
- data/synthetic/erp/inventory_daily.csv — Daily snapshots (~36,000 rows). Cols: snapshot_date, warehouse_id, sku_id, quantity_on_hand, reorder_point, quantity_on_order

### POS Data (JSON):
- data/synthetic/pos/pos_transactions.json — ~50,000 transactions as NDJSON. Each: {transaction_id, store_id, customer_id, timestamp, items:[{sku_id,qty,price,discount}], payment_method, total}

### Production Data (JSON):
- data/synthetic/production/batches.json — ~500 batches. Each: {batch_id, sku_id, production_line, start_time, end_time, target_quantity, actual_quantity, yield_rate, status}
- data/synthetic/production/quality_checks.json — ~2,000 checks. Each: {check_id, batch_id, check_type, value, min_threshold, max_threshold, result, timestamp, inspector_id}
- data/synthetic/production/downtime_events.json — ~100 events. Each: {event_id, production_line, start_time, end_time, reason, category}

### Logistics Data (CSV):
- data/synthetic/logistics/shipments.csv — ~3,000 shipments. Cols: shipment_id, order_id, warehouse_id, carrier, ship_date, estimated_delivery, actual_delivery, cases_shipped, freight_cost, status

### Realistic Dirty Data (for testing quality checks):
- ~2% nulls in non-key fields, ~1% duplicate order_ids, ~5 future-dated orders, some negative prices
- Total revenue ~$15-25M over 90 days
- Seasonal patterns: higher inventory in Oct, peak Dec
- Production yield avg 94-97%, some batches down to 85%
- On-time delivery ~92%

Also create:
- data/synthetic/DATA_DICTIONARY.md — all files, schemas, row counts, relationships
- data/synthetic/generate_data.py — reproducible generator (seed=42, stdlib only, no pandas)
```

---

## STEP 8 — BMad Phase 4: Build Pipelines 🤖

> **What this does:** Agent Teams build ALL pipeline code in parallel — one teammate per layer. Data Engineer agents use Databricks AI Dev Kit patterns.

Start a **new Claude Code session** and paste:

```
BMad Phase 4 (Implementation). Build all pipeline code.

Read: _bmad-output/project-context.md, _bmad-output/architecture/ (ALL docs), _bmad-output/requirements/kpi-definitions.md, data/synthetic/DATA_DICTIONARY.md, .claude/skills/ (READ Databricks AI Dev Kit skills: SDP, Jobs, Python SDK)

Create an agent team with 4 teammates. All use data-engineer agent type from .claude/agents/data-engineer.md.

Teammate 1 — Utilities & Bronze:
Build shared modules first:
1. src/utils/logger.py — Structured logging with context (layer, domain, batch_id)
2. src/utils/config_loader.py — Load config from config/*.yaml
3. src/utils/quality_checks.py — Reusable framework: check_not_null, check_unique, check_range, check_referential_integrity, check_custom. A run_quality_checks(df, rules) that returns pass/fail report. Error severity = fail pipeline, warning = log and continue.
4. src/bronze/sales_ingestion.py — Ingest orders, order_lines, pos_transactions
5. src/bronze/inventory_ingestion.py — Ingest inventory_daily
6. src/bronze/production_ingestion.py — Ingest batches, quality_checks, downtime
7. src/bronze/distribution_ingestion.py — Ingest shipments
8. src/bronze/master_data_ingestion.py — Ingest products, customers, warehouses
9. config/pipeline_config.yaml — All paths, table names, thresholds

Every ingestion: read source → add _ingested_at, _source_file, _batch_id → write Delta. Use AI Dev Kit SDP patterns if applicable.

Teammate 2 — Silver Layer (wait for Teammate 1 utils):
1. src/silver/sales_transform.py — Clean, dedupe, join orders+lines+POS, validate
2. src/silver/inventory_transform.py — Clean snapshots, calc days_of_supply
3. src/silver/production_transform.py — Clean batches, join quality checks, calc yield
4. src/silver/distribution_transform.py — Clean shipments, calc on_time flag
5. src/silver/master_data_transform.py — SCD Type 2 for products, customers, warehouses

Each: read Bronze → quality checks → transform → quality checks → write Delta.

Teammate 3 — Gold Layer + Dashboard Queries (wait for Teammate 2 schemas):
1. src/gold/dim_date.py — Full date dimension for 2025
2. src/gold/fact_sales.py — Revenue, quantity, discount metrics
3. src/gold/fact_inventory_snapshot.py — Daily inventory position
4. src/gold/fact_production_batch.py — Yield, quality, downtime
5. src/gold/fact_shipment.py — Delivery performance
6. src/gold/kpi_daily_revenue.py — Revenue by category, region, channel
7. src/gold/kpi_inventory_turnover.py — Turnover rate by warehouse
8. src/gold/kpi_production_yield.py — Yield by batch, line, period
9. src/gold/kpi_fulfillment_rate.py — Fulfillment and on-time delivery
10. src/dashboard/*.sql — One SQL query per dashboard KPI widget

Teammate 4 — Unit Tests (parallel):
1. tests/unit/test_quality_checks.py
2. tests/unit/test_bronze_ingestion.py
3. tests/unit/test_silver_transforms.py
4. tests/unit/test_gold_aggregations.py
5. tests/conftest.py — Shared fixtures (sample DataFrames, SparkSession)
Each: happy path + edge cases (empty, nulls) + boundary conditions. Use pytest.

Team Lead: Coordinate deps (Utils→Bronze→Silver→Gold, Tests parallel). Ensure AI Dev Kit patterns used. After all finish, run code review via code-reviewer agent. Verify tests pass. Completion report: files created, test results, issues.
```

---

## STEP 9 — Deploy to Databricks 🤖

> **What this does:** Agent Teams deploy everything to Databricks using the AI Dev Kit MCP server. One teammate handles pipelines/jobs, another builds the dashboard.

Start a **new Claude Code session** and paste:

```
Deploy everything to Databricks.

Read: _bmad-output/project-context.md, _bmad-output/architecture/, all code in src/, .claude/skills/ (Jobs, Dashboards, SDP skills), .claude/mcp.json

Create an agent team with 2 teammates:

Teammate 1 — Pipeline Deployer (use deployer agent type):

Step 1 — Catalog Setup: Use MCP tools to create databases brz_freshsip, slv_freshsip, gld_freshsip. If Unity Catalog available, create catalog freshsip with schemas bronze/silver/gold.

Step 2 — Upload Data: Upload data/synthetic/ to Databricks DBFS at /FileStore/freshsip/raw/ via MCP or CLI.

Step 3 — Convert & Upload Pipelines: Convert src/*.py to Databricks notebook format (add "# Databricks notebook source" and "# COMMAND ----------" markers). Save to notebooks/. Upload to workspace at /Workspace/Users/{email}/freshsip/ via MCP.

Step 4 — Create Workflow: Create a Databricks Workflow job with tasks in dependency order:
- Tasks 1-5: Bronze ingestion (master_data first, rest parallel)
- Tasks 6-10: Silver transforms (master_data first, rest depend on corresponding Bronze + master Silver)
- Task 11: dim_date (Gold, independent)
- Tasks 12-15: Facts (Gold, depend on corresponding Silver + dim_date)
- Task 16: All KPI aggregations (depend on facts)
Use Databricks Jobs skill patterns. Save config to config/databricks/. Schedule daily 6AM UTC.
If MCP creation fails, save JSON and document manual steps.

Step 5 — Initial Run: Trigger workflow, monitor, verify tables created and contain data.

Step 6 — Documentation: docs/deployment/deployment-checklist.md + docs/deployment/runbook.md (re-run, rollback, add domains). Note Community Edition limitations.

Teammate 2 — Dashboard Builder:

Step 1: Read Databricks Dashboards AI Dev Kit skill
Step 2: Read KPI definitions and Gold schema
Step 3: Create/verify dashboard SQL queries in src/dashboard/
Step 4: Use MCP tools to create AI/BI Dashboard "FreshSip Executive Dashboard":
- Header: Company name, refresh time, date selector
- Row 1: KPI cards (Revenue MTD, Inventory Turnover, Yield, On-Time %)
- Row 2: Revenue trend line (daily, by category) + Revenue by region bar
- Row 3: Inventory heatmap (warehouse × category) + Days of Supply gauge
- Row 4: Production yield by line + Quality pass rate trend
- Row 5: Fulfillment rate trend + Top 10 customers table
Step 5: If MCP dashboard creation fails, create definition + manual README
Step 6: Save to config/databricks/dashboard_definition.json

Team Lead: Dashboard SQL prep can start while pipelines deploy but needs tables for testing. If MCP ops fail, document manual alternatives. After both finish, verify end-to-end: source→Bronze→Silver→Gold→Dashboard. Deployment status report.
```

---

## STEP 10 — Final Review, Git Push & Documentation 👤

> **What this does:** Final quality check, documentation, git commit, and wrap-up summary.

Start a **new Claude Code session** and paste:

```
Final step! Comprehensive review, then commit and push to GitHub.

Read the full project: _bmad-output/, src/, tests/, config/, docs/, data/synthetic/

## 1. Code Review
Spawn a subagent using the code-reviewer agent type. Review all files in src/. Produce docs/code-review-report.md. Fix any 🔴 Blocker issues directly.

## 2. Documentation
Create/update:
- README.md — Professional: overview, architecture diagram (Mermaid), setup instructions (including Databricks AI Dev Kit install), how to run, KPIs, project structure, tech stack
- docs/architecture-decision-records.md — ADRs: why Medallion, why Delta Lake, why SCD Type 2, SDP vs notebooks, why AI Dev Kit
- docs/runbook.md — Operations: daily ops, monitoring, troubleshooting, adding domains, adding KPIs, Databricks-specific operations

## 3. Project Summary
Create docs/project-summary.md:
- What was built (executive summary)
- Architecture overview
- All artifacts (list every file with purpose)
- KPIs implemented with formulas
- Deployment status (what's on Databricks, what needs manual setup)
- Known limitations (Community Edition constraints)
- Future enhancements (streaming, ML, alerting, full Unity Catalog)

## 4. Git Commit & Push
```bash
git add _bmad-output/ && git commit -m "[DOCS] feat: add BMad planning artifacts"
git add src/utils/ && git commit -m "[UTILS] feat: add shared utilities"
git add src/bronze/ && git commit -m "[BRONZE] feat: add ingestion pipelines"
git add src/silver/ && git commit -m "[SILVER] feat: add transformation pipelines"
git add src/gold/ && git commit -m "[GOLD] feat: add aggregations and KPIs"
git add src/dashboard/ && git commit -m "[DASHBOARD] feat: add dashboard SQL queries"
git add tests/ && git commit -m "[TESTS] feat: add unit tests"
git add data/synthetic/ && git commit -m "[DATA] feat: add synthetic data generator"
git add config/ notebooks/ && git commit -m "[CONFIG] feat: add configs and notebook exports"
git add docs/ README.md && git commit -m "[DOCS] feat: add documentation and runbook"
git add .claude/ && git commit -m "[INFRA] feat: add agent definitions and AI Dev Kit config"
git push origin main
```

## 5. Final Status Table
| Phase | Status | Key Artifacts |
|-------|--------|---------------|
| Phase 1: Discovery | ✅/❌ | files... |
| Phase 2: Planning | ✅/❌ | files... |
| Phase 3: Architecture | ✅/❌ | files... |
| Phase 4: Implementation | ✅/❌ | files... |
| Deployment | ✅/❌ | files... |
| Documentation | ✅/❌ | files... |
```

---

## Quick Reference

| Step | Phase | Key Outputs | Mode |
|------|-------|-------------|------|
| 1 | Setup | BMad framework + AI Dev Kit skills | 👤 Terminal |
| 2 | Setup | project-context.md, CLAUDE.md | 👤 Solo |
| 3 | Setup | 5 subagent definitions | 👤 Solo |
| 4 | Phase 1 | Product brief, brainstorm | 👤 Solo |
| 5 | Phase 2 | PRD, KPI registry, Jira plan | 🤖 2 teammates |
| 6 | Phase 3 | Full architecture, schemas, lineage | 🤖 2 teammates |
| 7 | Data | Synthetic data (6 domains, 90 days) | 👤 Solo |
| 8 | Phase 4 | All pipeline code + tests | 🤖 4 teammates |
| 9 | Deploy | Databricks deployment + dashboard | 🤖 2 teammates |
| 10 | Wrap-up | Code review, docs, git push | 👤 Solo |

---

## Troubleshooting

### BMad commands not recognized
- Verify `npx bmad-method install` ran in Step 1
- Try `bmad-help` — should respond with guidance
- If commands fail, follow workflows manually by creating described artifacts

### Databricks AI Dev Kit MCP not working
- Check `.claude/mcp.json` exists with databricks server config
- Verify: `databricks workspace list /`
- Re-run installer: `bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)`
- If MCP fails, fall back to: `databricks workspace import` CLI, Jobs via UI, Dashboards via SQL UI

### Agent Teams not spawning
- Verify: `echo $CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS` → must print `1`
- Claude Code version must be February 2026+: `claude --version`
- Fallback: run each teammate's task as a separate solo prompt

### Community Edition limitations
- No Unity Catalog → Hive metastore with brz_/slv_/gld_ database prefixes
- No SDP → standard notebook pipelines
- No Jobs API → create jobs in UI manually
- Limited compute → run pipelines sequentially
- No AI/BI Dashboards → SQL queries + manual visualization

### Token usage
- Agent Teams use 3-7x more tokens than solo sessions
- Budget option: replace 🤖 steps with sequential 👤 solo prompts (run each teammate's task separately)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    FreshSip Beverages                            │
│                  CPG Data Platform                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ ERP/SAP  │  │ POS JSON │  │ IoT JSON │  │ Logistics│       │
│  │  (CSV)   │  │ (hourly) │  │(streaming)│  │  (CSV)   │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
│       │              │              │              │              │
│  ═════╪══════════════╪══════════════╪══════════════╪══════════   │
│       ▼              ▼              ▼              ▼              │
│  ┌──────────────────────────────────────────────────────┐       │
│  │  BRONZE (Raw Ingestion)                              │       │
│  │  brz_sales · brz_inventory · brz_production · brz_dist│      │
│  │  Schema-on-read · Append-only · Delta Lake            │       │
│  └──────────────────────┬───────────────────────────────┘       │
│                         ▼                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │  SILVER (Clean & Validate)                           │       │
│  │  slv_sales · slv_inventory · slv_production · slv_dist│      │
│  │  SCD Type 2 · Deduped · Quality Checked · Delta Lake  │       │
│  └──────────────────────┬───────────────────────────────┘       │
│                         ▼                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │  GOLD (Business Aggregations)                        │       │
│  │  Star Schema: fact_sales · dim_product · dim_customer │       │
│  │  KPI Tables: kpi_revenue · kpi_inventory · kpi_yield  │       │
│  └──────────────────────┬───────────────────────────────┘       │
│                         ▼                                        │
│  ┌──────────────────────────────────────────────────────┐       │
│  │  DASHBOARD (Databricks AI/BI)                        │       │
│  │  Revenue Trends · Inventory Heatmap · Yield Rates     │       │
│  │  Fulfillment KPIs · Customer Rankings                 │       │
│  └──────────────────────────────────────────────────────┘       │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│  METHODOLOGY: BMad Method V6 (4 Phases)                         │
│  AGENTS: Claude Code Agent Teams (Orchestrator + Specialists)   │
│  DEPLOYMENT: Databricks AI Dev Kit (Skills + MCP Server)        │
│  VERSION CONTROL: GitHub · PROJECT MGMT: Jira                   │
└─────────────────────────────────────────────────────────────────┘
```

---

*Generated for CPG Soft Drinks Data Platform project. April 2026.*
