# CLAUDE.md — FreshSip Beverages CPG Data Platform

---

## CPG Data Platform — Agent Instructions

### Domain Context

This is a data engineering project for **FreshSip Beverages** (fictional soft drinks CPG company).

**All agents must read `_bmad-output/project-context.md` before starting any task.**

That file contains the authoritative source for: company background, technical stack, architecture decisions, data domains, team roles, quality standards, and KPI definitions.

---

### Architecture

```
Bronze (raw, append-only) → Silver (clean, validated) → Gold (aggregated, KPI-ready)
```

- Target platform: **Databricks**
- All code must be **PySpark** or **Databricks SQL** compatible
- Delta Lake format for all Silver and Gold tables
- Naming: `brz_<entity>`, `slv_<entity>`, `gld_<kpi>` (snake_case throughout)

---

### Databricks AI Dev Kit Integration

This project uses the **Databricks AI Dev Kit**. Every agent writing or deploying Databricks code must follow this workflow:

1. **Read the relevant skill** in `.claude/skills/` before writing Databricks-specific code
2. **Use MCP tools** (configured in `.claude/mcp.json`) to execute Databricks operations directly — do not simulate deployments
3. **Prefer Databricks-native patterns** taught by the skills over generic PySpark/vanilla Spark
4. **Deployment flow:** Write code locally → test locally → deploy to Databricks via MCP tools or push to GitHub for DABs deployment

#### Community Edition Limitations
If Unity Catalog or SDP are unavailable, fall back to:
- Notebook-based ingestion (instead of Auto Loader / SDP)
- Hive metastore with databases: `brz_freshsip`, `slv_freshsip`, `gld_freshsip`

---

### Agent Team Structure

When using Agent Teams, the following roles apply:

| Role | Responsibilities |
|---|---|
| **Team Lead (Orchestrator)** | Coordinates work, validates outputs, manages handovers. Does NOT write pipeline code. |
| **Product Owner Teammate** | Focuses on BRDs, KPIs, acceptance criteria. Reads from `_bmad-output/` artifacts. |
| **Architect Teammate** | Designs schemas, data models, architecture docs. Writes to `_bmad-output/architecture/`. |
| **Data Engineer Teammate** | Writes pipeline code, tests, deployment configs. Works in `src/` and `tests/`. Uses Databricks AI Dev Kit skills. |
| **Deployer Teammate** | Uses Databricks MCP tools to deploy code, create jobs, build dashboards. |

---

### Subagent Definitions

Subagents are defined in `.claude/agents/` for reuse across teams and solo sessions:

| Agent | Role |
|---|---|
| `product-owner` | BRD and KPI specialist |
| `data-architect` | Schema and model designer |
| `data-engineer` | Pipeline builder (uses AI Dev Kit skills) |
| `code-reviewer` | Reviews PRs for quality standards |
| `deployer` | Deploys to Databricks via MCP tools |

---

### File Organization

```
cpg-soft-drinks-platform/
├── CLAUDE.md
├── _bmad-output/              # BMad artifacts
│   ├── project-context.md     # ← Read this first
│   ├── requirements/
│   ├── architecture/
│   └── stories/
├── src/
│   ├── bronze/                # Raw ingestion pipelines
│   ├── silver/                # Cleaning & validation pipelines
│   ├── gold/                  # Aggregation & KPI pipelines
│   ├── utils/                 # Shared helpers (logging, DQ checks, schema utils)
│   └── dashboard/             # Dashboard definitions
├── tests/
│   ├── unit/                  # Unit tests for transformation logic
│   └── integration/           # Integration tests against real Delta tables
├── data/
│   └── synthetic/             # Generated sample data for local dev/testing
├── config/
│   ├── databricks/            # Job configs, cluster policies, DABs config
│   └── schemas/               # JSON Schema / DDL definitions per layer
├── notebooks/                 # Databricks notebook exports (.py or .ipynb)
├── .claude/
│   ├── agents/                # Subagent definitions
│   ├── skills/                # Databricks AI Dev Kit skills
│   └── mcp.json               # Databricks MCP server config
├── .github/
│   └── workflows/             # CI/CD GitHub Actions
└── docs/                      # Architecture diagrams, runbooks, ADRs
```

---

### Commit Convention

Format: `[LAYER] type: description`

| Layer tag | When to use |
|---|---|
| `[BRONZE]` | Raw ingestion pipelines, Auto Loader configs |
| `[SILVER]` | Cleaning, validation, SCD logic |
| `[GOLD]` | Aggregations, KPI tables, star schema |
| `[INFRA]` | Jobs, DABs configs, GitHub Actions, MCP setup |
| `[DASH]` | Dashboard definitions and queries |
| `[TEST]` | Unit or integration tests |
| `[DOCS]` | Documentation, architecture diagrams, ADRs |

**Examples:**
```
[BRONZE] feat: add POS transaction ingestion via Auto Loader
[SILVER] fix: correct null handling in customer dedup logic
[GOLD] feat: add daily revenue by product category and region
[INFRA] chore: add DABs deployment config for production workflow
```
