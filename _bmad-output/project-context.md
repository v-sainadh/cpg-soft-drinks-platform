# FreshSip Beverages — CPG Data Platform: Project Context

> This document is the canonical reference for all AI agents working on this project.
> Read this file in full before beginning any task.

---

## Project Overview

- **Company:** FreshSip Beverages (fictional soft drinks manufacturer)
- **Products:** Carbonated soft drinks, flavored water, energy drinks, juice blends
- **Markets:** Operates across 12 US states; 3 distribution channels — Retail, Wholesale, Direct-to-Consumer
- **Goal:** Build an end-to-end data platform using Medallion Architecture on Databricks to power a real-time executive dashboard tracking sales, inventory, production, and distribution KPIs

---

## Technical Stack

| Concern | Technology |
|---|---|
| Cloud Platform | Databricks (with AI Dev Kit for deployment) |
| Architecture | Medallion (Bronze → Silver → Gold) |
| Languages | Python (PySpark), SQL |
| Pipeline Framework | Spark Declarative Pipelines (SDP) — AI Dev Kit skills available |
| Orchestration | Databricks Workflows/Jobs — AI Dev Kit MCP server can create these directly |
| Dashboards | Databricks AI/BI Dashboards — AI Dev Kit MCP server can create these directly |
| Version Control | GitHub |
| Project Management | Jira (Scrum, project key: CPG) |
| CI/CD | GitHub Actions + Databricks Asset Bundles (DABs) |
| Deployment | Databricks AI Dev Kit MCP Server (Claude executes Databricks operations directly) |

---

## Deployment Architecture (Databricks AI Dev Kit)

The project uses the **Databricks AI Dev Kit**, which provides two integration points:

### Skills (`.claude/skills/`)
Teach Claude Databricks-native patterns:
- Spark Declarative Pipelines (SDP)
- Databricks Jobs
- AI/BI Dashboards
- Unity Catalog

**Always read the relevant skill before writing Databricks-specific code.**

### MCP Server (`.claude/mcp.json`)
Lets Claude execute Databricks operations directly:
- Create/update SDP pipelines
- Schedule and run jobs
- Build AI/BI dashboards
- Query tables
- Manage Unity Catalog schemas

### Deployment Flow
```
Write code locally → Push to GitHub → Deploy to Databricks via MCP tools or DABs
```

Claude can directly: create/update SDP pipelines, schedule jobs, build AI/BI dashboards, query tables, manage Unity Catalog schemas.

### Community Edition Fallback
If Unity Catalog or SDP are unavailable:
- Fall back to notebook-based ingestion instead of Auto Loader
- Use Hive metastore with database-per-layer (`brz_freshsip`, `slv_freshsip`, `gld_freshsip`)

---

## Architecture Decisions

### Bronze Layer (Raw Ingestion)
- **Sources:** CSV/JSON files simulating ERP, POS, and IoT sensors
- **Pattern:** Auto Loader (if SDP available), else notebook-based ingestion
- **Schema:** Schema-on-read
- **Write mode:** Append-only, no transformations
- **Naming:** `brz_freshsip.<domain>_<entity>_raw`

### Silver Layer (Cleaned & Conformed)
- **Transformations:** Deduplication, null handling, type casting, business rule validation
- **Schema:** Schema-on-write, Delta Lake format
- **SCD:** Type 2 for dimensional data (customers, products)
- **Naming:** `slv_freshsip.<domain>_<entity>`

### Gold Layer (Business Aggregations)
- **Model:** Star schema; pre-computed KPIs
- **Optimization:** Partitioned and Z-ordered for dashboard query patterns
- **Naming:** `gld_freshsip.<domain>_<kpi_name>`

### Catalog Strategy
- **Primary:** Unity Catalog (three-level: `catalog.schema.table`)
- **Fallback:** Hive metastore — databases `brz_freshsip`, `slv_freshsip`, `gld_freshsip`

---

## Data Domains

| # | Domain | Key Entities |
|---|---|---|
| 1 | **Sales** | POS transactions, order lines, returns, promotions |
| 2 | **Inventory** | Warehouse stock levels, reorder points, shelf life |
| 3 | **Production** | Batch records, quality checks, yield rates, downtime |
| 4 | **Distribution** | Shipments, delivery routes, fulfillment rates, logistics costs |
| 5 | **Customers** | Retailer profiles, segments, credit terms, satisfaction scores |
| 6 | **Products** | SKU master, formulations, packaging, pricing tiers |

---

## Team Structure — Agent Roles

| Role | Responsibilities | Does NOT do |
|---|---|---|
| **Orchestrator** | Oversees handovers, status updates, task assignment, result validation, artifact archiving | Write pipeline code |
| **Product Owner** | Creates BRDs from business requirements, defines KPIs, acceptance criteria | Write code |
| **Data Architect** | Designs data models, defines schemas per layer, creates architecture diagrams, specifies data quality rules | Write pipeline code |
| **Data Engineer** | Builds pipelines (ingestion, transformation, validation), writes PySpark/SQL, deploys via AI Dev Kit, creates workflows and dashboards, fixes bugs | Define business requirements |
| **Code Reviewer** | Reviews all code against quality standards before deployment | Write new features |
| **Deployer** | Uses Databricks AI Dev Kit MCP tools to deploy pipelines, create jobs, build dashboards | Write pipeline logic |

---

## Quality Standards

- All code must include **docstrings and inline comments**
- Every pipeline must have **data quality checks**: null checks, range validation, referential integrity
- **Naming convention:** `snake_case` for tables/columns, prefixed by layer (`brz_`, `slv_`, `gld_`)
- All changes via **Pull Requests** with code review
- **Unit tests** required for all transformation logic
- **Cost optimization:** minimize shuffles, use partition pruning, cache strategically

---

## KPI Definitions (Gold Layer Targets)

| # | KPI | Grain |
|---|---|---|
| 1 | Daily Revenue | By Product Category and Region |
| 2 | Inventory Turnover Rate | By Warehouse |
| 3 | Production Yield Rate | By Batch |
| 4 | Order Fulfillment Rate | By Distribution Channel |
| 5 | Customer Acquisition Cost | By Segment |
| 6 | Gross Margin | By SKU |
| 7 | Days Sales of Inventory (DSI) | Company-wide and by Warehouse |
| 8 | On-Time Delivery Percentage | By Distribution Channel and Region |
