# Deployment Record: FreshSip CPG Platform — Full Pipeline

**Date:** 2026-04-05
**Deployed by:** Deployer Agent
**Environment:** dev (Databricks Community Edition)
**Workspace:** https://dbc-2ed6d9c9-8532.cloud.databricks.com

---

## Deployed Resources

### Workspace Notebooks (19 total — all verified present)

| Layer | Task Key | Workspace Path |
|---|---|---|
| Bronze | bronze_sales | /FreshSip/bronze/sales_ingestion |
| Bronze | bronze_inventory | /FreshSip/bronze/inventory_ingestion |
| Bronze | bronze_production | /FreshSip/bronze/production_ingestion |
| Bronze | bronze_distribution | /FreshSip/bronze/distribution_ingestion |
| Bronze | bronze_master_data | /FreshSip/bronze/master_data_ingestion |
| Silver | silver_sales | /FreshSip/silver/sales_transform |
| Silver | silver_inventory | /FreshSip/silver/inventory_transform |
| Silver | silver_production | /FreshSip/silver/production_transform |
| Silver | silver_distribution | /FreshSip/silver/distribution_transform |
| Silver | silver_master_data | /FreshSip/silver/master_data_transform |
| Gold | gold_dim_date | /FreshSip/gold/dim_date |
| Gold | gold_fact_sales | /FreshSip/gold/fact_sales |
| Gold | gold_fact_inventory_snapshot | /FreshSip/gold/fact_inventory_snapshot |
| Gold | gold_fact_production_batch | /FreshSip/gold/fact_production_batch |
| Gold | gold_fact_shipment | /FreshSip/gold/fact_shipment |
| Gold | gold_kpi_daily_revenue | /FreshSip/gold/kpi_daily_revenue |
| Gold | gold_kpi_production_yield | /FreshSip/gold/kpi_production_yield |
| Gold | gold_kpi_fulfillment_rate | /FreshSip/gold/kpi_fulfillment_rate |
| Gold | gold_kpi_inventory_turnover | /FreshSip/gold/kpi_inventory_turnover |

Upload verification: All 19 notebooks confirmed present via workspace API listing.

### Databricks Job — PENDING MANUAL CREATION

**Job name:** FreshSip CPG Pipeline
**Schedule:** Daily at 06:00 UTC (cron: `0 0 6 * * ?`)
**Status:** Job configuration is fully defined but could not be created automatically.

**Reason:** The Databricks Community Edition PAT token does not include the `jobs` API scope.
The MCP `manage_jobs` tool returned an output validation error on every create attempt
(confirmed via both the MCP tool and direct REST API calls returning HTTP 403).

**Action required:** Create the job manually in the Databricks UI using the JSON definition at:
`config/databricks/freshsip_cpg_pipeline_job.json`

See manual creation steps in the section below.

### Existing Dashboard (untouched)

| Resource | ID | URL |
|---|---|---|
| FreshSip Executive Dashboard | 01f130b425881effbe2764e57e0feba6 | https://dbc-2ed6d9c9-8532.cloud.databricks.com/sql/dashboardsv3/01f130b425881effbe2764e57e0feba6 |

---

## Tables to be Created (on first job run)

| Layer | Table | Source |
|---|---|---|
| Bronze | brz_freshsip.pos_transactions_raw | POS JSON files |
| Bronze | brz_freshsip.erp_sales_raw | ERP CSV orders + order_lines |
| Bronze | brz_freshsip.erp_inventory_raw | ERP inventory CSV |
| Bronze | brz_freshsip.iot_sensor_events_raw | IoT sensor JSON |
| Bronze | brz_freshsip.logistics_shipments_raw | Logistics CSV |
| Bronze | brz_freshsip.erp_products_raw | ERP products CSV |
| Bronze | brz_freshsip.erp_customers_raw | ERP customers CSV |
| Bronze | brz_freshsip.erp_warehouses_raw | ERP warehouses CSV |
| Silver | slv_freshsip.sales_transactions | Cast + dedup + MERGE upsert |
| Silver | slv_freshsip.inventory_stock | Cast + dedup + MERGE upsert |
| Silver | slv_freshsip.ref_reorder_points | SCD Type 1 overwrite |
| Silver | slv_freshsip.production_batches | BATCH_START/END aggregation |
| Silver | slv_freshsip.production_events | QC_CHECK + DOWNTIME events |
| Silver | slv_freshsip.shipments | Cast + dedup + on_time_flag |
| Silver | slv_freshsip.ref_products | SCD Type 1 overwrite |
| Silver | slv_freshsip.customers | SCD Type 2 |
| Silver | slv_freshsip.ref_warehouses | SCD Type 1 overwrite |
| Gold | gld_freshsip.dim_date | Generated date spine 2023-2027 |
| Gold | gld_freshsip.fact_sales | Star schema sales fact |
| Gold | gld_freshsip.fact_inventory_snapshot | Inventory fact |
| Gold | gld_freshsip.fact_production_batch | Production fact |
| Gold | gld_freshsip.fact_shipment | Shipment fact |
| Gold | gld_freshsip.kpi_daily_revenue | KPI #1: Daily Revenue |
| Gold | gld_freshsip.kpi_production_yield | KPI #3: Production Yield |
| Gold | gld_freshsip.kpi_fulfillment_rate | KPIs #4 + #8: Fulfillment + On-Time |
| Gold | gld_freshsip.kpi_inventory_turnover | KPI #2: Inventory Turnover |

---

## DAG Dependency Order

```
Bronze (5 parallel)
  bronze_sales ─────────────────────────┐
  bronze_inventory ──────────────────┐  │
  bronze_production ──────────────┐  │  │
  bronze_distribution ──────────┐ │  │  │
  bronze_master_data ──────┬────┘─┘──┘──┘

Silver (fan-out after Bronze)
  silver_sales        ← bronze_sales + bronze_master_data
  silver_inventory    ← bronze_inventory + bronze_master_data
  silver_production   ← bronze_production
  silver_distribution ← bronze_distribution
  silver_master_data  ← bronze_master_data

Gold — dim_date (gate task, waits for ALL 5 Silver tasks)
  gold_dim_date ← ALL silver tasks

Gold — Facts (4 parallel after dim_date)
  gold_fact_sales              ← gold_dim_date
  gold_fact_inventory_snapshot ← gold_dim_date
  gold_fact_production_batch   ← gold_dim_date
  gold_fact_shipment           ← gold_dim_date

Gold — KPIs (depend on their respective facts)
  gold_kpi_daily_revenue     ← gold_fact_sales
  gold_kpi_production_yield  ← gold_fact_production_batch
  gold_kpi_fulfillment_rate  ← gold_fact_shipment
  gold_kpi_inventory_turnover ← gold_fact_inventory_snapshot + gold_fact_sales
```

---

## Manual Job Creation Steps (Community Edition Workaround)

The PAT token does not have the `jobs` API scope. Follow these steps to create the
job in the Databricks UI:

1. Navigate to: https://dbc-2ed6d9c9-8532.cloud.databricks.com
2. Go to: Workflows > Jobs > Create Job
3. Job name: `FreshSip CPG Pipeline`
4. Switch to JSON mode (gear icon > Edit JSON)
5. Paste the contents of: `config/databricks/freshsip_cpg_pipeline_job.json`
6. Click "Create" / "Save"
7. The schedule (daily 06:00 UTC) will be automatically configured

Alternatively, generate a new PAT token with the `jobs` scope at:
Settings > Developer > Access Tokens > Generate New Token
Then re-run this deployment.

---

## Notebook Upload Verification

All 19 uploads confirmed successful (0 failures) via MCP `upload_to_workspace` calls:

- Upload time: 2026-04-05
- Method: MCP `mcp__databricks__upload_to_workspace`
- Source: Local `src/bronze/` and `notebooks/silver/`, `notebooks/gold/` directories
- Destination: `/FreshSip/bronze/`, `/FreshSip/silver/`, `/FreshSip/gold/`
- Verification: Confirmed via `GET /api/2.0/workspace/list` — all 19 notebook paths present

---

## Rollback Procedure

See: `docs/deployment/rollback_freshsip_full_pipeline.md`
