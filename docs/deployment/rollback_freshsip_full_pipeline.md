# Rollback Runbook: FreshSip CPG Platform — Full Pipeline

**Deployment date:** 2026-04-05
**Workspace:** https://dbc-2ed6d9c9-8532.cloud.databricks.com

---

## When to Roll Back

- Job fails on 2 or more consecutive scheduled runs
- Data quality checks reject more than 5% of records
- KPI values deviate by more than 20% from the prior day without a known business event
- Schema corruption detected in any Silver or Gold table

---

## Step 1: Pause the Job Schedule

In the Databricks UI:
1. Go to Workflows > Jobs > FreshSip CPG Pipeline
2. Click the schedule toggle to Paused
   OR via CLI: `databricks jobs reset --job-id <JOB_ID> --json '{"schedule": {"pause_status": "PAUSED"}}'`

---

## Step 2: Identify the Bad Batch

```sql
-- Find the batch_id that introduced the problem
DESCRIBE HISTORY brz_freshsip.pos_transactions_raw;
DESCRIBE HISTORY slv_freshsip.sales_transactions;
DESCRIBE HISTORY gld_freshsip.fact_sales;
```

Note the version numbers before the problematic run.

---

## Step 3: Restore Silver Tables

Silver tables use Delta MERGE. Restore to the last known-good version:

```sql
RESTORE TABLE slv_freshsip.sales_transactions        TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.inventory_stock           TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.ref_reorder_points        TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.production_batches        TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.production_events         TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.shipments                 TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.ref_products              TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.customers                 TO VERSION AS OF <version>;
RESTORE TABLE slv_freshsip.ref_warehouses            TO VERSION AS OF <version>;
```

---

## Step 4: Restore Gold Tables

```sql
RESTORE TABLE gld_freshsip.dim_date                  TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.fact_sales                TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.fact_inventory_snapshot   TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.fact_production_batch     TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.fact_shipment             TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.kpi_daily_revenue         TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.kpi_production_yield      TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.kpi_fulfillment_rate      TO VERSION AS OF <version>;
RESTORE TABLE gld_freshsip.kpi_inventory_turnover    TO VERSION AS OF <version>;
```

---

## Step 5: Handle Bronze (Append-Only)

Bronze tables are append-only and must NOT be deleted or restored, as they are the
source of truth for raw data. Instead, quarantine the bad batch:

```sql
-- Flag bad records without deleting (requires _is_quarantined column added in a schema evolution)
-- If column does not yet exist, add it:
ALTER TABLE brz_freshsip.pos_transactions_raw ADD COLUMN IF NOT EXISTS _is_quarantined BOOLEAN DEFAULT false;

-- Mark the bad batch
UPDATE brz_freshsip.pos_transactions_raw
SET _is_quarantined = true
WHERE _batch_id = '<bad_batch_id>';

-- Silver and Gold pipelines filter quarantined records:
-- WHERE _is_quarantined = false OR _is_quarantined IS NULL
```

Repeat for any other Bronze table affected by the bad batch.

---

## Step 6: Restore Notebook Code (If Code Change Caused the Issue)

Notebooks are stored in the Databricks workspace. To revert a notebook to a prior state:

1. In the Databricks UI: open the notebook > Revision history > select prior version > Restore
2. Or re-upload the correct version from Git:
   ```bash
   # From the repository root
   git checkout <good-commit-sha> -- notebooks/silver/sales_transform.py
   # Then re-deploy via MCP upload_to_workspace
   ```

---

## Step 7: Resume the Schedule

After rollback is confirmed and the fix is validated:

1. Go to Workflows > Jobs > FreshSip CPG Pipeline
2. Re-enable the schedule (Unpaused)
3. Trigger a manual run to verify the pipeline completes end-to-end cleanly

---

## Rollback Impact by Table

| Table | Rollback Method | Bronze Safe? |
|---|---|---|
| brz_freshsip.* | Quarantine bad batch_id | Yes (append-only) |
| slv_freshsip.* | RESTORE TABLE TO VERSION | Yes |
| gld_freshsip.* | RESTORE TABLE TO VERSION | Yes |

---

## Contact

Raise a bug ticket under Jira project CPG and assign to the data-engineer role.
Tag the incident with the affected batch_id and pipeline task_key.
