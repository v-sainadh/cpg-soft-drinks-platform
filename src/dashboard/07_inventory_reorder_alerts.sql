-- Dashboard Widget: Inventory Reorder Alerts (Current Snapshot)
-- KPI: #7 — Days Sales of Inventory (DSI)
-- Source: gld_freshsip.fact_inventory_snapshot
-- Chart type: Table / Alert panel — SKUs below reorder point right now

SELECT
    f.snapshot_date,
    f.warehouse_key,
    f.product_key,
    f.units_on_hand,
    f.reorder_point_units,
    f.inventory_value,
    f.dsi_days,
    f.reorder_alert_flag,
    CASE
        WHEN f.units_on_hand = 0 THEN 'OUT_OF_STOCK'
        WHEN f.reorder_alert_flag = true THEN 'REORDER_REQUIRED'
        ELSE 'OK'
    END AS stock_status
FROM gld_freshsip.fact_inventory_snapshot f
WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM gld_freshsip.fact_inventory_snapshot)
ORDER BY f.reorder_alert_flag DESC, f.dsi_days ASC;
