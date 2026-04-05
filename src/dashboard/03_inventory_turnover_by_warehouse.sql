-- Dashboard Widget: Inventory Turnover Rate by Warehouse
-- KPI: #2 — Inventory Turnover Rate | Grain: report_month × warehouse_key
-- Source: gld_freshsip.kpi_inventory_turnover
-- Chart type: Bar chart (x=warehouse_key, y=inventory_turnover_rate, grouped by month)

SELECT
    report_month,
    warehouse_key,
    avg_inventory_value,
    total_cogs,
    inventory_turnover_rate,
    days_inventory_outstanding,
    total_units_on_hand
FROM gld_freshsip.kpi_inventory_turnover
WHERE report_month >= DATE_FORMAT(DATEADD(month, -3, CURRENT_DATE()), 'yyyy-MM')
ORDER BY report_month DESC, inventory_turnover_rate DESC;
