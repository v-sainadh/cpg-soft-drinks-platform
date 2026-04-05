-- Inventory Heatmap: Warehouse x Product Category (Latest Snapshot)
SELECT
  dw.warehouse_name,
  dw.region AS warehouse_region,
  dp.product_category,
  SUM(fi.quantity_on_hand) AS total_units,
  ROUND(SUM(fi.quantity_on_hand * dp.standard_cost_per_unit), 2) AS inventory_value,
  ROUND(AVG(fi.days_of_supply), 1) AS avg_days_of_supply
FROM gld_freshsip.fact_inventory_snapshot fi
JOIN gld_freshsip.dim_warehouse dw ON fi.warehouse_key = dw.warehouse_key
JOIN gld_freshsip.dim_product dp ON fi.product_key = dp.product_key
JOIN gld_freshsip.dim_date dd ON fi.date_key = dd.date_key
WHERE dd.full_date = (SELECT MAX(full_date) FROM gld_freshsip.dim_date WHERE full_date <= CURRENT_DATE())
GROUP BY dw.warehouse_name, dw.region, dp.product_category
ORDER BY inventory_value DESC
