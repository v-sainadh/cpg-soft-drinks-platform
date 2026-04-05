-- Days of Supply by Warehouse (Latest Snapshot)
SELECT
  dw.warehouse_name,
  ROUND(AVG(fi.days_of_supply), 1) AS avg_days_of_supply,
  MIN(fi.days_of_supply) AS min_days_of_supply,
  MAX(fi.days_of_supply) AS max_days_of_supply,
  SUM(CASE WHEN fi.days_of_supply < 7 THEN 1 ELSE 0 END) AS low_stock_skus
FROM gld_freshsip.fact_inventory_snapshot fi
JOIN gld_freshsip.dim_warehouse dw ON fi.warehouse_key = dw.warehouse_key
JOIN gld_freshsip.dim_date dd ON fi.date_key = dd.date_key
WHERE dd.full_date = (SELECT MAX(full_date) FROM gld_freshsip.dim_date WHERE full_date <= CURRENT_DATE())
GROUP BY dw.warehouse_name
ORDER BY avg_days_of_supply
