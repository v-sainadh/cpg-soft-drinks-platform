-- KPI Cards: Month-to-Date Summary
-- Used for: Revenue MTD, Avg Inventory Turnover, Avg Yield Rate, Avg On-Time Delivery %
SELECT
  COALESCE(SUM(kr.revenue), 0) AS revenue_mtd,
  ROUND(AVG(ki.inventory_turnover_rate), 2) AS avg_inventory_turnover,
  ROUND(AVG(kp.yield_rate_pct), 1) AS avg_yield_rate_pct,
  ROUND(AVG(kf.fulfillment_rate_pct), 1) AS avg_on_time_pct
FROM gld_freshsip.kpi_daily_revenue kr
LEFT JOIN gld_freshsip.kpi_inventory_turnover ki ON kr.report_date = ki.report_date
LEFT JOIN gld_freshsip.kpi_production_yield kp ON kr.report_date = kp.report_date
LEFT JOIN gld_freshsip.kpi_fulfillment_rate kf ON kr.report_date = kf.report_date
WHERE kr.report_date >= DATE_TRUNC('month', CURRENT_DATE())
