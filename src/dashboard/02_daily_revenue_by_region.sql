-- Dashboard Widget: Daily Revenue by Region
-- KPI: #1 — Daily Revenue | Grain: report_date × region
-- Source: gld_freshsip.kpi_daily_revenue
-- Chart type: Line chart (x=date, y=total_revenue, series=region)

SELECT
    report_date,
    region,
    SUM(total_revenue)       AS total_revenue,
    SUM(total_gross_margin)  AS total_gross_margin,
    SUM(units_sold)          AS units_sold
FROM gld_freshsip.kpi_daily_revenue
WHERE report_date >= DATEADD(day, -30, CURRENT_DATE())
  AND region IS NOT NULL
  AND region != ''
GROUP BY report_date, region
ORDER BY report_date DESC, region;
