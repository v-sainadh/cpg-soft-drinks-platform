-- Dashboard Widget: Daily Revenue by Product Category
-- KPI: #1 — Daily Revenue | Grain: report_date × product_category
-- Source: gld_freshsip.kpi_daily_revenue
-- Chart type: Stacked Bar (categories on x=date, y=total_revenue, color=product_category)

SELECT
    report_date,
    product_category,
    SUM(total_revenue)       AS total_revenue,
    SUM(total_gross_margin)  AS total_gross_margin,
    AVG(gross_margin_pct)    AS avg_margin_pct,
    SUM(units_sold)          AS units_sold,
    SUM(transaction_count)   AS transaction_count
FROM gld_freshsip.kpi_daily_revenue
WHERE report_date >= DATEADD(day, -30, CURRENT_DATE())
  AND product_category IS NOT NULL
GROUP BY report_date, product_category
ORDER BY report_date DESC, total_revenue DESC;
