-- Dashboard Widget: Production Yield Rate by Line
-- KPI: #3 — Production Yield Rate | Grain: report_date × production_line_id
-- Source: gld_freshsip.kpi_production_yield
-- Chart type: Line chart (x=date, y=avg_yield_rate_pct, series=production_line_id)

SELECT
    report_date,
    production_line_id,
    batch_count,
    avg_yield_rate_pct,
    min_yield_rate_pct,
    max_yield_rate_pct,
    overall_yield_pct,
    qc_pass_rate_pct,
    total_downtime_hours,
    total_expected_units,
    total_actual_units
FROM gld_freshsip.kpi_production_yield
WHERE report_date >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY report_date DESC, production_line_id;
