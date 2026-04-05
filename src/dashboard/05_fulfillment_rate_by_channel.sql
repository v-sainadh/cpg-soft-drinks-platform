-- Dashboard Widget: Order Fulfillment Rate and On-Time Delivery by Channel
-- KPI: #4 — Order Fulfillment Rate | KPI #8 — On-Time Delivery %
-- Grain: report_date × channel × region
-- Source: gld_freshsip.kpi_fulfillment_rate
-- Chart type: Grouped bar (x=channel, y=fulfillment_rate_pct and on_time_delivery_pct)

SELECT
    report_date,
    channel,
    region,
    total_shipments,
    on_time_count,
    fully_shipped_count,
    on_time_delivery_pct,
    fulfillment_rate_pct,
    total_cases_delivered,
    total_logistics_cost,
    avg_logistics_cost_per_shipment
FROM gld_freshsip.kpi_fulfillment_rate
WHERE report_date >= DATEADD(day, -30, CURRENT_DATE())
  AND channel IS NOT NULL
  AND channel != ''
ORDER BY report_date DESC, channel, region;
