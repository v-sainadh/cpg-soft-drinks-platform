-- Dashboard Widget: Gross Margin by SKU (Top 20)
-- KPI: #6 — Gross Margin by SKU
-- Source: gld_freshsip.fact_sales + slv_freshsip.ref_products
-- Chart type: Horizontal bar chart (y=product_name, x=gross_margin_amount)

SELECT
    p.sku_id,
    p.product_name,
    p.product_category,
    p.price_tier,
    SUM(f.net_revenue)         AS total_revenue,
    SUM(f.cogs)                AS total_cogs,
    SUM(f.gross_margin_amount) AS total_gross_margin,
    SUM(f.quantity_sold)       AS units_sold,
    ROUND(SUM(f.gross_margin_amount) / NULLIF(SUM(f.net_revenue), 0) * 100, 2) AS gross_margin_pct
FROM gld_freshsip.fact_sales f
JOIN slv_freshsip.ref_products p
  ON f.product_key = ABS(HASH(p.sku_id))
WHERE f.transaction_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY p.sku_id, p.product_name, p.product_category, p.price_tier
ORDER BY total_gross_margin DESC
LIMIT 20;
