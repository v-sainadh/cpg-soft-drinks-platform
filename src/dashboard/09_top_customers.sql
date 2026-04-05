-- Top 10 Customers by Revenue (Last 30 Days)
SELECT
  dc.retailer_name,
  dc.retail_segment,
  dc.region,
  dc.channel,
  ROUND(SUM(fs.net_revenue), 2) AS total_revenue,
  COUNT(DISTINCT fs.transaction_key) AS transaction_count,
  ROUND(AVG(fs.net_revenue), 2) AS avg_transaction_value
FROM gld_freshsip.fact_sales fs
JOIN gld_freshsip.dim_customer dc ON fs.customer_key = dc.customer_key
JOIN gld_freshsip.dim_date dd ON fs.date_key = dd.date_key
WHERE dd.full_date >= date_sub(CURRENT_DATE(), 30)
GROUP BY dc.retailer_name, dc.retail_segment, dc.region, dc.channel
ORDER BY total_revenue DESC
LIMIT 10
