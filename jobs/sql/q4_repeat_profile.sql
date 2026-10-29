SELECT
    CASE WHEN frequency >= 2 THEN 'repeat' ELSE 'one_shot' END AS cohort,
    COUNT(*) AS customers,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
    ROUND(AVG(recency_days), 1) AS avg_recency,
    ROUND(AVG(frequency), 3) AS avg_frequency,
    ROUND(AVG(monetary), 2) AS avg_monetary,
    ROUND(AVG(avg_order_value), 2) AS avg_aov
FROM agg_customer_rfm
GROUP BY 1