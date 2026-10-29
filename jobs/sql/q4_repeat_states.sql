SELECT
    customer_state,
    COUNT(*) AS repeat_customers,
    ROUND(AVG(frequency), 3) AS avg_frequency,
    ROUND(AVG(monetary), 2) AS avg_monetary,
    ROUND(AVG(avg_order_value), 2) AS avg_aov
FROM agg_customer_rfm
WHERE frequency >= 2
GROUP BY customer_state
ORDER BY repeat_customers DESC