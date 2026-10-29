SELECT 
    customer_state,
    orders,
    customers,
    revenue,
    gmv,
    avg_order_value,
    avg_freight,
    freight_pct_of_revenue,
    avg_delivery_days,
    late_delivery_pct,
    avg_review_score,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS revenue_share_pct,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
FROM agg_customer_state
ORDER BY revenue DESC;