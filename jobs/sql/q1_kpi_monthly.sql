SELECT 
    purchase_year,
    purchase_month,
    orders,
    customers,
    items,
    revenue,
    freight,
    gmv,
    avg_order_value,
    avg_review_score,
    avg_delivery_days,
    late_delivery_pct,
    ROUND(100.0 * (revenue - LAG(revenue) OVER w)
        / NULLIF(LAG(revenue) OVER w, 0), 2) AS revenue_mom_pct,
    ROUND(100.0 * (orders - LAG(orders) OVER w) 
        / NULLIF(LAG(orders) OVER w, 0), 2) AS orders_mom_pct,
    ROUND(100.0 * (revenue - LAG(revenue) OVER w)
        / NULLIF(LAG(revenue) OVER w, 0), 2) AS revenue_yoy_pct,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
    CASE WHEN purchase_year = 2017 AND purchase_month = 11 THEN TRUE ELSE FALSE END
        AS is_black_friday_month
FROM agg_sales_monthly
WHERE (purchase_year = 2017)
    OR (purchase_year = 2018 AND purchase_month <= 8)
WINDOW w AS (ORDER BY purchase_year, purchase_month)