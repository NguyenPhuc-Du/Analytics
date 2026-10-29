SELECT 
    category,
    SUM(orders) AS orders,
    SUM(items) AS items,
    SUM(revenue) AS revenue,
    SUM(freight) AS freight,
    ROUND(SUM(revenue) / NULLIF(SUM(items), 0), 2) AS avg_price,
    ROUND(100.0 * SUM(freight) / NULLIF(SUM(revenue), 0), 2) AS freight_to_revenue_pct,
    ROUND(100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER (), 2) AS revenue_share_pct,
    ROUND(100.0 * SUM(SUM(revenue)) OVER (ORDER BY SUM(revenue) DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        / SUM(SUM(revenue)) OVER (), 2) AS cumulative_share_pct
FROM agg_category_monthly
WHERE (purchase_year = 2017)
    OR (purchase_year = 2018 AND purchase_month <= 8)
GROUP BY category
ORDER BY revenue DESC