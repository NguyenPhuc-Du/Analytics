SELECT 
    category,
    purchase_month,
    SUM(revenue) AS revenue,
    SUM(orders) AS orders,
    ROUND(100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER (PARTITION BY category), 2)
        AS pct_of_category_year
FROM agg_category_monthly
WHERE (purchase_year = 2017)
GROUP BY category, purchase_month
ORDER BY category, purchase_month