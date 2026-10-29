SELECT *
FROM (
    SELECT
        seller_id, seller_state, orders, revenue,
        avg_review_score, avg_delivery_days, late_delivery_pct,
        avg_distance_km, active_months,
        PERCENT_RANK() OVER (ORDER BY revenue) AS revenue_pctile
    FROM agg_seller_performance
) s
WHERE revenue_pctile >= 0.90
  AND (avg_review_score < 4.0
       OR avg_delivery_days > 15
       OR late_delivery_pct > 10)
ORDER BY revenue DESC