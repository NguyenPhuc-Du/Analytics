SELECT
    seller_id,
    seller_state,
    orders,
    revenue,
    avg_review_score,
    avg_delivery_days,
    late_delivery_pct,
    avg_distance_km,
    active_months,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS revenue_share_pct,
    ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          / SUM(revenue) OVER (), 2) AS cumulative_share_pct
FROM agg_seller_performance
ORDER BY revenue DESC