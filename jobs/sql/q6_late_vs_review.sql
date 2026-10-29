SELECT
    CASE WHEN is_late THEN 'late' ELSE 'on_time' END AS delivery,
    COUNT(*) AS orders,
    ROUND(AVG(review_score), 2) AS avg_review,
    ROUND(100.0 * AVG(CASE WHEN review_score <= 2 THEN 1.0 ELSE 0.0 END), 2) AS pct_score_1_2,
    ROUND(100.0 * AVG(CASE WHEN review_has_comment THEN 1.0 ELSE 0.0 END), 2) AS pct_has_comment
FROM fact_orders
WHERE is_valid_sale
  AND order_status = 'delivered'
  AND NOT dq_carrier_before_purchase
  AND NOT dq_delivered_without_date
  AND review_score IS NOT NULL
  AND is_late IS NOT NULL
  AND (purchase_year * 100 + purchase_month) BETWEEN 201701 AND 201808
GROUP BY is_late
ORDER BY delivery