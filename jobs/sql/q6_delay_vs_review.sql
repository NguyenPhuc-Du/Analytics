SELECT
    CASE
        WHEN delivery_delay_days <= 0 THEN 'on_time_or_early'
        WHEN delivery_delay_days <= 3 THEN 'late_1_3d'
        WHEN delivery_delay_days <= 7 THEN 'late_4_7d'
        WHEN delivery_delay_days <= 14 THEN 'late_8_14d'
        ELSE 'late_15d_plus'
    END AS delay_band,
    CASE
        WHEN delivery_delay_days <= 0 THEN 0
        WHEN delivery_delay_days <= 3 THEN 1
        WHEN delivery_delay_days <= 7 THEN 2
        WHEN delivery_delay_days <= 14 THEN 3
        ELSE 4
    END AS band_order,
    COUNT(*) AS orders,
    ROUND(AVG(review_score), 2) AS avg_review,
    ROUND(100.0 * AVG(CASE WHEN review_score = 1 THEN 1.0 ELSE 0.0 END), 2) AS pct_1_star,
    ROUND(100.0 * AVG(CASE WHEN review_has_comment THEN 1.0 ELSE 0.0 END), 2) AS pct_has_comment
FROM fact_orders
WHERE is_valid_sale
  AND order_status = 'delivered'
  AND NOT dq_carrier_before_purchase
  AND NOT dq_delivered_without_date
  AND review_score IS NOT NULL
  AND delivery_delay_days IS NOT NULL
  AND (purchase_year * 100 + purchase_month) BETWEEN 201701 AND 201808
GROUP BY 1, 2
ORDER BY 2