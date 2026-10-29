SELECT
    s.*,
    CASE
        WHEN frequency >= 2 AND recency_days <= 90 AND m_score >= 4 THEN 'Champions'
        WHEN frequency >= 2 AND recency_days <= 180 THEN 'Loyal'
        WHEN frequency >= 2 THEN 'At Risk'
        WHEN recency_days <= 90 THEN 'One-shot Recent'
        WHEN recency_days <= 180 THEN 'One-shot Warm'
        ELSE 'One-shot Lost'
    END AS rfm_segment
FROM (
    SELECT
        customer_unique_id, customer_state,
        first_purchase_date, last_purchase_date,
        recency_days, frequency, monetary, avg_order_value,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
    FROM agg_customer_rfm
) s