SELECT
    COALESCE(main_payment_type, 'unknown') AS main_payment_type,
    CASE
        WHEN max_installments IS NULL OR max_installments <= 1 THEN '1_or_none'
        WHEN max_installments <= 3 THEN '2_3'
        WHEN max_installments <= 6 THEN '4_6'
        ELSE '7_plus'
    END AS installment_band,
    COUNT(*) AS orders,
    ROUND(SUM(order_value), 2) AS gmv,
    ROUND(AVG(order_value), 2) AS avg_order_value
FROM fact_orders
WHERE is_valid_sale
  AND (purchase_year * 100 + purchase_month) BETWEEN 201701 AND 201808
GROUP BY 1, 2
ORDER BY 1, 2