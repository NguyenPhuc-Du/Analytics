SELECT
    i.category,
    COUNT(DISTINCT i.customer_unique_id) AS repeat_customers,
    COUNT(*) AS items,
    ROUND(SUM(i.price), 2) AS revenue
FROM fact_order_items i
INNER JOIN agg_customer_rfm r
    ON i.customer_unique_id = r.customer_unique_id
WHERE r.frequency >= 2
  AND i.is_valid_sale
  AND (i.purchase_year * 100 + i.purchase_month) BETWEEN 201701 AND 201808
GROUP BY i.category
ORDER BY revenue DESC