SELECT 
    CASE
        WHEN i.distance_km < 200 THEN '0-200km'
        WHEN i.distance_km < 500 THEN '200-500km'
        WHEN i.distance_km < 1000 THEN '500-1000km'
        ELSE '1000km+'
    END AS distance_band,
    CASE
        WHEN i.distance_km < 200 THEN 1
        WHEN i.distance_km < 500 THEN 2
        WHEN i.distance_km < 1000 THEN 3
        ELSE 4
    END AS band_order,
    COUNT(*) AS items,
    ROUND(AVG(i.freight_value), 2) AS avg_freight,
    ROUND(AVG(i.delivery_days), 1) AS avg_delivery_days,
    ROUND(AVG(i.delivery_delay_days), 1) AS avg_delay_days,
    ROUND(100.0 * AVG(CAST(i.is_late AS INT)), 2) AS late_pct
FROM fact_order_items i
INNER JOIN fact_orders o ON i.order_id = o.order_id
WHERE i.is_valid_sale
    AND i.distance_km IS NOT NULL
    AND o.order_status = 'delivered'
    AND NOT o.dq_carrier_before_purchase
    AND NOT o.dq_delivered_without_date
    AND (i.purchase_year * 100 + i.purchase_month) BETWEEN 201701 AND 201808
GROUP BY 1, 2
ORDER BY 2