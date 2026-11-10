SELECT
    order_date,
    COUNT(order_id) AS total_orders,
    COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) AS completed_orders,
    COUNT(CASE WHEN order_status = 'CANCELLED' THEN 1 END) AS cancelled_orders,
    COALESCE(SUM(CASE WHEN order_status = 'COMPLETED' THEN total_amount ELSE 0 END), 0) AS total_revenue,
    ROUND(
        COALESCE(
            SUM(CASE WHEN order_status = 'COMPLETED' THEN total_amount ELSE 0 END) / 
            NULLIF(COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END), 0),
            0
        ), 
        2
    ) AS avg_order_value
FROM {{ ref('orders') }}
GROUP BY order_date
ORDER BY order_date DESC
