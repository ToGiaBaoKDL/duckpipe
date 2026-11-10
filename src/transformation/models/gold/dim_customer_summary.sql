WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) AS completed_orders,
        COALESCE(SUM(CASE WHEN order_status = 'COMPLETED' THEN total_amount ELSE 0 END), 0) AS total_spent,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM {{ ref('orders') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.country_code,
    c.created_at AS customer_since,
    COALESCE(co.total_orders, 0) AS total_orders,
    COALESCE(co.completed_orders, 0) AS completed_orders,
    COALESCE(co.total_spent, 0) AS total_spent,
    co.first_order_date,
    co.last_order_date,
    CASE
        WHEN COALESCE(co.total_spent, 0) >= 400 THEN 'VIP'
        WHEN COALESCE(co.total_spent, 0) >= 150 THEN 'REGULAR'
        WHEN COALESCE(co.total_orders, 0) > 0 THEN 'BRONZE'
        ELSE 'INACTIVE'
    END AS customer_tier
FROM {{ ref('customers') }} c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
ORDER BY total_spent DESC
