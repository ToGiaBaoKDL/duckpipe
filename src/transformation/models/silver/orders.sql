SELECT
    order_id,
    customer_id,
    order_timestamp,
    order_date,
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    order_status,
    total_amount,
    payment_method,
    CURRENT_TIMESTAMP AS ingested_at
FROM {{ ref('stg_orders') }}
WHERE order_id IS NOT NULL
-- Khử trùng lặp (Deduplication): Chỉ giữ lại bản ghi mới nhất của mỗi order_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_timestamp DESC) = 1
