{{ config(materialized='view') }}

SELECT
    TRIM(order_id) AS order_id,
    TRIM(customer_id) AS customer_id,
    CAST(order_date AS TIMESTAMP) AS order_timestamp,
    CAST(order_date AS DATE) AS order_date,
    UPPER(TRIM(status)) AS order_status,
    CAST(total_amount AS DECIMAL(10, 2)) AS total_amount,
    UPPER(TRIM(payment_method)) AS payment_method
FROM {{ source('duckpipe_bronze', 'orders') }}
