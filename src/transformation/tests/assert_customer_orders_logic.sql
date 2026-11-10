-- Test kiểm tra logic nghiệp vụ: Số đơn COMPLETED không được vượt quá tổng đơn, và total_spent không được âm
SELECT
    customer_id,
    total_orders,
    completed_orders,
    total_spent
FROM {{ ref('dim_customer_summary') }}
WHERE completed_orders > total_orders
   OR total_spent < 0
