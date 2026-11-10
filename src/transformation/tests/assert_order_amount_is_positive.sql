-- Test kiểm tra logic nghiệp vụ: Không được có đơn hàng nào có số tiền âm hoặc bằng 0
-- Trong dbt: Nếu query này trả về 0 dòng -> Test PASS. Nếu có dòng trả về -> Test FAIL.
SELECT
    order_id,
    total_amount
FROM {{ ref('orders') }}
WHERE total_amount <= 0
