SELECT
    customer_id,
    full_name,
    email,
    country_code,
    created_at,
    CURRENT_TIMESTAMP AS ingested_at
FROM {{ ref('stg_customers') }}
WHERE customer_id IS NOT NULL
-- Khử trùng lặp (Deduplication): Chỉ giữ lại bản ghi mới nhất của mỗi customer_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) = 1
