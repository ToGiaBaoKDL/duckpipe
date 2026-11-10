{{ config(materialized='view') }}

SELECT
    TRIM(customer_id) AS customer_id,
    TRIM(full_name) AS full_name,
    LOWER(TRIM(email)) AS email,
    UPPER(TRIM(country)) AS country_code,
    CAST(created_at AS TIMESTAMP) AS created_at
FROM {{ source('duckpipe_bronze', 'customers') }}
