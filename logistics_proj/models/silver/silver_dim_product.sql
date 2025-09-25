WITH products AS (
    SELECT * FROM {{ ref("bronze_dim_product") }}
)
SELECT
    product_sk AS product_id,
    product_code,
    product_name,
    department,
    category,
    supplier_sk AS supplier_id,
    list_price
FROM 
    products