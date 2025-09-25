WITH store AS (
    SELECT * FROM {{ ref("bronze_dim_customer") }}
)
SELECT * FROM store