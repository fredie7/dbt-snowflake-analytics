WITH dim_customers AS (
    SELECT 
         customer_id, 
         customer_name, 
         city
    FROM {{ source("dev_silver", "silver_customers") }}
)
SELECT
    customer_id,
    customer_name,
    city
FROM dim_customers
