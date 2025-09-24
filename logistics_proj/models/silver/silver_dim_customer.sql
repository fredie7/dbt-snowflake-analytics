-- Populate the customer data on the silver layer

WITH customers AS (
    SELECT * FROM {{ ref("bronze_dim_customer") }}
)
SELECT
    customer_sk as customer_id,
    customer_code,
    first_name,
    last_name,
    gender,
    email,
    phone,
    loyalty_tier,
    signup_date
FROM customers