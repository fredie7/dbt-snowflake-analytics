{# Select all customers from the bronze layer #}
WITH customers AS (
    SELECT * FROM {{ ref("bronze_customers") }}
),
{# FInd duplicate customer ids #}
duplicate_customers AS (
    SELECT 
    customer_id,
    COUNT(*) AS customer_count,
    FROM customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
),
{# Remove duplicate customers and NULL values#}
unique_customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone_number,
        city,
        signup_date,
        last_updated_timestamp
    FROM 
        customers
    WHERE
        customer_id NOT IN (SELECT customer_id FROM duplicate_customers)
    AND
        customer_id IS NOT NULL
    AND
        first_name IS NOT NULL
    AND
        last_name IS NOT NULL
    AND
        email IS NOT NULL
    AND
        phone_number IS NOT NULL
    AND
        city IS NOT NULL
    AND
        signup_date IS NOT NULL
    AND
        last_updated_timestamp IS NOT NULL
)
{# Recover clean customers data with treated names and phone numbers #}
SELECT
    customer_id,
    CONCAT(first_name, ' ', last_name) AS customer_name,
    email,
    REGEXP_REPLACE(phone_number, '[^0-9]', '') AS phone_number,
    city,
    signup_date,
    last_updated_timestamp
FROM
    unique_customers
ORDER BY
    customer_id


