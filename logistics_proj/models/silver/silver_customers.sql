-- Prepare the silver_customers using the macros utility to remove duplicates and nulls.
WITH customers AS (
    SELECT * FROM {{ ref("bronze_customers") }}
),

unique_customers AS (
    {{ remove_duplicates_and_nulls("customers", "customer_id", [
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "city",
        "signup_date",
        "last_updated_timestamp"
    ]) }}
)

SELECT
    customer_id,
    CONCAT(first_name, ' ', last_name) AS customer_name,
    email,
    REGEXP_REPLACE(phone_number, '[^0-9]', '') AS phone_number,
    city,
    signup_date,
    last_updated_timestamp
FROM unique_customers
ORDER BY customer_id
