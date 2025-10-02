{% snapshot customer_snapshot %}
{# Track changes in the customer table #}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_key',
        strategy='timestamp',
        updated_at='last_updated_timestamp'
    )
}}

WITH raw_customers AS (
    SELECT *
    FROM {{ ref('bronze_customers') }}
),

-- Find duplicates
duplicate_customers AS (
    SELECT 
        customer_id,
        COUNT(*) AS customer_count
    FROM raw_customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
),

-- Remove duplicates and NULLs
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
    FROM raw_customers
    WHERE customer_id NOT IN (SELECT customer_id FROM duplicate_customers)
      AND customer_id IS NOT NULL
      AND first_name IS NOT NULL
      AND last_name IS NOT NULL
      AND email IS NOT NULL
      AND phone_number IS NOT NULL
      AND city IS NOT NULL
      AND signup_date IS NOT NULL
      AND last_updated_timestamp IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'email', 'phone_number', 'city']) }} AS customer_key,
    customer_id,
    CONCAT(first_name, ' ', last_name) AS customer_name,
    REGEXP_REPLACE(phone_number, '[^0-9]','') AS phone_number,
    email,
    city,
    signup_date,
    last_updated_timestamp
FROM unique_customers
ORDER BY customer_id

{% endsnapshot %}

