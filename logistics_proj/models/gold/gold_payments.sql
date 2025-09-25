WITH fact_trips AS (
    SELECT
        payment_id,
        trip_id,
        customer_id,
        amount,
        payment_method,
    FROM {{ source("dev_silver", "silver_payments")}}
)
SELECT
    *
FROM fact_trips