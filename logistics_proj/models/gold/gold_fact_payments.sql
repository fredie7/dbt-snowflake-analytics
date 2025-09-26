WITH fact_trips AS (
    SELECT
        payment_id,
        trip_id,
        customer_id,
        amount
    FROM {{ source("dev_silver", "silver_payments")}}
)
SELECT
    payment_id,
    trip_id,
    customer_id,
    amount
FROM fact_trips