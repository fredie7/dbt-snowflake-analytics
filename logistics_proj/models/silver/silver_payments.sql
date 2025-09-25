{# Select all payments from the bronze layer #}
WITH payments AS (
    SELECT * FROM {{ ref("bronze_payments") }}
),
{# FInd duplicate payment ids #}
duplicate_payments AS (
    SELECT
        payment_id,
        COUNT(*) AS payment_count
    FROM
        payments
    GROUP BY
        payment_id
    HAVING
        COUNT(*) > 1
),
{# Remove duplicate payments and NULL values#}
unique_payments AS (
    SELECT
        *
    FROM
        payments
    WHERE
        payment_id NOT IN (SELECT payment_id FROM duplicate_payments)
    AND
        payment_id IS NOT NULL
    AND
        trip_id IS NOT NULL
    AND
        customer_id IS NOT NULL
    AND
        payment_method IS NOT NULL
    AND
        payment_status IS NOT NULL
    AND
        amount IS NOT NULL
    AND
        transaction_time IS NOT NULL
    AND
        last_updated_timestamp IS NOT NULL
)
{# Recover clean payments data #}
SELECT 
    * 
FROM 
    unique_payments
ORDER BY
    payment_id