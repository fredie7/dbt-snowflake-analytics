WITH payment_methods AS (
    SELECT
        payment_id,
        payment_method
    FROM
        {{ source("dev_silver", "silver_payments") }}
)
SELECT
    payment_id,
    payment_method
FROM
    payment_methods