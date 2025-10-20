-- Analyze customer retention by examining their lifetime value

WITH customer_first_trip AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(trip_start_time)) AS first_trip_month
    FROM 
        {{ source("dev_gold", "gold_fact_trips") }}
    GROUP BY 
        customer_id
)
SELECT
    cft.first_trip_month,
    DATE_TRUNC('month', t.trip_start_time) AS trip_month,
    COUNT(DISTINCT t.customer_id) AS active_customers
FROM
    {{ source("dev_gold", "gold_fact_trips") }} t
LEFT JOIN
    customer_first_trip cft
    ON t.customer_id = cft.customer_id
GROUP BY
    cft.first_trip_month,
    DATE_TRUNC('month', t.trip_start_time)
ORDER BY
    cft.first_trip_month,
    trip_month;
