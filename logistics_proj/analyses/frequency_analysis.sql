{# Find the frequency of customer request over the past 30 days #}
WITH recent_trips AS (
    SELECT
        customer_id,
        COUNT(*) AS trip_requests
    FROM 
        {{ source("dev_gold","gold_fact_trips") }}
    WHERE
        trip_start_time >= DATEADD("day", -30, CURRENT_DATE())
    GROUP BY 
        customer_id
)
SELECT 
    customer.customer_id, 
    customer.customer_name,
    recent_trips.trip_requests
FROM 
    recent_trips
LEFT JOIN
    {{ source("dev_gold", "gold_dim_customers") }} customer
ON
    recent_trips.customer_id = customer.customer_id
ORDER BY
    recent_trips.trip_requests DESC