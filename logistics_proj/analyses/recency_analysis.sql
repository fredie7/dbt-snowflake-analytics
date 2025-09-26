{# FInd the timeframe of the last trip/order for each customer #}

WITH last_trip AS (
    SELECT
        customer_id,
        MAX(trip_start_time)::DATE AS last_trip_date
        FROM 
            {{ source("dev_gold","gold_fact_trips") }}
        GROUP BY
            customer_id
)
SELECT
    last_trip.customer_id,
    dim_cust.customer_name,
    last_trip.last_trip_date,
    DATEDIFF("day", last_trip_date, CURRENT_DATE()) AS days_since_last_trip
FROM
    last_trip
LEFT JOIN
    {{ source("dev_gold", "gold_dim_customers") }} dim_cust
ON
    last_trip.customer_id = dim_cust.customer_id
ORDER BY
    days_since_last_trip