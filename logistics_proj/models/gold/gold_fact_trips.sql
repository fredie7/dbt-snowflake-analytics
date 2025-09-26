WITH fact_trips AS (
    SELECT
        trip_id,
        driver_id,
        customer_id,
        vehicle_id,
        trip_start_time,
        trip_end_time,
        distance_in_km,
        fare_amount,
    FROM {{ source("dev_silver", "silver_trips")}}
)
SELECT
    trip_id,
    driver_id,
    customer_id,
    vehicle_id,
    trip_start_time,
    trip_end_time,
    distance_in_km,
    fare_amount
FROM 
    fact_trips