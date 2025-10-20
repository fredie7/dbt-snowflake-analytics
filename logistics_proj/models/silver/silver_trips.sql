{# Select all trips from the bronze layer #}
WITH trips AS (
    SELECT * FROM {{ ref("bronze_trips") }}
),
{# FFind duplicate trip ids #}
duplicate_trips AS (
    SELECT
        trip_id,
        COUNT(*) AS trip_count
    FROM
        trips
    GROUP BY
        trip_id
    HAVING
        COUNT(*) > 1
),
unique_trips AS (
    SELECT
        trip_id,
        driver_id,
        customer_id,
        vehicle_id,
        trip_start_time,
        trip_end_time,
        start_location,
        end_location,
        distance_km AS distance_in_km,
        fare_amount,
        trip_status,
        last_updated_timestamp,
    FROM 
        trips
    WHERE
        trip_id NOT IN (SELECT trip_id FROM duplicate_trips)
    AND
        trip_id IS NOT NULL
    AND
        driver_id IS NOT NULL
    AND
        customer_id IS NOT NULL
    AND
        vehicle_id IS NOT NULL
    AND
        trip_start_time IS NOT NULL
    AND
        trip_end_time IS NOT NULL
    AND
        start_location IS NOT NULL
    AND
        end_location IS NOT NULL
    AND
        distance_km IS NOT NULL
    AND
        fare_amount IS NOT NULL
    AND
        trip_status IS NOT NULL
    AND
        last_updated_timestamp IS NOT NULL
)

SELECT
    trip_id,
    driver_id,
    customer_id,
    vehicle_id,
    trip_start_time,
    trip_end_time,
    start_location,
    end_location,
    distance_in_km,
    fare_amount,
    trip_status,
    last_updated_timestamp
FROM 
    unique_trips
ORDER BY
    trip_id