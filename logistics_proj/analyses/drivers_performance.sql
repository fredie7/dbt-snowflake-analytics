-- Analyze drivers' performance, referencing total trips, distance covered, average trip duration, and average fare amount
SELECT
    drivers.driver_id,
    drivers.driver_name,
    COUNT(trips.trip_id) AS total_trips,
    ROUND(AVG(DATEDIFF("minutes", trips.trip_start_time, trips.trip_end_time)), 2) AS avg_duration_minutes,
    ROUND(AVG(trips.fare_amount), 2) AS avg_fare_amount,
    ROUND(AVG(trips.distance_in_km), 2) AS avg_distance_km
FROM
    {{ source("dev_gold", "gold_fact_trips")  }} trips
LEFT JOIN
    {{ source("dev_gold", "gold_dim_drivers")  }} drivers
ON
    trips.driver_id = drivers.driver_id
GROUP BY
    drivers.driver_id,
    drivers.driver_name
ORDER BY
    total_trips DESC;
    