WITH dim_drivers AS (
    SELECT
        vehicle_id,
        vehicle_type,
        make,
        production_year        
    FROM {{ source("dev_silver", "silver_vehicles")}}
)
SELECT
    vehicle_id,
    vehicle_type,
    make,
    production_year
FROM dim_drivers
