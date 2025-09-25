{# Select all vehicles from the bronze layer #}
WITH vehicles AS (
    SELECT * FROM {{ ref("bronze_vehicles") }}
),
{# FInd duplicate vehicles ids #}
duplicate_vehicles AS (
    SELECT
        vehicle_id,
        COUNT(*) AS vehicle_count
    FROM 
        vehicles
    GROUP BY
        vehicle_id
    HAVING
        COUNT(*) > 1
),
{# Remove duplicate vehicles and NULL values#}
unique_vehicles AS (
    SELECT
        *
    FROM
        vehicles
    WHERE
        vehicle_id NOT IN (SELECT vehicle_id FROM duplicate_vehicles)
    AND
        vehicle_id IS NOT NULL
    AND
        license_plate IS NOT NULL
    AND
        make IS NOT NULL
    AND
        year IS NOT NULL
    AND
        vehicle_type IS NOT NULL
    AND
        last_updated_timestamp IS NOT NULL
)
{# Recover clean payments data with easily understandable column arrangement and renaming#}
SELECT
    vehicle_id,
    vehicle_type,
    license_plate,
    make,
    year AS production_year,
    last_updated_timestamp
FROM
    unique_vehicles
ORDER BY
    vehicle_id