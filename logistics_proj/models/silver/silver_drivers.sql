{# Select all driverss from the bronze layer #}
WITH drivers AS (
    SELECT * FROM {{ ref("bronze_drivers") }}
),
{# FInd duplicate driver ids #}
duplicate_drivers AS (
    SELECT
        driver_id,
        COUNT(*) AS driver_count
    FROM 
        drivers
    GROUP BY 
        driver_id
    HAVING 
        COUNT(*) > 1
),
{# Remove duplicate drivers as well as NULL values#}
unique_drivers AS (
    SELECT 
        driver_id,
        first_name,
        last_name,
        phone_number,
        vehicle_id,
        driver_rating,
        city,
        last_updated_timestamp
    FROM 
        drivers
    WHERE
        driver_id NOT IN (SELECT driver_id FROM duplicate_drivers)
    AND
        driver_id IS NOT NULL
    AND
        first_name IS NOT NULL
    AND
        last_name IS NOT NULL
    AND
        phone_number IS NOT NULL
    AND 
        vehicle_id IS NOT NULL
    AND
        driver_rating IS NOT NULL
    AND
        city IS NOT NULL
    AND
        last_updated_timestamp IS NOT NULL
)
{# Recover clean drivers data with treated names and phone numbers #}
SELECT
    driver_id,
    CONCAT(first_name, ' ', last_name) AS driver_name,
    REGEXP_REPLACE(phone_number, '[^0-9]','') AS phone_number,
    vehicle_id,
    driver_rating,
    city,
    last_updated_timestamp
FROM 
    unique_drivers
ORDER BY
    driver_id