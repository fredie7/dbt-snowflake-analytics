{# Select all locations from the bronze layer #}
WITH locations AS (
    SELECT * FROM {{ ref("bronze_locations") }}
),
{# FInd duplicate locations ids #}
duplicate_locations AS (
    SELECT
        location_id,
        COUNT(*) AS location_count
    FROM
        locations
    GROUP BY
        location_id
    HAVING
        COUNT(*) > 1
),
{# Remove duplicate locations and NULL values#}
unique_locations AS (
    SELECT
        location_id,
        city,
        state,
        country,
        latitude,
        longitude,
        last_updated_timestamp
    FROM
        locations
    WHERE
        location_id NOT IN (SELECT location_id FROM duplicate_locations)
    AND
        city IS NOT NULL
    AND
        state IS NOT NULL
    AND
        country IS NOT NULL
    AND
        latitude IS NOT NULL
    AND
        longitude IS NOT NULL
    AND
        last_updated_timestamp IS NOT NULL
)
{# Recover clean locations data #}
SELECT 
    * 
FROM 
    unique_locations
ORDER BY 
    location_id