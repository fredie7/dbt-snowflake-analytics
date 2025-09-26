WITH dim_locations AS (
    SELECT
        location_id,
        city,
        state,
        country
    FROM {{ source("dev_silver", "silver_locations")}}
)
SELECT
   location_id,
    city,
    state,
    country
FROM dim_locations