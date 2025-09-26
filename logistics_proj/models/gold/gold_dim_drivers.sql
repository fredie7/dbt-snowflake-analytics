WITH dim_drivers AS (
    SELECT
        *
    FROM
        {{ source("dev_silver", "silver_drivers") }}
)
SELECT driver_id, driver_name, driver_rating FROM dim_drivers