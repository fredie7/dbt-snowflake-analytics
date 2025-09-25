{# Engage TYPE-2 Slowly Changing Dimension for incremental loading on the trips FACT table #}
{{ config(
    materialized = "incremental",
    unique_key = "trip_id"
) }}

{# Start with data cleaning #}
WITH cleaned_trips_data as (
    SELECT
        *
    FROM(
        SELECT
        trip_id,
        driver_id,
        customer_id,
        vehicle_id,
        trip_start_time,
        trip_end_time,
        start_location,
        end_location,
        distance_km,
        fare_amount,
        trip_status,
        last_updated_timestamp,
        ROW_NUMBER() OVER(PARTITION BY trip_id, last_updated_timestamp ORDER BY last_updated_timestamp DESC) AS trip_row
        FROM {{ ref("bronze_trips") }}
        WHERE
            trip_id IS NOT NULL
        AND
            last_updated_timestamp IS NOT NULL
    ) trip
    WHERE trip_row = 1
),
{# Track the new updates #}
trip_updates AS (
    SELECT
        *
    FROM 
        cleaned_trips_data AS previous_trip
        {% if is_incremental() %}
    LEFT JOIN
        {{ this }} current_trip
    ON
        previous_trip.trip_id = current_trip.trip_id
    AND
        current_trip.is_current = True
    WHERE 
        current_trip.trip_id IS NULL
    OR
        previous_trip.last_updated_timestamp > current_trip.last_updated_timestamp
    {% endif %}
)
{# Retrieve fresh trips data #}
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
    CURRENT_TIMESTAMP AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM
    trip_updates
ORDER BY
    trip_id