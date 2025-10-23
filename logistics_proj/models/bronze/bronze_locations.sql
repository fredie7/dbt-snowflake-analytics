-- Populate the locations bronze layer

WITH locations AS (
    SELECT * FROM LOGISTICS.RAW.LOCATIONS
)
SELECT * FROM locations