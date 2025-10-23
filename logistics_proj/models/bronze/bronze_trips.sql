-- Populate trips table on the bronze layer

WITH trips AS (
    SELECT * FROM LOGISTICS.RAW.TRIPS
)
SELECT * FROM trips