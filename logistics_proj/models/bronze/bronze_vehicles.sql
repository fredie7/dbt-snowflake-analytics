-- Populate the vehicles bronze layer
WITH vehicles AS (
    SELECT * FROM LOGISTICS.RAW.VEHICLES
)
SELECT * FROM vehicles