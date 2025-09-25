{# Populate the drivers bronze layer #}
WITH drivers AS (
    SELECT * FROM LOGISTICS.RAW.DRIVERS
)
SELECT * FROM drivers