-- Populate the customers bronze layer
WITH customers AS (
    SELECT * FROM LOGISTICS.RAW.CUSTOMERS
)
SELECT * FROM customers