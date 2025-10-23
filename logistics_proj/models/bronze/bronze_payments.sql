-- Populate the payments bronze layer
WITH payments AS (
    SELECT * FROM LOGISTICS.RAW.PAYMENTS
)
SELECT * FROM payments