{# Calculate monetary value by customer spend #}
SELECT
    dim_customers.customer_id,
    dim_customers.customer_name,
    COUNT(fact_trips.trip_id) total_trips,
    SUM(fact_payments.amount) AS total_revenue,
    ROUND(SUM(fact_payments.amount) / NULLIF(COUNT(fact_trips.trip_id),0),2) AS avg_order_value
FROM
    {{ source("dev_gold", "gold_fact_trips") }} fact_trips
INNER JOIN
    {{ source("dev_gold", "gold_fact_payments") }} fact_payments
ON
    fact_trips.trip_id = fact_payments.trip_id
LEFT JOIN
    {{ source("dev_gold", "gold_dim_customers") }} dim_customers
ON
    fact_trips.customer_id = dim_customers.customer_id
GROUP BY
    dim_customers.customer_id,
    dim_customers.customer_name
ORDER BY
    total_revenue DESC