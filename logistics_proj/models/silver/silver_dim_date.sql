{# Process the dates table #}
WITH dates as (
    SELECT * FROM {{ ref("bronze_dim_date") }}
)

SELECT
    date_sk AS date_id,
    date AS delivery_date,
    month_name AS delivery_month,
    quarter AS delivery_quarter,
    year AS delivery_year,
    day_name AS delivery_day,
    is_weekend,
    is_month_start,
    is_month_end,
    is_quarter_start,
    is_quarter_end
FROM
    dates