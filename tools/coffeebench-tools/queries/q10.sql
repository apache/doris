insert into temp2
WITH daily_orders AS (
    SELECT
        f.order_date,
        l.city,
        COUNT(DISTINCT f.order_id) AS daily_distinct_orders
    FROM fact_sales f
    JOIN dim_locations l
        ON f.location_id = l.location_id
    GROUP BY
        l.city,
        f.order_date

)
SELECT
    coalesce(order_date, DATE('1970-01-01')) AS order_date,
    coalesce(city, '') AS city,
    daily_distinct_orders,
    SUM(daily_distinct_orders) OVER (
        PARTITION BY city
        ORDER BY order_date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS rolling_90d_distinct_orders
FROM daily_orders
ORDER BY city, order_date;
