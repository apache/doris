WITH daily_city_qty AS (
    SELECT
        f.order_date,
        l.city,
        SUM(f.quantity) AS daily_qty
    FROM fact_sales f
    JOIN dim_locations l
        ON f.location_id = l.location_id
    GROUP BY
        f.order_date,
        l.city
)
SELECT
    order_date,
    city,
    daily_qty,
    SUM(daily_qty) OVER (
        PARTITION BY city
        ORDER BY order_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30day_qty
FROM daily_city_qty
ORDER BY city, order_date;
