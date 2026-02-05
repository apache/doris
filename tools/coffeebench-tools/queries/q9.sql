WITH daily_discount AS (
    SELECT
        l.city,
        f.order_date,
        AVG(f.discount_percentage) AS avg_discount
    FROM fact_sales f
    JOIN dim_locations l
        ON f.location_id = l.location_id
    GROUP BY
        l.city,
        f.order_date
)
SELECT
    city,
    order_date,
    avg_discount,
    AVG(avg_discount) OVER (
        PARTITION BY city
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_avg_discount
FROM daily_discount
ORDER BY city, order_date;
