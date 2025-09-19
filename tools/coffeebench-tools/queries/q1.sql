SELECT
    f.order_date,
    l.city,
    SUM(f.sales_amount) AS total_sales,
    AVG(SUM(f.sales_amount)) OVER (
        PARTITION BY l.city
        ORDER BY f.order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM fact_sales f
JOIN dim_locations l
    ON f.location_id = l.location_id
GROUP BY
    f.order_date,
    l.city
ORDER BY
    l.city,
    f.order_date;
