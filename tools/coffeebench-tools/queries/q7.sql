WITH yearly_sales AS (
    SELECT
        l.location_id,
        l.city,
        l.state,
        YEAR(f.order_date) AS sales_year,
        SUM(f.sales_amount) AS total_sales_year
    FROM fact_sales f
    JOIN dim_locations l
        ON f.location_id = l.location_id
    GROUP BY
        l.location_id,
        l.city,
        l.state,
        YEAR(f.order_date)
)
SELECT
    city,
    state,
    SUM(CASE WHEN sales_year = 2023 THEN total_sales_year ELSE 0 END) AS sales_2023,
    SUM(CASE WHEN sales_year = 2024 THEN total_sales_year ELSE 0 END) AS sales_2024,
    (SUM(CASE WHEN sales_year = 2024 THEN total_sales_year ELSE 0 END)
     - SUM(CASE WHEN sales_year = 2023 THEN total_sales_year ELSE 0 END)) AS yoy_diff
FROM yearly_sales
GROUP BY
    city,
    state
ORDER BY
    city,
    state;
