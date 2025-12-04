WITH seasonal_data AS (
    SELECT
        l.state,
        f.season,
        p.category,
        SUM(f.sales_amount) AS total_sales,
        SUM(f.quantity) AS total_units,
        COUNT(DISTINCT f.order_id) AS order_count
    FROM fact_sales f
    JOIN dim_products p
        ON f.product_name = p.name
        AND DATE(f.order_date) BETWEEN DATE(p.from_date) AND DATE(p.to_date)
    JOIN dim_locations l
        ON f.location_id = l.location_id
    WHERE DATE(f.order_date) BETWEEN DATE('2023-01-01') AND DATE('2024-06-30')
    GROUP BY l.state, f.season, p.category
),
ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY state, season ORDER BY total_sales DESC) AS category_rank
    FROM seasonal_data
)
SELECT *
FROM ranked
WHERE category_rank <= 3
ORDER BY state, season, category_rank;
