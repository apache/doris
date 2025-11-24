WITH city_quarter_subcat AS (
    SELECT
        l.city,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        p.subcategory,
        SUM(f.sales_amount) AS total_sales
    FROM fact_sales f
    JOIN dim_locations l
        ON f.location_id = l.location_id
    JOIN dim_products p
        ON f.product_name = p.name
        AND f.order_date BETWEEN p.from_date AND p.to_date
    WHERE l.city IN ('Charlotte', 'Houston')
      AND DATE_TRUNC('quarter', f.order_date) IN (
            DATE('2023-01-01'), DATE('2023-04-01'),
            DATE('2024-01-01'), DATE('2024-04-01')
      )
    GROUP BY
        l.city,
        DATE_TRUNC('quarter', f.order_date),
        p.subcategory
)
SELECT
    city,
    sales_quarter,
    subcategory,
    total_sales,
    RANK() OVER (PARTITION BY city, sales_quarter ORDER BY total_sales DESC) AS subcat_rank
FROM city_quarter_subcat
ORDER BY city, sales_quarter, subcat_rank;
