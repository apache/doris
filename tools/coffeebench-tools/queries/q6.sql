insert into temp1
WITH monthly_cat AS (
    SELECT
        DATE_TRUNC('month', f.order_date) AS sales_month,
        p.category,
        SUM(f.sales_amount) AS monthly_revenue
    FROM fact_sales f
    JOIN dim_products p
        ON f.product_name = p.name
        AND f.order_date BETWEEN p.from_date AND p.to_date
    GROUP BY
        DATE_TRUNC('month', f.order_date),
        p.category
)
SELECT
    coalesce(sales_month, DATE('1970-01-01')) AS sales_month,
    category,
    monthly_revenue
FROM monthly_cat;
