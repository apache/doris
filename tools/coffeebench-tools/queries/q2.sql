WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', f.order_date) AS sales_month,
        f.product_name,
        SUM(f.sales_amount) AS total_sales
    FROM fact_sales f
    GROUP BY
        DATE_TRUNC('month', f.order_date),
        f.product_name
)
SELECT
    sales_month,
    product_name,
    total_sales,
    RANK() OVER (PARTITION BY sales_month ORDER BY total_sales DESC) AS sales_rank
FROM monthly_sales
ORDER BY sales_month, sales_rank;
