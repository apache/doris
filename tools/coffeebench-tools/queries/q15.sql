insert into temp3
WITH base_data AS (
    SELECT
        f.location_id as location_id,
        l.city,
        f.product_name,
        DATE_TRUNC('quarter', f.order_date) AS sales_quarter,
        SUM(f.sales_amount) AS total_sales,
        SUM(f.sales_amount * (f.discount_percentage / 100.0)) AS total_discount,
        SUM(f.quantity * p.standard_cost) AS total_cogs
    FROM fact_sales f
    INNER JOIN dim_products p
        ON f.product_name = p.name
        AND f.order_date BETWEEN p.from_date AND p.to_date
    INNER JOIN dim_locations l
        ON f.location_id = l.location_id
    WHERE f.order_date BETWEEN '2022-01-01' AND '2024-12-31'
    GROUP BY
        f.location_id,
        l.city,
        f.product_name,
        DATE_TRUNC('quarter', f.order_date)
),
with_profit AS (
    SELECT
        *,
        total_sales - total_discount - total_cogs AS profit
    FROM base_data
)
SELECT
    city,
    product_name,
    sales_quarter,
    profit,
    LAG(profit, 1) OVER (
        PARTITION BY location_id, product_name 
        ORDER BY sales_quarter
    ) AS prev_profit,
    ROUND(
        CASE 
            WHEN LAG(profit, 1) OVER (
                PARTITION BY location_id, product_name 
                ORDER BY sales_quarter
            ) = 0 OR 
            LAG(profit, 1) OVER (
                PARTITION BY location_id, product_name 
                ORDER BY sales_quarter
            ) IS NULL 
            THEN NULL
            ELSE 100.0 * (profit - LAG(profit, 1) OVER (
                PARTITION BY location_id, product_name 
                ORDER BY sales_quarter
            )) / LAG(profit, 1) OVER (
                PARTITION BY location_id, product_name 
                ORDER BY sales_quarter
            )
        END,
        2
    ) AS yoy_profit_pct
FROM with_profit
ORDER BY location_id, product_name, sales_quarter;
