SELECT
    f.order_date,
    f.product_name,
    p.standard_price,
    p.standard_cost,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.sales_amount) AS total_sales_amount,
    (p.standard_price - p.standard_cost) * SUM(f.quantity) AS theoretical_margin
FROM fact_sales f
JOIN dim_products p
    ON f.product_name = p.name
    AND f.order_date BETWEEN p.from_date AND p.to_date
GROUP BY
    f.order_date,
    f.product_name,
    p.standard_price,
    p.standard_cost
ORDER BY
    f.order_date,
    f.product_name;
