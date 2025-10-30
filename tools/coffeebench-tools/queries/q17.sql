WITH raw_agg AS (
    SELECT
        l.state,
        f.season,
        p.category,
        SUM(f.sales_amount)       AS total_sales,
        SUM(f.quantity)           AS total_units,
        COUNT(DISTINCT f.order_id) AS order_count
    FROM fact_sales AS f
    JOIN dim_products AS p
      ON f.product_id = p.product_id
     AND DATE(f.order_date) BETWEEN DATE(p.from_date) AND DATE(p.to_date)
    JOIN dim_locations AS l
      ON f.location_id = l.location_id
    WHERE
        l.region        = 'West'
      AND f.time_of_day  = 'Morning'
      AND p.name         = 'Frappe'
      AND DATE(f.order_date)  BETWEEN DATE('2023-01-01') AND DATE('2024-06-30')
    GROUP BY
        l.state,
        f.season,
        p.category
),
seasonal_data AS (
    SELECT
        state,
        season,
        category,
        total_sales,
        total_units,
        order_count,
        SUM(total_sales) OVER (PARTITION BY state, season) AS season_total_sales
    FROM raw_agg
),
ranked AS (
    SELECT
        state,
        season,
        category,
        total_sales,
        total_units,
        order_count,
        season_total_sales,
        ROUND(100.0 * total_sales / season_total_sales, 2) AS pct_of_season,
        DENSE_RANK() OVER (
          PARTITION BY state, season
          ORDER BY total_sales DESC
        ) AS category_rank
    FROM seasonal_data
)
SELECT
    state,
    season,
    category,
    total_sales,
    total_units,
    order_count,
    pct_of_season,
    category_rank
FROM ranked
WHERE category_rank <= 3
ORDER BY
    state,
    season,
    category_rank;
