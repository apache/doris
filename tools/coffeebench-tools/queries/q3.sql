WITH season_discount AS (
    SELECT
        l.city,
        l.state,
        f.season,
        AVG(f.discount_percentage) AS avg_discount
    FROM fact_sales f
    JOIN dim_locations l
        ON f.location_id = l.location_id
    GROUP BY
        l.city,
        l.state,
        f.season
)
SELECT
    city,
    state,
    season,
    avg_discount,
    discount_rank
FROM (
    SELECT
        city,
        state,
        season,
        avg_discount,
        DENSE_RANK() OVER (PARTITION BY season ORDER BY avg_discount DESC) AS discount_rank
    FROM season_discount
) t
WHERE discount_rank <= 3
ORDER BY season, discount_rank;
