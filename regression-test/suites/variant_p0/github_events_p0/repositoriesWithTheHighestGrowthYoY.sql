SELECT
    cast(v:repo.name as string),
    sum(cast(v:created_at as datetime)_2020) AS stars2020,
    sum(cast(v:created_at as datetime)_2019) AS stars2019,
    round(sum(cast(v:created_at as datetime)_2020) / sum(cast(v:created_at as datetime)_2019), 3) AS yoy,
    min(cast(v:created_at as datetime)) AS first_seen
FROM
(
    SELECT
        cast(v:repo.name as string),
        CASE year(cast(v:created_at as datetime)) WHEN 2020 THEN 1 ELSE 0 END AS cast(v:created_at as datetime)_2020,
        CASE year(cast(v:created_at as datetime)) WHEN 2019 THEN 1 ELSE 0 END AS cast(v:created_at as datetime)_2019,
        cast(v:created_at as datetime)
    FROM github_events
    WHERE cast(v:type as string) = 'WatchEvent'
) t
GROUP BY cast(v:repo.name as string)
HAVING (min(cast(v:created_at as datetime)) <= '2019-01-01 00:00:00') AND (stars2019 >= 1000)
ORDER BY yoy DESC, cast(v:repo.name as string)
LIMIT 50
