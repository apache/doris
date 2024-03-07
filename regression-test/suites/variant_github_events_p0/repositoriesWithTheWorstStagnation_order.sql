SELECT
    repo_name,
    sum(created_at_2022) AS stars2022,
    sum(created_at_2015) AS stars2015,
    round(sum(created_at_2022) / sum(created_at_2015), 3) AS yoy,
    min(created_at) AS first_seen
FROM
(
    SELECT
        cast(v["repo"]["name"] as string) as repo_name,
        CASE year(cast(v["created_at"] as datetime)) WHEN 2022 THEN 1 ELSE 0 END AS created_at_2022,
        CASE year(cast(v["created_at"] as datetime)) WHEN 2015 THEN 1 ELSE 0 END AS created_at_2015,
        cast(v["created_at"] as datetime) as created_at
    FROM github_events
    WHERE cast(v["type"] as string) = 'WatchEvent'
) t
GROUP BY repo_name
HAVING (min(created_at) <= '2019-01-01 00:00:00') AND (max(created_at) >= '2020-06-01 00:00:00') AND (stars2015 >= 2)
ORDER BY yoy, repo_name
LIMIT 50
