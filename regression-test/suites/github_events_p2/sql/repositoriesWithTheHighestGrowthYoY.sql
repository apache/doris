SELECT
    repo_name,
    sum(created_at_2020) AS stars2020,
    sum(created_at_2019) AS stars2019,
    round(sum(created_at_2020) / sum(created_at_2019), 3) AS yoy,
    min(created_at) AS first_seen
FROM
(
    SELECT
        repo_name,
        CASE year(created_at) WHEN 2020 THEN 1 ELSE 0 END AS created_at_2020,
        CASE year(created_at) WHEN 2019 THEN 1 ELSE 0 END AS created_at_2019,
        created_at
    FROM github_events
    WHERE event_type = 'WatchEvent'
) t
GROUP BY repo_name
HAVING (min(created_at) <= '2019-01-01 00:00:00') AND (stars2019 >= 1000)
ORDER BY yoy DESC, repo_name
LIMIT 50
