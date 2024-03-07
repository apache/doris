SELECT
    repo_name,
    sum(created_at_2022) AS stars2022,
    sum(created_at_2015) AS stars2015,
    cast(round(sum(created_at_2022) / sum(created_at_2015), 0) as int) AS yoy
FROM
(
    SELECT
        cast(repo["name"] as string) as repo_name,
        CASE year(created_at) WHEN 2022 THEN 1 ELSE 0 END AS created_at_2022,
        CASE year(created_at) WHEN 2015 THEN 1 ELSE 0 END AS created_at_2015,
        created_at as created_at
    FROM github_events
    WHERE type = 'WatchEvent'
) t
GROUP BY  repo_name 
HAVING (min(created_at) <= '2023-01-01 00:00:00') AND ((stars2022 >= 1) or (stars2015 >= 1))
ORDER BY yoy DESC, repo_name 
LIMIT 50
