SELECT
    lower(split_part(repo_name, '/', 1)) AS org,
    count(distinct repo_name) AS repos
FROM
(
    SELECT cast(repo:name as string) as repo_name
    FROM github_events
    WHERE type = 'WatchEvent'
    GROUP BY cast(repo:name as string)
    HAVING count() >= 10
) t
GROUP BY org
ORDER BY repos DESC, org ASC
LIMIT 50
