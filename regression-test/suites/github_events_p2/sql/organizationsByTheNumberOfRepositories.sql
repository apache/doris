SELECT
    lower(split_part(repo_name, '/', 1)) AS org,
    count(distinct repo_name) AS repos
FROM
(
    SELECT repo_name
    FROM github_events
    WHERE event_type = 'WatchEvent'
    GROUP BY repo_name
    HAVING count() >= 10
) t
GROUP BY org
ORDER BY repos DESC, org ASC
LIMIT 50
