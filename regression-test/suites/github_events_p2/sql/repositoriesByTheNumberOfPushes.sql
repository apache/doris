SELECT
    repo_name,
    count() AS pushes,
    count(distinct actor_login) AS authors
FROM github_events
WHERE (event_type = 'PushEvent') AND (repo_name IN
(
    SELECT repo_name
    FROM github_events
    WHERE event_type = 'WatchEvent'
    GROUP BY repo_name
    ORDER BY count() DESC
    LIMIT 10000
))
GROUP BY repo_name
ORDER BY count() DESC
LIMIT 50
