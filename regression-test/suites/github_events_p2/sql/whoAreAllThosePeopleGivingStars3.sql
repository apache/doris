SELECT
    repo_name,
    count() AS stars
FROM github_events
WHERE (event_type = 'WatchEvent') AND (repo_name IN
(
    SELECT repo_name
    FROM github_events
    WHERE (event_type = 'WatchEvent') AND (actor_login = 'alexey-milovidov')
))
GROUP BY repo_name
ORDER BY stars DESC
LIMIT 50
