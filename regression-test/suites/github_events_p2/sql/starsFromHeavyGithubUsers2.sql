SELECT
    repo_name,
    count()
FROM github_events
WHERE (event_type = 'WatchEvent') AND (actor_login IN
(
    SELECT actor_login
    FROM github_events
    WHERE (event_type = 'PullRequestEvent') AND (action = 'opened')
    GROUP BY actor_login
    HAVING count() >= 10
))
GROUP BY repo_name
ORDER BY count() DESC
LIMIT 50
