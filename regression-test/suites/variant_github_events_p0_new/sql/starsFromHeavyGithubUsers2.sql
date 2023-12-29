SELECT
    cast(v["repo"]["name"] as string) as repo_name,
    count()
FROM github_events
WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["actor"]["login"] as string) IN
(
    SELECT cast(v["actor"]["login"] as string) as actor_login
    FROM github_events
    WHERE (cast(v["type"] as string) = 'PullRequestEvent') AND (cast(v["payload"]["action"] as string) = 'opened')
    GROUP BY actor_login
    HAVING count() >= 2
))
GROUP BY repo_name
ORDER BY 1, count() DESC, 1
LIMIT 50
