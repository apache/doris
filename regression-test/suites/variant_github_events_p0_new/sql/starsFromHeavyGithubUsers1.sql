SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    cast(v["repo"]["name"] as string) as repo_name,
    count()
FROM github_events
WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["actor"]["login"] as string) IN
(
    SELECT cast(v["actor"]["login"] as string)
    FROM github_events
    WHERE (cast(v["type"] as string) = 'PullRequestEvent') AND (cast(v["payload"]["action"] as string) = 'opened')
))
GROUP BY repo_name
ORDER BY count() DESC, repo_name
LIMIT 50
