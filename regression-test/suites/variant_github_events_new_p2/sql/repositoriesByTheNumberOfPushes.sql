SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    cast(v["repo"]["name"] as string) as repo_name,
    count() AS pushes,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE (cast(v["type"] as string) = 'PushEvent') AND (cast(v["repo"]["name"] as string) IN
(
    SELECT cast(v["repo"]["name"] as string) as repo_name
    FROM github_events
    WHERE cast(v["type"] as string) = 'WatchEvent'
    GROUP BY repo_name
    ORDER BY count() DESC
    LIMIT 10000
))
GROUP BY repo_name
ORDER BY count() DESC, repo_name
LIMIT 50
