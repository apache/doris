SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    cast(v["repo"]["name"] as string) as repo_name,
    count() AS comments,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE cast(v["type"] as string) = 'CommitCommentEvent'
GROUP BY repo_name
ORDER BY count() DESC, 1, 3
LIMIT 50
