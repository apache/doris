SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    cast(v["repo"]["name"] as string) as repo_name,
    cast(v["payload"]["issue"]["number"] as int)  as number,
    count() AS comments,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE cast(v["type"] as string) = 'IssueCommentEvent' AND (cast(v["payload"]["action"] as string) = 'created') AND (cast(v["payload"]["issue"]["number"] as int) > 10)
GROUP BY repo_name, number
HAVING authors >= 4
ORDER BY comments DESC, repo_name
LIMIT 50
