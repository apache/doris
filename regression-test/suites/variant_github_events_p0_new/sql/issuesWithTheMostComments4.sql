SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    cast(v["repo"]["name"] as string) as repo_name,
    cast(v["payload"]["issue"]["number"] as int)  as number,
    count() AS comments
FROM github_events
WHERE cast(v["type"] as string) = 'IssueCommentEvent' AND (cast(v["payload"]["action"] as string) = 'created')
GROUP BY repo_name, number 
ORDER BY comments DESC, number ASC, 1
LIMIT 50
