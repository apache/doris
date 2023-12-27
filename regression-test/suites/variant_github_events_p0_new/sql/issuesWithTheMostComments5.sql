SELECT
    cast(v["repo"]["name"] as string) as repo_name,
    cast(v["payload"]["issue"]["number"] as int)  as number,
    count() AS comments
FROM github_events
WHERE cast(v["type"] as string) = 'IssueCommentEvent' AND (cast(v["payload"]["action"] as string) = 'created') AND (cast(v["payload"]["issue"]["number"] as int)  > 10)
GROUP BY repo_name, number
ORDER BY comments DESC, repo_name, number
LIMIT 50
