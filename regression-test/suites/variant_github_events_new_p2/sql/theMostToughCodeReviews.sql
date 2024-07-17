SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    concat('https://github.com/', cast(v["repo"]["name"] as string), '/pull/') AS URL,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE (cast(v["type"] as string) = 'PullRequestReviewCommentEvent') AND (cast(v["payload"]["action"] as string) = 'created')
GROUP BY
    cast(v["repo"]["name"] as string),
    cast(v["payload"]["issue"]["number"] as string) 
ORDER BY authors DESC, URL ASC
LIMIT 50