SELECT
    cast(v["repo"]["name"] as string),
    cast(v["payload"]["issue"]["number"] as int)  as number,
    count() AS comments,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE cast(v["type"] as string) = 'IssueCommentEvent' AND (cast(v["payload"]["action"] as string) = 'created') AND (cast(v["payload"]["issue"]["number"] as int) > 10)
GROUP BY cast(v["repo"]["name"] as string), number
HAVING authors >= 4
ORDER BY comments DESC, cast(v["repo"]["name"] as string)
LIMIT 50
