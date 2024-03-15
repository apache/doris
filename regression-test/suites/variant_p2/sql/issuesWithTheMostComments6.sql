SELECT
    cast(repo["name"] as string),
    cast(payload["issue"]["number"] as int)  as number,
    count() AS comments,
    count(distinct cast(actor["login"] as string)) AS authors
FROM github_events
WHERE type = 'IssueCommentEvent' AND (cast(payload["action"] as string) = 'created') AND (cast(payload["issue"]["number"] as int) > 10)
GROUP BY cast(repo["name"] as string), number
HAVING authors >= 4
ORDER BY comments DESC, number
LIMIT 50
