SELECT
    cast(v["repo"]["name"] as string),
    count() AS comments,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE cast(v["type"] as string) = 'CommitCommentEvent'
GROUP BY cast(v["repo"]["name"] as string)
ORDER BY count() DESC, 1, 3
LIMIT 50
