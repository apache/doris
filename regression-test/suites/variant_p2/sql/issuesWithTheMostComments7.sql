SELECT
    cast(repo:name as string),
    count() AS comments,
    count(distinct cast(actor:login as string)) AS authors
FROM github_events
WHERE type = 'CommitCommentEvent'
GROUP BY cast(repo:name as string)
ORDER BY count() DESC, 1, 3
LIMIT 50
