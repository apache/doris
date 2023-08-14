SELECT
    cast(v:repo.name as string),
    number,
    count() AS comments
FROM github_events
WHERE cast(v:type as string) = 'IssueCommentEvent' AND (cast(v:payload.action as string) = 'created')
GROUP BY cast(v:repo.name as string), number
ORDER BY comments DESC, number ASC
LIMIT 50
