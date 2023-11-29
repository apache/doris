SELECT
    cast(repo:name as string),
    cast(payload:issue.`number` as int)  as number,
    count() AS comments
FROM github_events
WHERE type = 'IssueCommentEvent' AND (cast(payload:action as string) = 'created')
GROUP BY cast(repo:name as string), number 
ORDER BY comments DESC, number ASC, 1
LIMIT 50

