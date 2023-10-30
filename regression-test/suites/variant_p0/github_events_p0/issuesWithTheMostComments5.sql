SELECT
    cast(v:repo.name as string),
    cast(v:payload.issue.`number` as int)  as number,
    count() AS comments
FROM github_events
WHERE cast(v:type as string) = 'IssueCommentEvent' AND (cast(v:payload.action as string) = 'created') AND (cast(v:payload.issue.`number` as int)  > 10)
GROUP BY cast(v:repo.name as string), number
ORDER BY comments DESC, cast(v:repo.name as string), number
LIMIT 50
