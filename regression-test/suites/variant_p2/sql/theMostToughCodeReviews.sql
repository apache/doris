SELECT
    concat('https://github.com/', cast(repo:name as string), '/pull/') AS URL,
    count(distinct cast(actor:login as string)) AS authors
FROM github_events
WHERE (type = 'PullRequestReviewCommentEvent') AND (cast(payload:action as string) = 'created')
GROUP BY
    cast(repo:name as string),
    cast(payload:issue.`number` as string) 
ORDER BY authors DESC, URL ASC
LIMIT 50