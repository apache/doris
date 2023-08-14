SELECT
    concat('https://github.com/', cast(v:repo.name as string), '/pull/', CAST(number AS STRING)) AS URL,
    count(distinct cast(v:actor.login as string)) AS authors
FROM github_events
WHERE (cast(v:type as string) = 'PullRequestReviewCommentEvent') AND (cast(v:payload.action as string) = 'created')
GROUP BY
    cast(v:repo.name as string),
    number
ORDER BY authors DESC, URL ASC
LIMIT 50
