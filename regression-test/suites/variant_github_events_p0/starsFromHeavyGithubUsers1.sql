SELECT
    cast(v:repo.name as string),
    count()
FROM github_events
WHERE (cast(v:type as string) = 'WatchEvent') AND (cast(v:actor.login as string) IN
(
    SELECT cast(v:actor.login as string)
    FROM github_events
    WHERE (cast(v:type as string) = 'PullRequestEvent') AND (cast(v:payload.action as string) = 'opened')
))
GROUP BY cast(v:repo.name as string)
ORDER BY count() DESC, cast(v:repo.name as string)
LIMIT 50
