SELECT
    cast(v:repo.name as string),
    count()
FROM github_events
WHERE (cast(v:type as string) = 'WatchEvent') AND (cast(v:actor.login as string) IN
(
    SELECT cast(v:actor.login as string)
    FROM github_events
    WHERE (cast(v:type as string) = 'PullRequestEvent') AND (cast(v:payload.action as string) = 'opened')
    GROUP BY cast(v:actor.login as string)
    HAVING count() >= 2
))
GROUP BY cast(v:repo.name as string)
ORDER BY 1, count() DESC, 1
LIMIT 50
