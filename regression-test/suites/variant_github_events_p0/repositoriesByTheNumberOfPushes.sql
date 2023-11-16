SELECT
    cast(v:repo.name as string),
    count() AS pushes,
    count(distinct cast(v:actor.login as string)) AS authors
FROM github_events
WHERE (cast(v:type as string) = 'PushEvent') AND (cast(v:repo.name as string) IN
(
    SELECT cast(v:repo.name as string)
    FROM github_events
    WHERE cast(v:type as string) = 'WatchEvent'
    GROUP BY cast(v:repo.name as string)
    ORDER BY count() DESC
    LIMIT 10000
))
GROUP BY cast(v:repo.name as string)
ORDER BY count() DESC, cast(v:repo.name as string)
LIMIT 50
