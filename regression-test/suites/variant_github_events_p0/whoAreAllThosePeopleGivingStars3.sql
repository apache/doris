SELECT
    cast(v:repo.name as string),
    count() AS stars
FROM github_events
WHERE (cast(v:type as string) = 'WatchEvent') AND (cast(v:repo.name as string) IN
(
    SELECT cast(v:repo.name as string)
    FROM github_events
    WHERE (cast(v:type as string) = 'WatchEvent') AND (cast(v:actor.login as string) = 'cliffordfajardo')
))
GROUP BY cast(v:repo.name as string)
ORDER BY stars DESC, cast(v:repo.name as string)
LIMIT 50
