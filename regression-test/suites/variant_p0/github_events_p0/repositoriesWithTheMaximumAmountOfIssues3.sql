SELECT
    cast(v:repo.name as string),
    sum(issue_created) AS c,
    count(distinct cast(v:actor.login as string)) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        cast(v:repo.name as string),
        CASE WHEN (cast(v:type as string) = 'IssuesEvent') AND (cast(v:payload.action as string) = 'opened') THEN 1 ELSE 0 END AS issue_created,
        CASE WHEN cast(v:type as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star,
        CASE WHEN (cast(v:type as string) = 'IssuesEvent') AND (cast(v:payload.action as string) = 'opened') THEN cast(v:actor.login as string) ELSE NULL END AS cast(v:actor.login as string)
    FROM github_events
    WHERE cast(v:type as string) IN ('IssuesEvent', 'WatchEvent')
) t
GROUP BY cast(v:repo.name as string)
HAVING stars >= 1000
ORDER BY c DESC
LIMIT 50
