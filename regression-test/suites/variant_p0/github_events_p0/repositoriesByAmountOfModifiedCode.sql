SELECT
    cast(v:repo.name as string),
    count() AS prs,
    count(distinct cast(v:actor.login as string)) AS authors,
    sum(additions) AS adds,
    sum(deletions) AS dels
FROM github_events
WHERE (cast(v:type as string) = 'PullRequestEvent') AND (cast(v:payload.action as string) = 'opened') AND (additions < 10000) AND (deletions < 10000)
GROUP BY cast(v:repo.name as string)
HAVING (adds / dels) < 10
ORDER BY adds + dels DESC
LIMIT 50
