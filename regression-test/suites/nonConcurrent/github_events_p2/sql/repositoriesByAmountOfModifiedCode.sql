SELECT
    repo_name,
    count() AS prs,
    count(distinct actor_login) AS authors,
    sum(additions) AS adds,
    sum(deletions) AS dels
FROM github_events
WHERE (event_type = 'PullRequestEvent') AND (action = 'opened') AND (additions < 10000) AND (deletions < 10000)
GROUP BY repo_name
HAVING (adds / dels) < 10
ORDER BY adds + dels DESC
LIMIT 50
