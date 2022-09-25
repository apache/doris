SELECT
    repo_name,
    count() AS prs,
    count(distinct actor_login) AS authors
FROM github_events
WHERE (event_type = 'IssuesEvent') AND (action = 'opened') AND (actor_login IN
(
    SELECT actor_login
    FROM github_events
    WHERE (event_type = 'IssuesEvent') AND (action = 'opened') AND (repo_name IN ('yandex/ClickHouse', 'ClickHouse/ClickHouse'))
)) AND (lower(repo_name) NOT LIKE '%clickhouse%')
GROUP BY repo_name
ORDER BY authors DESC, prs DESC, repo_name ASC
LIMIT 50
