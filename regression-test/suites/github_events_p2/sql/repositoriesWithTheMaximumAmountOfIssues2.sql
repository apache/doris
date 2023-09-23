SELECT
    repo_name,
    sum(issue_created) AS c,
    count(distinct actor_login) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        repo_name,
        CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN 1 ELSE 0 END AS issue_created,
        CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star,
        CASE WHEN (event_type = 'IssuesEvent') AND (action = 'opened') THEN actor_login ELSE NULL END AS actor_login
    FROM github_events
    WHERE event_type IN ('IssuesEvent', 'WatchEvent')
) t
GROUP BY repo_name
ORDER BY c DESC, repo_name
LIMIT 50
