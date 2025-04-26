SELECT
    repo_name,
    sum(issue_created) AS c,
    count(distinct actor_login) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        cast(v["repo"]["name"] as string) as repo_name,
        CASE WHEN (cast(v["type"] as string) = 'IssuesEvent') AND (cast(v["payload"]["action"] as string) = 'opened') THEN 1 ELSE 0 END AS issue_created,
        CASE WHEN cast(v["type"] as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star,
        CASE WHEN (cast(v["type"] as string) = 'IssuesEvent') AND (cast(v["payload"]["action"] as string) = 'opened') THEN cast(v["actor"]["login"] as string) ELSE NULL END AS actor_login 
    FROM github_events
    WHERE cast(v["type"] as string) IN ('IssuesEvent', 'WatchEvent')
) t
GROUP BY repo_name
ORDER BY u, c, stars DESC, 1
LIMIT 50
