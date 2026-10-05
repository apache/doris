SELECT
    repo_name,
    count(distinct actor_login) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        lower(cast(repo["name"] as string)) as repo_name,
        CASE WHEN type = 'PushEvent' THEN cast(actor["login"] as string) ELSE NULL END AS actor_login,
        CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events WHERE type IN ('PushEvent', 'WatchEvent') AND cast(repo["name"] as string) != '/'
) t
GROUP BY repo_name ORDER BY u, stars, repo_name DESC LIMIT 50
