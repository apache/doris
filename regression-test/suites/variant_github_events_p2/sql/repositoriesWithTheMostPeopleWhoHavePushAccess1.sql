SELECT
    repo_name,
    count(distinct actor_login) AS u,
    sum(star) AS stars
FROM
(
    SELECT
        lower(cast(v["repo"]["name"] as string)) as repo_name,
        CASE WHEN cast(v["type"] as string) = 'PushEvent' THEN cast(v["actor"]["login"] as string) ELSE NULL END AS actor_login,
        CASE WHEN cast(v["type"] as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events WHERE cast(v["type"] as string) IN ('PushEvent', 'WatchEvent') AND cast(v["repo"]["name"] as string) != '/'
) t
GROUP BY repo_name ORDER BY u, stars, repo_name DESC LIMIT 50
