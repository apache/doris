SELECT
    sum(forks) AS forks,
    sum(stars) AS stars,
    round(sum(stars) / sum(forks), 2) AS ratio
FROM
(
    SELECT
        sum(fork) AS forks,
        sum(star) AS stars
    FROM
    (
        SELECT
            cast(v["repo"]["name"] as string) as repo_name,
            CASE WHEN cast(v["type"] as string) = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
            CASE WHEN cast(v["type"] as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star
        FROM github_events
        WHERE cast(v["type"] as string) IN ('ForkEvent', 'WatchEvent')
    ) t
    GROUP BY repo_name
    HAVING stars > 10
) t2
