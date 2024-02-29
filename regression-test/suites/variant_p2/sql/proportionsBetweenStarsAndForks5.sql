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
            cast(repo["name"] as string) as repo_name,
            CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
            CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END AS star
        FROM github_events
        WHERE type IN ('ForkEvent', 'WatchEvent')
    ) t
    GROUP BY repo_name
    HAVING stars > 10
) t2
