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
            repo_name,
            CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
            CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
        FROM github_events
        WHERE event_type IN ('ForkEvent', 'WatchEvent')
    ) t
    GROUP BY repo_name
    HAVING stars > 100
) t2
