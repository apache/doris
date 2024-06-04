SELECT
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(star) / sum(fork), 2) AS ratio
FROM
(
    SELECT
        cast(repo["name"] as string),
        CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
        CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE type IN ('ForkEvent', 'WatchEvent')
) t
