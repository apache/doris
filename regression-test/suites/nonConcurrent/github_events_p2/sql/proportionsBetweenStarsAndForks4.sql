SELECT
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(star) / sum(fork), 2) AS ratio
FROM
(
    SELECT
        repo_name,
        CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
        CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE event_type IN ('ForkEvent', 'WatchEvent')
) t
