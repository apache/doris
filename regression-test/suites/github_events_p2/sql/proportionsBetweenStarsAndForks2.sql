SELECT
    repo_name,
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(star) / sum(fork), 3) AS ratio
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
HAVING (stars > 100) AND (forks > 100)
ORDER BY ratio DESC, repo_name
LIMIT 50
