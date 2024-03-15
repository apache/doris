SELECT
    repo_name,
    sum(fork) AS forks,
    sum(star) AS stars,
    cast(round(sum(star) / sum(fork), 0) as int) AS ratio
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
ORDER BY forks DESC, 1, 3, 4
LIMIT 50
