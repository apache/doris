SELECT
    repo_name,
    stars
FROM
(
    SELECT
        row_number() OVER (PARTITION BY repo_name  ORDER BY stars DESC) AS rank,
        repo_name,
        stars
    FROM
    (
        SELECT
            cast(repo:name as string) as repo_name,
            count() AS stars
        FROM github_events
        WHERE type = 'WatchEvent'
        GROUP BY cast(repo:name as string)
    ) t1
) t2
WHERE rank = 1
ORDER BY stars DESC, repo_name, 1
LIMIT 50
