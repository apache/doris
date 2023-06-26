SELECT
    repo_name,
    max(stars) AS daily_stars,
    sum(stars) AS total_stars,
    cast(round(sum(stars) / max(stars), 0) as int) AS rate
FROM
(
    SELECT
        cast(repo:name as string) as repo_name,
        count() AS stars
    FROM github_events
    WHERE type = 'WatchEvent'
    GROUP BY
        repo_name
) t
GROUP BY repo_name
ORDER BY rate DESC, repo_name, 1
LIMIT 50
