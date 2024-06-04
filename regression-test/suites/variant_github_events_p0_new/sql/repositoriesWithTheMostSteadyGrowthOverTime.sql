SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    repo_name,
    max(stars) AS daily_stars,
    sum(stars) AS total_stars,
    sum(stars) / max(stars) AS rate
FROM
(
    SELECT
        cast(v["repo"]["name"] as string) as repo_name,
        to_date(cast(v["created_at"] as datetime)) AS day,
        count() AS stars
    FROM github_events
    WHERE cast(v["type"] as string) = 'WatchEvent'
    GROUP BY
        repo_name,
        day
) t
GROUP BY repo_name
ORDER BY rate DESC, 1
LIMIT 50
