SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(star) / sum(fork), 2) AS ratio
FROM
(
    SELECT
        cast(v["repo"]["name"] as string),
        CASE WHEN cast(v["type"] as string) = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
        CASE WHEN cast(v["type"] as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE cast(v["type"] as string) IN ('ForkEvent', 'WatchEvent')
) t
