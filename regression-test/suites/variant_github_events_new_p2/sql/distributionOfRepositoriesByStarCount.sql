SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    pow(10, floor(log10(c))) AS stars,
    count(distinct repo)
FROM
(
    SELECT
        cast(v["repo"]["name"] as string) as repo,
        count() AS c
    FROM github_events
    WHERE cast(v["type"] as string) = 'WatchEvent'
    GROUP BY repo
) t
GROUP BY stars
ORDER BY stars ASC
