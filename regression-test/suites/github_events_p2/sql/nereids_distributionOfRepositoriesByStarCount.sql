SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT
    pow(10, floor(log10(c))) AS stars,
    count(distinct k)
FROM
(
    SELECT
        repo_name AS k,
        count() AS c
    FROM github_events
    WHERE event_type = 'WatchEvent'
    GROUP BY k
) t
GROUP BY stars
ORDER BY stars ASC
