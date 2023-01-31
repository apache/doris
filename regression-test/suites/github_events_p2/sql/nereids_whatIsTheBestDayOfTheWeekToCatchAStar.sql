SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT dayofweek(created_at) AS day, count() AS stars FROM github_events WHERE event_type = 'WatchEvent' GROUP BY day ORDER BY day
