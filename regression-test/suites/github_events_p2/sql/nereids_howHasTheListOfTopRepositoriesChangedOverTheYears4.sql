SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT repo_name, count() AS stars FROM github_events WHERE event_type = 'WatchEvent' AND year(created_at) = '2018' GROUP BY repo_name ORDER BY stars DESC LIMIT 50
