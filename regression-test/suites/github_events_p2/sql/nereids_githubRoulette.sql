SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT repo_name FROM github_events WHERE event_type = 'WatchEvent' ORDER BY created_at LIMIT 50
