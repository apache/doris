SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT count(), repo_name FROM github_events WHERE event_type = 'WatchEvent' GROUP BY repo_name ORDER BY length(repo_name) DESC, repo_name LIMIT 50
