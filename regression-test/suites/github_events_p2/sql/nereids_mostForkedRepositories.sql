SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT repo_name, count() AS forks FROM github_events WHERE event_type = 'ForkEvent' GROUP BY repo_name ORDER BY forks DESC LIMIT 50
