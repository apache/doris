SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT repo_name, count() AS c, count(distinct actor_login) AS u FROM github_events WHERE event_type = 'IssuesEvent' AND action = 'opened' GROUP BY repo_name ORDER BY c DESC, repo_name LIMIT 50
