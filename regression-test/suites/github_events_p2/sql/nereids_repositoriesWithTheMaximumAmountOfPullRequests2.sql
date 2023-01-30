SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT repo_name, count(), count(distinct actor_login) AS u FROM github_events WHERE event_type = 'PullRequestEvent' AND action = 'opened' GROUP BY repo_name ORDER BY u DESC, 2 DESC LIMIT 50
