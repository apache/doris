SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT repo_name, count() FROM github_events WHERE event_type = 'IssueCommentEvent' GROUP BY repo_name ORDER BY count() DESC LIMIT 50
