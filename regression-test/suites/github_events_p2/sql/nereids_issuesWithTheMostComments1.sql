SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT count() FROM github_events WHERE event_type = 'IssueCommentEvent'
