SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT count(distinct actor_login) FROM github_events WHERE event_type = 'PushEvent'
