SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT actor_login, count() AS stars FROM github_events WHERE event_type = 'WatchEvent' AND actor_login = 'alexey-milovidov' GROUP BY actor_login ORDER BY stars DESC LIMIT 50
