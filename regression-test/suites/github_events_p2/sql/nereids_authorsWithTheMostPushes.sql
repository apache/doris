SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
-- SELECT
--       actor_login,
--       count() AS c,
--       count(distinct repo_name) AS repos
--   FROM github_events
--   WHERE event_type = 'PushEvent'
--   GROUP BY actor_login
--   ORDER BY c DESC
--   LIMIT 50
