SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
-- SELECT body, count() FROM github_events WHERE body != "" AND length(body) < 100 GROUP BY body ORDER BY count() DESC LIMIT 50
