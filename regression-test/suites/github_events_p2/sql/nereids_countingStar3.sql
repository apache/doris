SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT count() FROM github_events WHERE event_type = 'WatchEvent' AND repo_name IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse') GROUP BY action
