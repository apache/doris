SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT CounterID, min(WatchID), max(WatchID) FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
