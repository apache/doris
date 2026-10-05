SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT CounterID, min(WatchID), max(WatchID) FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
