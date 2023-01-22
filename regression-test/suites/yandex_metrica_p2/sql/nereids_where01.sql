SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT CounterID, count(distinct UserID) FROM hits WHERE 0 != 0 GROUP BY CounterID
