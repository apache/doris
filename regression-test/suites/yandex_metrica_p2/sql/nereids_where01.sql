SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT CounterID, count(distinct UserID) FROM hits WHERE 0 != 0 GROUP BY CounterID
