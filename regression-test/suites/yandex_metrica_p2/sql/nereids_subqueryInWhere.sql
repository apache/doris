SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT count() FROM hits WHERE UserID IN (SELECT UserID FROM hits WHERE CounterID = 800784)
