SET enable_nereids_planner=true;
SET enable_vectorized_engine=true;
SET enable_fallback_to_original_planner=false;

SELECT count() FROM hits WHERE UserID IN (SELECT UserID FROM hits WHERE CounterID = 800784)
