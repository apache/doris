SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*) FROM workers HAVING SUM(salary * 2)/COUNT(*) > 0
