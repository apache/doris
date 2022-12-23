SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*) FROM workers GROUP BY id_department * 2 HAVING SUM(log10(salary + 1)) > 0
