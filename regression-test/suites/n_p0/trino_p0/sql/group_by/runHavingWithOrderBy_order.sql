SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT id_department, COUNT(*) FROM workers GROUP BY id_department HAVING COUNT(*) > 1 ORDER BY id_department desc
