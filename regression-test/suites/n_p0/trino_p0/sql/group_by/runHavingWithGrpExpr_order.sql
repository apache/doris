SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*) FROM workers GROUP BY salary * id_department HAVING salary * id_department IS NOT NULL
