SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT first_name, COUNT(*) FROM workers GROUP BY first_name HAVING first_name IS NULL
