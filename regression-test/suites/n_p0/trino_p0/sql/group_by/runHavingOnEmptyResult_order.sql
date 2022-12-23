SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(n_regionkey) FROM nation WHERE 1=2 HAVING SUM(n_regionkey) IS NULL
