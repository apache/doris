SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT n_regionkey, COUNT(DISTINCT n_name) FROM nation
GROUP BY n_regionkey
HAVING n_regionkey < 4
