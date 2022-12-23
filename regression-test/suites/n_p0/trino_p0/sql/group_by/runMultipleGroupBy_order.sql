SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*), n_regionkey, n_nationkey FROM nation WHERE n_regionkey < 2 GROUP BY n_nationkey, n_regionkey ORDER BY n_regionkey, n_nationkey DESC
