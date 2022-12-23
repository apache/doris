SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT n_regionkey, COUNT(null) FROM nation WHERE n_nationkey > 5 GROUP BY n_regionkey
