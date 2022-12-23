SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(DISTINCT n_regionkey), COUNT(DISTINCT n_name), MIN(DISTINCT n_nationkey) FROM nation
