SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_nationkey, n_name from nation order by n_regionkey, n_nationkey
