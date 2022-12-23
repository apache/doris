SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_name from nation where n_nationkey in (select r_regionkey from region)

