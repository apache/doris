SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_regionkey as n_nationkey, n_nationkey as n_regionkey, n_name from nation where n_nationkey < 20 order by n_nationkey desc, n_regionkey asc

