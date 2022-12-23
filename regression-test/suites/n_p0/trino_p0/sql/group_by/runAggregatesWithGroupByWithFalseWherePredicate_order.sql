SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select count(*), sum(n_nationkey) from nation where 1=2 group by n_regionkey
