SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_regionkey, count(*), sum(n_regionkey) from nation where n_regionkey > 2 group by 1
