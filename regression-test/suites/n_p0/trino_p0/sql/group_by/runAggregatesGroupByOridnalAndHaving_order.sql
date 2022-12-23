SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_regionkey, count(*) from nation group by 1 having sum(n_regionkey) > 5 and sum(n_regionkey) < 20
