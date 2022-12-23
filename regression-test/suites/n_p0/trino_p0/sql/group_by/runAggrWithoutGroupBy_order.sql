SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select count(*), count(n_regionkey), min(n_regionkey), max(n_regionkey), sum(n_regionkey) from nation
