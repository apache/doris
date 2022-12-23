SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select count(*) from nation join region on nation.n_regionkey = region.r_regionkey

