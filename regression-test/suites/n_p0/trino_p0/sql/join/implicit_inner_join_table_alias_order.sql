SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n.n_name, r.r_name from nation n, region r where n.n_regionkey = r.r_regionkey

