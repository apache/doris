SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_name, p_name from nation left outer join part on n_regionkey = p_partkey and n_name = p_name
