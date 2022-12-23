SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select distinct p_brand from part where p_partkey < 15 order by 1 desc
