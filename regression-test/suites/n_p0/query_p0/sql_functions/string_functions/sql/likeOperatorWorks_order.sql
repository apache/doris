SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;
select name from tpch_tiny_nation where name like '%AN'
