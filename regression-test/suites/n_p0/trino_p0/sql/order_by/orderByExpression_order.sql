SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select o_totalprice*1.0625, o_custkey from orders order by 1 limit 20
