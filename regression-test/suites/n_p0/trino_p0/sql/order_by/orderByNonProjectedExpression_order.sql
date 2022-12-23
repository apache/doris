SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select o_custkey, o_orderstatus from orders order by o_totalprice*1.0625 limit 20
