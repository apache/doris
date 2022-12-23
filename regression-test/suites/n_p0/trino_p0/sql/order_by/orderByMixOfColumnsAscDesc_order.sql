SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select o_orderdate, o_orderpriority, o_custkey from orders order by 1 desc, 2, 3 desc limit 20

