SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_nationkey, n_regionkey, n_name from (select n_regionkey, n_nationkey, n_name from nation where n_nationkey < 20 order by 2 desc limit 5) t order by 2, 1 asc
