-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

WITH ordered AS (select nationkey a, regionkey b, name c from tpch_tiny_nation order by 1,2 limit 10)
select * from  ordered order by 1,2 limit 5
