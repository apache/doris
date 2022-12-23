-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

WITH w1 AS (select * from tpch_tiny_nation),
w2 AS (select * from w1)
select count(*) from w1, w2
