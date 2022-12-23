-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

WITH ct AS (SELECT * FROM tpch_tiny_region) SELECT name FROM tpch_tiny_nation where nationkey = 0
