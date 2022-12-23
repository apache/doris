-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

WITH nested AS (SELECT * FROM tpch_tiny_nation) SELECT count(*) FROM (select * FROM nested) as a
