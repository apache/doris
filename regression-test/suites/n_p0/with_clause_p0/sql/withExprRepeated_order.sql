-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

WITH wnation AS (SELECT name, nationkey, regionkey FROM tpch_tiny_nation)
SELECT n1.name, n2.name FROM wnation n1 JOIN wnation n2
ON n1.nationkey=n2.regionkey
