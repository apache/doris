-- database: presto; groups: limit; tables: nation

SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*) FROM (SELECT * FROM tpch_tiny_nation LIMIT 2) AS foo LIMIT 5
