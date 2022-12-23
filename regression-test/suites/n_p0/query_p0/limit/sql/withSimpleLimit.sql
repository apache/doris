-- database: presto; groups: limit; tables: nation

SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT nationkey from tpch_tiny_nation ORDER BY nationkey DESC LIMIT 5
