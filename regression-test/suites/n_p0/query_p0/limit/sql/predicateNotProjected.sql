-- database: presto; groups: limit; tables: nation

SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT nationkey FROM tpch_tiny_nation WHERE name < 'INDIA'
ORDER BY nationkey LIMIT 3
