-- database: presto; groups: limit; tables: nation

SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*), regionkey FROM tpch_tiny_nation GROUP BY regionkey
ORDER BY regionkey DESC
LIMIT 2
