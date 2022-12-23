-- database: presto; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: intersect_and_union
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT name FROM tpch_tiny_nation WHERE nationkey = 17
INTERSECT 
SELECT name FROM tpch_tiny_nation WHERE regionkey = 1 
UNION 
SELECT name FROM tpch_tiny_nation WHERE regionkey = 2
