-- database: presto; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: except_uniointersect
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT name FROM tpch_tiny_nation WHERE nationkey = 17
EXCEPT
SELECT name FROM tpch_tiny_nation WHERE regionkey = 2
UNION ALL
(SELECT name FROM tpch_tiny_nation WHERE regionkey = 2
INTERSECT
SELECT name FROM tpch_tiny_nation WHERE nationkey > 15)
