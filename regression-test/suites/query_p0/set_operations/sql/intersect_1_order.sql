-- database: presto; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: intersect_and_union
SELECT name FROM tpch_tiny_nation WHERE nationkey = 17 
INTERSECT 
SELECT name FROM tpch_tiny_nation WHERE regionkey = 1 
UNION 
SELECT name FROM tpch_tiny_nation WHERE regionkey = 2
