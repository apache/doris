-- database: presto; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: except_uniointersect
SELECT name FROM tpch_tiny_nation WHERE nationkey = 17
EXCEPT
SELECT name FROM tpch_tiny_nation WHERE regionkey = 2
UNION
(SELECT name FROM tpch_tiny_nation WHERE regionkey = 2
INTERSECT
SELECT name FROM tpch_tiny_nation WHERE nationkey > 15)
