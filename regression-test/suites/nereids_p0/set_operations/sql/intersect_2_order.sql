-- database: presto; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: intersect_and_union
SELECT id_employee
FROM tpch_tiny_workers
INTERSECT
SELECT department
FROM tpch_tiny_workers
