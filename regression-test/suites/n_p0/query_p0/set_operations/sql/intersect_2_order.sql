-- database: presto; tables: nation, workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: intersect_and_union
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT id_employee
FROM tpch_tiny_workers
INTERSECT
SELECT department
FROM tpch_tiny_workers
