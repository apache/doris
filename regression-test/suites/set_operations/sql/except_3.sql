-- database: presto; tables: nation, tpch_tiny_workers; groups: set_operation;
-- delimiter: |; ignoreOrder: true;
--! name: except_uniointersect
SELECT id_employee FROM tpch_tiny_workers
EXCEPT
SELECT department FROM tpch_tiny_workers where department IS NOT NULL
