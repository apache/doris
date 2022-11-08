SELECT id_employee FROM tpch_tiny_workers
EXCEPT
SELECT department FROM tpch_tiny_workers where department IS NOT NULL
