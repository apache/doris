SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT id_employee FROM tpch_tiny_workers
EXCEPT
SELECT department FROM tpch_tiny_workers where department IS NOT NULL
