SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT p_type, COUNT(*) FROM part GROUP BY p_type HAVING COUNT(*) > 20 and AVG(p_retailprice) > 1000
