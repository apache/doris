
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*) FROM (SELECT n1.regionkey, n1.nationkey FROM tpch_tiny_nation n1 JOIN tpch_tiny_nation n2 ON n1.regionkey = n2.regionkey LIMIT 5) foo
