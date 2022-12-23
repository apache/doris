-- database: presto; groups: limit; tables: partsupp

SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT COUNT(*) FROM (
    SELECT suppkey, COUNT(*) FROM tpch_tiny_partsupp
    GROUP BY suppkey LIMIT 20) t1
