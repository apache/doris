-- database: presto; groups: limit; tables: nation

SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT foo.c, foo.regionkey FROM
    (SELECT regionkey, COUNT(*) AS c FROM tpch_tiny_nation
    GROUP BY regionkey ORDER BY regionkey LIMIT 2) foo
