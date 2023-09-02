-- database: presto; groups: limit; tables: partsupp
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2, parallel_pipeline_task_num=2) */
COUNT(*) FROM (
    SELECT suppkey, COUNT(*) FROM tpch_tiny_partsupp
    GROUP BY suppkey LIMIT 20) t1
