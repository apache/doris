-- database: presto; groups: limit; tables: nation
SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=2, parallel_pipeline_task_num=2) */
COUNT(*) FROM (SELECT * FROM tpch_tiny_nation LIMIT 2) AS foo LIMIT 5
