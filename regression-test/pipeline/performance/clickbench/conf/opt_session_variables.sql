set global exec_mem_limit=34359738368;
set global parallel_fragment_exec_instance_num=16;
set global parallel_pipeline_task_num=16;
set global enable_single_distinct_column_opt=true;
set global enable_function_pushdown=true;
set global forbid_unknown_col_stats=false;
set global runtime_filter_mode=GLOBAL;
