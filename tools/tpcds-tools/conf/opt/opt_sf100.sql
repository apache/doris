set global experimental_enable_nereids_planner=true;
set global experimental_enable_pipeline_engine=true;
set global enable_runtime_filter_prune=false;
set global runtime_filter_wait_time_ms=10000;
set global enable_fallback_to_original_planner=false;
set global forbid_unknown_col_stats=true;
set global max_join_number_bushy_tree=10;
set global memo_max_group_expression_size=15000;
