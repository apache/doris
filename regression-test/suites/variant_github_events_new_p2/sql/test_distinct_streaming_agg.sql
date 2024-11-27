SELECT
    /*+SET_VAR(batch_size=50,experimental_enable_pipeline_x_engine=false,parallel_pipeline_task_num=1,disable_streaming_preaggregations=false) */
    count(distinct v)
FROM
    github_events_2;