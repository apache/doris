---
{
    "title": "BE Configuration",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<!-- Please sort the configuration alphabetically -->

# BE Configuration

This document mainly introduces the relevant configuration items of BE.

## View configuration items
(TODO)

## Set configuration items
(TODO)

## Examples
(TODO)

## Configurations

### `alter_tablet_worker_count`

### `base_compaction_check_interval_seconds`

### `base_compaction_interval_seconds_since_last_operation`

### `base_compaction_num_cumulative_deltas`

### `base_compaction_num_threads_per_disk`

### `base_compaction_write_mbytes_per_sec`

### `base_cumulative_delta_ratio`

### `be_port`

### `be_service_threads`

### `brpc_max_body_size`

This configuration is mainly used to modify the parameter `max_body_size` of brpc.

Sometimes the query fails and an error message of `body_size is too large` will appear in the BE log. This may happen when the SQL mode is "multi distinct + no group by + more than 1T of data".

This error indicates that the packet size of brpc exceeds the configured value. At this time, you can avoid this error by increasing the configuration.

Since this is a brpc configuration, users can also modify this parameter directly during operation. Modify by visiting `http://be_host:brpc_port/flags`.

### `brpc_socket_max_unwritten_bytes`

This configuration is mainly used to modify the parameter `socket_max_unwritten_bytes` of brpc.

Sometimes the query fails and an error message of `The server is overcrowded` will appear in the BE log. This means there are too many messages to buffer at the sender side, which may happen when the SQL needs to send large bitmap value. You can avoid this error by increasing the configuration.

Since this is a brpc configuration, users can also modify this parameter directly during operation. Modify by visiting `http://be_host:brpc_port/flags`.

### `brpc_num_threads`

This configuration is mainly used to modify the number of bthreads for brpc. The default value is set to -1, which means the number of bthreads is #cpu-cores.

User can set this configuration to a larger value to get better QPS performance. For more information, please refer to `https://github.com/apache/incubator-brpc/blob/master/docs/cn/benchmark.md`

### `brpc_port`

### `buffer_pool_clean_pages_limit`

### `buffer_pool_limit`

### `check_consistency_worker_count`

### `chunk_reserved_bytes_limit`

### `clear_transaction_task_worker_count`

### `clone_worker_count`

### `cluster_id`

### `column_dictionary_key_ratio_threshold`

### `column_dictionary_key_size_threshold`

### `compress_rowbatches`

### `create_tablet_worker_count`

### `cumulative_compaction_budgeted_bytes`

### `cumulative_compaction_check_interval_seconds`

### `cumulative_compaction_num_threads_per_disk`

### `cumulative_compaction_skip_window_seconds`

### `default_num_rows_per_column_file_block`

### `default_query_options`

### `default_rowset_type`

### `delete_worker_count`

### `disable_mem_pools`

### `disable_storage_page_cache`

### `disk_stat_monitor_interval`

### `doris_cgroups`

### `doris_max_pushdown_conjuncts_return_rate`

### `doris_max_scan_key_num`

* Type: int
* Description: Used to limit the maximum number of scan keys that a scan node can split in a query request. When a conditional query request reaches the scan node, the scan node will try to split the conditions related to the key column in the query condition into multiple scan key ranges. After that, these scan key ranges will be assigned to multiple scanner threads for data scanning. A larger value usually means that more scanner threads can be used to increase the parallelism of the scanning operation. However, in high concurrency scenarios, too many threads may bring greater scheduling overhead and system load, and will slow down the query response speed. An empirical value is 50. This configuration can be configured separately at the session level. For details, please refer to the description of `max_scan_key_num` in [Variables](../variables.md).
* Default value: 1024

When the concurrency cannot be improved in high concurrency scenarios, try to reduce this value and observe the impact.

### `doris_scan_range_row_count`

### `doris_scanner_queue_size`

### `doris_scanner_row_num`

### `doris_scanner_thread_pool_queue_size`

### `doris_scanner_thread_pool_thread_num`

### `download_low_speed_limit_kbps`

### `download_low_speed_time`

### `download_worker_count`

### `drop_tablet_worker_count`

### `enable_metric_calculator`

### `enable_partitioned_aggregation`

### `enable_prefetch`

### `enable_quadratic_probing`

### `enable_system_metrics`

### `enable_token_check`

### `es_http_timeout_ms`

### `es_scroll_keepalive`

### `etl_thread_pool_queue_size`

### `etl_thread_pool_size`

### `exchg_node_buffer_size_bytes`

### `file_descriptor_cache_capacity`

### `file_descriptor_cache_clean_interval`

### `flush_thread_num_per_store`

### `force_recovery`

### `fragment_pool_queue_size`

### `fragment_pool_thread_num`

### `heartbeat_service_port`

### `heartbeat_service_thread_count`

### `ignore_broken_disk`

### `ignore_load_tablet_failure`
When BE starts, it will check all the paths under the `storage_root_path` in  configuration.

- `ignore_broken_disk=true`

  If the path does not exist or the file under the path cannot be read or written (broken disk), it will be ignored. If there are any other available paths, the startup will not be interrupted.

- `ignore_broken_disk=false`

  If the path does not exist or the file under the path cannot be read or written (bad disk), the startup will fail and exit.

The default value is `false`.

### inc_rowset_expired_sec

* Type: boolean
* Description: Whether to continue to start be when load tablet from header failed.
* Default: false

When the BE starts, it will start a separate thread for each data directory to load the tablet header meta information. In the default configuration, if a tablet fails to load its header, the startup process is terminated. At the same time, you will see the following error message in the `be.INFO`:

```
load tablets from header failed, failed tablets size: xxx, path=xxx
```

Indicates how many tablets in this data directory failed to load. At the same time, the log will also have specific information about the tablet that failed to load. In this case, manual intervention is required to troubleshoot the cause of the error. After troubleshooting, there are usually two ways to recover:

1. If the tablet information is not repairable, you can delete the wrong tablet through the `meta_tool` tool under the condition that other copies are normal.
2. Set `ignore_load_tablet_failure` to true, BE will ignore these wrong tablets and start normally.

### `inc_rowset_expired_sec`

### `index_stream_cache_capacity`

### `load_data_reserve_hours`

### `load_error_log_reserve_hours`

### `load_process_max_memory_limit_bytes`

### `load_process_max_memory_limit_percent`

### `local_library_dir`

### `log_buffer_level`

### `madvise_huge_pages`

### `make_snapshot_worker_count`

### `max_client_cache_size_per_host`

### `max_compaction_concurrency`

### `max_consumer_num_per_group`

### `max_cumulative_compaction_num_singleton_deltas`

### `max_download_speed_kbps`

### `max_free_io_buffers`

### `max_garbage_sweep_interval`

### `max_memory_sink_batch_count`

### `max_percentage_of_error_disk`

### `max_pushdown_conditions_per_column`

* Type: int
* Description: Used to limit the maximum number of conditions that can be pushed down to the storage engine for a single column in a query request. During the execution of the query plan, the filter conditions on some columns can be pushed down to the storage engine, so that the index information in the storage engine can be used for data filtering, reducing the amount of data that needs to be scanned by the query. Such as equivalent conditions, conditions in IN predicates, etc. In most cases, this parameter only affects queries containing IN predicates. Such as `WHERE colA IN (1,2,3,4, ...)`. A larger number means that more conditions in the IN predicate can be pushed to the storage engine, but too many conditions may cause an increase in random reads, and in some cases may reduce query efficiency. This configuration can be individually configured for session level. For details, please refer to the description of `max_pushdown_conditions_per_column` in [Variables](../ variables.md).
* Default value: 1024

* Example

    The table structure is `id INT, col2 INT, col3 varchar (32), ...`.

    The query is `... WHERE id IN (v1, v2, v3, ...)`

    If the number of conditions in the IN predicate exceeds the configuration, try to increase the configuration value and observe whether the query response has improved.

### `max_runnings_transactions_per_txn_map`

### `max_tablet_num_per_shard`

### `mem_limit`

### `memory_limitation_per_thread_for_schema_change`

### `memory_maintenance_sleep_time_s`

### `memory_max_alignment`

### `min_buffer_size`

### `min_compaction_failure_interval_sec`

### `min_cumulative_compaction_num_singleton_deltas`

### `min_file_descriptor_number`

### `min_garbage_sweep_interval`

### `mmap_buffers`

### `num_cores`

### `num_disks`

### `num_threads_per_core`

### `num_threads_per_disk`

### `number_tablet_writer_threads`

### `path_gc_check`

### `path_gc_check_interval_second`

### `path_gc_check_step`

### `path_gc_check_step_interval_ms`

### `path_scan_interval_second`

### `pending_data_expire_time_sec`

### `periodic_counter_update_period_ms`

### `plugin_path`

### `port`

### `pprof_profile_dir`

### `priority_networks`

### `priority_queue_remaining_tasks_increased_frequency`

### `publish_version_worker_count`

### `pull_load_task_dir`

### `push_worker_count_high_priority`

### `push_worker_count_normal_priority`

### `push_write_mbytes_per_sec`

### `query_scratch_dirs`

### `read_size`

### `release_snapshot_worker_count`

### `report_disk_state_interval_seconds`

### `report_tablet_interval_seconds`

### `report_task_interval_seconds`

### `result_buffer_cancelled_interval_time`

### `routine_load_thread_pool_size`

### `row_nums_check`

### `scan_context_gc_interval_min`

### `scratch_dirs`

### `serialize_batch`

### `sleep_five_seconds`

### `sleep_one_second`

### `small_file_dir`

### `snapshot_expire_time_sec`

### `sorter_block_size`

### `status_report_interval`

### `storage_flood_stage_left_capacity_bytes`

### `storage_flood_stage_usage_percent`

### `storage_medium_migrate_count`

### `storage_page_cache_limit`

### `storage_root_path`

### `streaming_load_max_mb`

### `streaming_load_rpc_max_alive_time_sec`

### `sync_tablet_meta`

### `sys_log_dir`

### `sys_log_level`

### `sys_log_roll_mode`

### `sys_log_roll_num`

### `sys_log_verbose_level`

### `sys_log_verbose_modules`

### `tablet_map_shard_size`

### `tablet_meta_checkpoint_min_interval_secs`

### `tablet_meta_checkpoint_min_new_rowsets_num`

### `tablet_stat_cache_update_interval_second`

### `tablet_writer_open_rpc_timeout_sec`

### `tc_free_memory_rate`

### `tc_max_total_thread_cache_bytes`

* Type: int64
* Description: Used to limit the total thread cache size in tcmalloc. This limit is not a hard limit, so the actual thread cache usage may exceed this limit. For details, please refer to [TCMALLOC\_MAX\_TOTAL\_THREAD\_CACHE\_BYTES](https://gperftools.github.io/gperftools/tcmalloc.html)
* Default: 1073741824

If the system is found to be in a high-stress scenario and a large number of threads are found in the tcmalloc lock competition phase through the BE thread stack, such as a large number of `SpinLock` related stacks, you can try increasing this parameter to improve system performance. [Reference] (https://github.com/gperftools/gperftools/issues/1111)

### `tc_use_memory_min`

### `thrift_connect_timeout_seconds`

### `thrift_rpc_timeout_ms`

### `trash_file_expire_time_sec`

### `txn_commit_rpc_timeout_ms`

### `txn_map_shard_size`

### `txn_shard_size`

### `unused_rowset_monitor_interval`

### `upload_worker_count`

### `use_mmap_allocate_chunk`

### `user_function_dir`

### `web_log_bytes`

### `webserver_num_workers`

### `webserver_port`

### `write_buffer_size`
