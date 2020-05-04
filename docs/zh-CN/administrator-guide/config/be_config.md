---
{
    "title": "BE 配置项",
    "language": "zh-CN"
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

# BE 配置项

该文档主要介绍 BE 的相关配置项。

## 查看配置项
（TODO）

## 设置配置项
（TODO）

## 应用举例
（TODO）

## 配置项列表

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

这个配置主要用来修改 brpc 的参数 `max_body_size`。

有时查询失败，在 BE 日志中会出现 `body_size is too large` 的错误信息。这可能发生在 SQL 模式为 multi distinct + 无 group by + 超过1T 数据量的情况下。这个错误表示 brpc 的包大小超过了配置值。此时可以通过调大该配置避免这个错误。

由于这是一个 brpc 的配置，用户也可以在运行中直接修改该参数。通过访问 `http://be_host:brpc_port/flags` 修改。

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
