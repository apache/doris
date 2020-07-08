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

### `brpc_socket_max_unwritten_bytes`

这个配置主要用来修改 brpc  的参数 `socket_max_unwritten_bytes`。

有时查询失败，BE 日志中会出现 `The server is overcrowded` 的错误信息，表示连接上有过多的未发送数据。当查询需要发送较大的bitmap字段时，可能会遇到该问题，此时可能通过调大该配置避免该错误。

由于这是一个 brpc 的配置，用户也可以在运行中直接修改该参数。通过访问 `http://be_host:brpc_port/flags` 修改。

### `brpc_num_threads`

该配置主要用来修改brpc中bthreads的数量. 该配置的默认值被设置为-1, 这意味着bthreads的数量将被设置为机器的cpu核数。

用户可以将该配置的值调大来获取更好的QPS性能。更多的信息可以参考`https://github.com/apache/incubator-brpc/blob/master/docs/cn/benchmark.md`。

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

* 类型：int
* 描述：用于限制一个查询请求中，scan node 节点能拆分的最大 scan key 的个数。当一个带有条件的查询请求到达 scan node 节点时，scan node 会尝试将查询条件中 key 列相关的条件拆分成多个 scan key range。之后这些 scan key range 会被分配给多个 scanner 线程进行数据扫描。较大的数值通常意味着可以使用更多的 scanner 线程来提升扫描操作的并行度。但在高并发场景下，过多的线程可能会带来更大的调度开销和系统负载，反而会降低查询响应速度。一个经验数值为 50。该配置可以单独进行会话级别的配置，具体可参阅 [变量](../variables.md) 中 `max_scan_key_num` 的说明。
* 默认值：1024

当在高并发场景下发下并发度无法提升时，可以尝试降低该数值并观察影响。

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

​	当BE启动时，会检查``storage_root_path`` 配置下的所有路径。

 - `ignore_broken_disk=true`

   如果路径不存在或路径下无法进行读写文件(坏盘)，将忽略此路径，如果有其他可用路径则不中断启动。

 - `ignore_broken_disk=false`

   如果路径不存在或路径下无法进行读写文件(坏盘)，将中断启动失败退出。

​    默认为false

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

* 类型：int
* 描述：用于限制一个查询请求中，针对单个列，能够下推到存储引擎的最大条件数量。在查询计划执行的过程中，一些列上的过滤条件可以下推到存储引擎，这样可以利用存储引擎中的索引信息进行数据过滤，减少查询需要扫描的数据量。比如等值条件、IN 谓词中的条件等。这个参数在绝大多数情况下仅影响包含 IN 谓词的查询。如 `WHERE colA IN (1,2,3,4,...)`。较大的数值意味值 IN 谓词中更多的条件可以推送给存储引擎，但过多的条件可能会导致随机读的增加，某些情况下可能会降低查询效率。该配置可以单独进行会话级别的配置，具体可参阅 [变量](../variables.md) 中 `max_pushdown_conditions_per_column ` 的说明。
* 默认值：1024

* 示例

    表结构为 `id INT, col2 INT, col3 varchar(32), ...`。
    
    查询请求为 `... WHERE id IN (v1, v2, v3, ...)`
    
    如果 IN 谓词中的条件数量超过了该配置，则可以尝试增加该配置值，观察查询响应是否有所改善。

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

* 类型：int64
* 描述：用来限制 tcmalloc 中总的线程缓存大小。这个限制不是硬限，因此实际线程缓存使用可能超过这个限制。具体可参阅 [TCMALLOC\_MAX\_TOTAL\_THREAD\_CACHE\_BYTES](https://gperftools.github.io/gperftools/tcmalloc.html)
* 默认值： 1073741824

如果发现系统在高压力场景下，通过 BE 线程堆栈发现大量线程处于 tcmalloc 的锁竞争阶段，如大量的 `SpinLock` 相关堆栈，则可以尝试增大该参数来提升系统性能。[参考](https://github.com/gperftools/gperftools/issues/1111)

### `tc_use_memory_min`

### `thrift_client_retry_interval_ms`

* 类型：int64
* 描述：用来为be的thrift客户端设置重试间隔, 避免fe的thrift server发生雪崩问题，单位为ms。
* 默认值：1000

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

### ignore_load_tablet_failure

* 类型：布尔
* 描述：用来决定在有tablet 加载失败的情况下是否忽略错误，继续启动be
* 默认值：false

BE启动时，会对每个数据目录单独启动一个线程进行 tablet header 元信息的加载。默认配置下，如果某个数据目录有 tablet 加载失败，则启动进程会终止。同时会在 `be.INFO` 日志中看到如下错误信息：

```
load tablets from header failed, failed tablets size: xxx, path=xxx
```

表示该数据目录共有多少 tablet 加载失败。同时，日志中也会有加载失败的 tablet 的具体信息。此时需要人工介入来对错误原因进行排查。排查后，通常有两种方式进行恢复：

1. tablet 信息不可修复，在确保其他副本正常的情况下，可以通过 `meta_tool` 工具将错误的tablet删除。
2. 将 `ignore_load_tablet_failure` 设置为 true，则 BE 会忽略这些错误的 tablet，正常启动。

### base_compaction_trace_threshold

* 类型：int32
* 描述：打印base compaction的trace信息的阈值，单位秒
* 默认值：10

base compaction是一个耗时较长的后台操作，为了跟踪其运行信息，可以调整这个阈值参数来控制trace日志的打印。打印信息如下：

```
W0610 11:26:33.804431 56452 storage_engine.cpp:552] Trace:
0610 11:23:03.727535 (+     0us) storage_engine.cpp:554] start to perform base compaction
0610 11:23:03.728961 (+  1426us) storage_engine.cpp:560] found best tablet 546859
0610 11:23:03.728963 (+     2us) base_compaction.cpp:40] got base compaction lock
0610 11:23:03.729029 (+    66us) base_compaction.cpp:44] rowsets picked
0610 11:24:51.784439 (+108055410us) compaction.cpp:46] got concurrency lock and start to do compaction
0610 11:24:51.784818 (+   379us) compaction.cpp:74] prepare finished
0610 11:26:33.359265 (+101574447us) compaction.cpp:87] merge rowsets finished
0610 11:26:33.484481 (+125216us) compaction.cpp:102] output rowset built
0610 11:26:33.484482 (+     1us) compaction.cpp:106] check correctness finished
0610 11:26:33.513197 (+ 28715us) compaction.cpp:110] modify rowsets finished
0610 11:26:33.513300 (+   103us) base_compaction.cpp:49] compaction finished
0610 11:26:33.513441 (+   141us) base_compaction.cpp:56] unused rowsets have been moved to GC queue
Metrics: {"filtered_rows":0,"input_row_num":3346807,"input_rowsets_count":42,"input_rowsets_data_size":1256413170,"input_segments_num":44,"merge_rowsets_latency_us":101574444,"merged_rows":0,"output_row_num":3346807,"output_rowset_data_size":1228439659,"output_segments_num":6}
```

### cumulative_compaction_trace_threshold

* 类型：int32
* 描述：打印cumulative compaction的trace信息的阈值，单位秒
* 默认值：2

与base_compaction_trace_threshold类似。

