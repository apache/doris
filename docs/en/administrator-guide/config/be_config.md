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

The BE configuration file `be.conf` is usually stored in the `conf/` directory of the BE deployment path. In version 0.14, another configuration file `be_custom.conf` will be introduced. The configuration file is used to record the configuration items that are dynamically configured and persisted by the user during operation.

After the BE process is started, it will read the configuration items in `be.conf` first, and then read the configuration items in `be_custom.conf`. The configuration items in `be_custom.conf` will overwrite the same configuration items in `be.conf`.

The location of the `be_custom.conf` file can be configured in `be.conf` through the `custom_config_dir` configuration item.

## View configuration items

Users can view the current configuration items by visiting BE's web page:

`http://be_host:be_webserver_port/varz`

## Set configuration items

There are two ways to configure BE configuration items:

1. Static configuration

    Add and set configuration items in the `conf/be.conf` file. The configuration items in `be.conf` will be read when BE starts. Configuration items not in `be.conf` will use default values.

2. Dynamic configuration

    After BE starts, the configuration items can be dynamically set with the following commands.

    ```
    curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}'
    ```

    In version 0.13 and before, the configuration items modified in this way will become invalid after the BE process restarts. In 0.14 and later versions, the modified configuration can be persisted through the following command. The modified configuration items are stored in the `be_custom.conf` file.

    ```
    curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}&persis=true'
    ```

## Examples

1. Modify `max_compaction_concurrency` statically

     By adding in the `be.conf` file:

     ```max_compaction_concurrency=5```

     Then restart the BE process to take effect the configuration.

2. Modify `streaming_load_max_mb` dynamically

    After BE starts, the configuration item `streaming_load_max_mb` is dynamically set by the following command:

    ```
    curl -X POST http://{be_ip}:{be_http_port}/api/update_config?streaming_load_max_mb=1024
    ```

    The return value is as follows, indicating that the setting is successful.

    ```
    {
        "status": "OK",
        "msg": ""
    }
    ```

    The configuration will become invalid after the BE restarts. If you want to persist the modified results, use the following command:

    ```
    curl -X POST http://{be_ip}:{be_http_port}/api/update_config?streaming_load_max_mb=1024\&persist=true
    ```

## Configurations

### `alter_tablet_worker_count`

### `base_compaction_check_interval_seconds`

### `base_compaction_interval_seconds_since_last_operation`

### `base_compaction_num_cumulative_deltas`

### `base_compaction_num_threads_per_disk`

### base_compaction_trace_threshold

* Type: int32
* Description: Threshold to logging base compaction's trace information, in seconds
* Default value: 10

Base compaction is a long time cost background task, this configuration is the threshold to logging trace information. Trace information in log file looks like:

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

### `base_compaction_write_mbytes_per_sec`

### `base_cumulative_delta_ratio`

### `be_port`

* Type: int32
* Description: The port of the thrift server on BE which used to receive requests from FE
* Default value: 9060

### `be_service_threads`

* Type: int32
* Description: The number of execution threads of the thrift server service on BE which represents the number of threads that can be used to execute FE requests.
* Default value: 64

### `brpc_max_body_size`

This configuration is mainly used to modify the parameter `max_body_size` of brpc.

Sometimes the query fails and an error message of `body_size is too large` will appear in the BE log. This may happen when the SQL mode is "multi distinct + no group by + more than 1T of data".

This error indicates that the packet size of brpc exceeds the configured value. At this time, you can avoid this error by increasing the configuration.

### `brpc_socket_max_unwritten_bytes`

This configuration is mainly used to modify the parameter `socket_max_unwritten_bytes` of brpc.

Sometimes the query fails and an error message of `The server is overcrowded` will appear in the BE log. This means there are too many messages to buffer at the sender side, which may happen when the SQL needs to send large bitmap value. You can avoid this error by increasing the configuration.

### `brpc_num_threads`

This configuration is mainly used to modify the number of bthreads for brpc. The default value is set to -1, which means the number of bthreads is #cpu-cores.

User can set this configuration to a larger value to get better QPS performance. For more information, please refer to `https://github.com/apache/incubator-brpc/blob/master/docs/cn/benchmark.md`

### `brpc_port`

* Type: int32
* Description: The port of BRPC on BE, used for communication between BEs
* Default value: 9060

### `buffer_pool_clean_pages_limit`

### `buffer_pool_limit`

* Type: string
* Description: The largest allocatable memory of the buffer pool
* Default value: 80G

The maximum amount of memory available in the BE buffer pool. The buffer pool is a new memory management structure of BE, which manages the memory by the buffer page and enables spill data to disk. The memory for all concurrent queries will be allocated from the buffer pool. The current buffer pool only works on **AggregationNode** and **ExchangeNode**.

### `check_auto_compaction_interval_seconds`

* Type: int32
* Description: Check the configuration of auto compaction in seconds when auto compaction disabled.
* Default value: 5

### `check_consistency_worker_count`

### `chunk_reserved_bytes_limit`

### `clear_transaction_task_worker_count`

### `clone_worker_count`

### `cluster_id`

* Type: int32
* Description: Configure the cluster id to which the BE belongs.
* Default value: -1

This value is usually delivered by the FE to the BE by the heartbeat, no need to configure. When it is confirmed that a BE belongs to a certain Drois cluster, it can be configured. The cluster_id file under the data directory needs to be modified to make sure same as this parament.

### `column_dictionary_key_ratio_threshold`

### `column_dictionary_key_size_threshold`

### `compaction_tablet_compaction_score_factor`

* Type: int32
* Description: Coefficient for compaction score when calculating tablet score to find a tablet for compaction.
* Default value: 1

### `compaction_tablet_scan_frequency_factor`

* Type: int32
* Description: Coefficient for tablet scan frequency when calculating tablet score to find a tablet for compaction.
* Default value: 0

Tablet scan frequency can be taken into consideration when selecting an tablet for compaction and preferentially do compaction for those tablets which are scanned frequently during a latest period of time at the present.
Tablet score can be calculated like this:

tablet_score = compaction_tablet_scan_frequency_factor * tablet_scan_frequency + compaction_tablet_scan_frequency_factor * compaction_score

### `compaction_task_num_per_disk`

* Type: int32
* Description: The number of compaction tasks which execute in parallel for a disk.
* Default value: 2

### `compress_rowbatches`

* Type: bool
* Description: enable to use Snappy compression algorithm for data compression when serializing RowBatch
* Default value: true

### `create_tablet_worker_count`

### `cumulative_compaction_rounds_for_each_base_compaction_round`

* Type: int32
* Description: How many rounds of cumulative compaction for each round of base compaction when compaction tasks generation.
* Default value: 9

### `disable_auto_compaction`

* Type: bool
* Description: Whether disable automatic compaction task
* Default value: false

Generally it needs to be turned off. When you want to manually operate the compaction task in the debugging or test environment, you can turn on the configuration.

### `cumulative_compaction_budgeted_bytes`

### `cumulative_compaction_check_interval_seconds`

### `cumulative_compaction_num_threads_per_disk`

### `cumulative_compaction_skip_window_seconds`

### cumulative_compaction_trace_threshold

* Type: int32
* Description: Threshold to logging cumulative compaction's trace information, in seconds
* Default value: 10

Similar to `base_compaction_trace_threshold`.

### `cumulative_compaction_policy`

* Type: string
* Description: Configure the merge policy of the cumulative compaction stage. Currently, two merge policy have been implemented, num_based and size_based.
* Default value: size_based

In detail, ordinary is the initial version of the cumulative compaction merge policy. After a cumulative compaction, the base compaction process is directly performed. The size_based policy is an optimized version of the ordinary strategy. Versions are merged only when the disk volume of the rowset is of the same order of magnitude. After the compaction, the output rowset which satisfies the conditions is promoted to the base compaction stage. In the case of a large number of small batch imports: reduce the write magnification of base compact, trade-off between read magnification and space magnification, and reducing file version data.

### `cumulative_size_based_promotion_size_mbytes`

* Type: int64
* Description: Under the size_based policy, the total disk size of the output rowset of cumulative compaction exceeds this configuration size, and the rowset will be used for base compaction. The unit is m bytes.
* Default value: 1024

In general, if the configuration is less than 2G, in order to prevent the cumulative compression time from being too long, resulting in the version backlog.

### `cumulative_size_based_promotion_ratio`

* Type: double
* Description: Under the size_based policy, when the total disk size of the cumulative compaction output rowset exceeds the configuration ratio of the base version rowset, the rowset will be used for base compaction.
* Default value: 0.05

Generally, it is recommended that the configuration should not be higher than 0.1 and lower than 0.02.

### `cumulative_size_based_promotion_min_size_mbytes`

* Type: int64
* Description: Under the size_based strategy, if the total disk size of the output rowset of the cumulative compaction is lower than this configuration size, the rowset will not undergo base compaction and is still in the cumulative compaction process. The unit is m bytes.
* Default value: 64

Generally, the configuration is within 512m. If the configuration is too large, the size of the early base version is too small, and base compaction has not been performed.

### `cumulative_size_based_compaction_lower_size_mbytes`

* Type: int64
* Description: Under the size_based strategy, when the cumulative compaction is merged, the selected rowsets to be merged have a larger disk size than this configuration, then they are divided and merged according to the level policy. When it is smaller than this configuration, merge directly. The unit is m bytes.
* Default value: 64

Generally, the configuration is within 128m. Over configuration will cause more cumulative compaction write amplification.

### `custom_config_dir`

Configure the location of the `be_custom.conf` file. The default is in the `conf/` directory.

In some deployment environments, the `conf/` directory may be overwritten due to system upgrades. This will cause the user modified configuration items to be overwritten. At this time, we can store `be_custom.conf` in another specified directory to prevent the configuration file from being overwritten.

### `default_num_rows_per_column_file_block`

* Type: int32
* Description: Configure how many rows of data are contained in a single RowBlock.
* Default value: 1024

### `default_rowset_type`

* Type: string
* Description: Identifies the storage format selected by BE by default. The configurable parameters are: "**ALPHA**", "**BETA**". Mainly play the following two roles
1. When the storage_format of the table is set to Default, select the storage format of BE through this configuration.
2. Select the storage format of when BE performing Compaction
* Default value: BETA

### `delete_worker_count`

### `disable_mem_pools`

### `disable_storage_page_cache`

* Type: bool
* Description: Disable to use page cache for index caching, this configuration only takes effect in BETA storage format, usually it is recommended to false
* Default value: false

### `disk_stat_monitor_interval`

### `doris_cgroups`

### `doris_max_pushdown_conjuncts_return_rate`

* Type: int32
* Description: When BE performs HashJoin, it will adopt a dynamic partitioning method to push the join condition to OlapScanner. When the data scanned by OlapScanner is larger than 32768 rows, BE will check the filter condition. If the filter rate of the filter condition is lower than this configuration, Doris will stop using the dynamic partition clipping condition for data filtering.
* Default value: 90

### `doris_max_scan_key_num`

* Type: int
* Description: Used to limit the maximum number of scan keys that a scan node can split in a query request. When a conditional query request reaches the scan node, the scan node will try to split the conditions related to the key column in the query condition into multiple scan key ranges. After that, these scan key ranges will be assigned to multiple scanner threads for data scanning. A larger value usually means that more scanner threads can be used to increase the parallelism of the scanning operation. However, in high concurrency scenarios, too many threads may bring greater scheduling overhead and system load, and will slow down the query response speed. An empirical value is 50. This configuration can be configured separately at the session level. For details, please refer to the description of `max_scan_key_num` in [Variables](../variables.md).
* Default value: 1024

When the concurrency cannot be improved in high concurrency scenarios, try to reduce this value and observe the impact.

### `doris_scan_range_row_count`

* Type: int32
* Description: When BE performs data scanning, it will split the same scanning range into multiple ScanRanges. This parameter represents the scan data range of each ScanRange. This parameter can limit the time that a single OlapScanner occupies the io thread.
* Default value: 524288

### `doris_scanner_queue_size`

* Type: int32
* Description: The length of the RowBatch buffer queue between TransferThread and OlapScanner. When Doris performs data scanning, it is performed asynchronously. The Rowbatch scanned by OlapScanner will be placed in the scanner buffer queue, waiting for the upper TransferThread to take it away.
* Default value: 1024

### `doris_scanner_row_num`

### `doris_scanner_thread_pool_queue_size`

* Type: int32
* Description: The queue length of the Scanner thread pool. In Doris' scanning tasks, each Scanner will be submitted as a thread task to the thread pool waiting to be scheduled, and after the number of submitted tasks exceeds the length of the thread pool queue, subsequent submitted tasks will be blocked until there is a empty slot in the queue. 
* Default value: 102400

### `doris_scanner_thread_pool_thread_num`

* Type: int32
* Description: The number of threads in the Scanner thread pool. In Doris' scanning tasks, each Scanner will be submitted as a thread task to the thread pool to be scheduled. This parameter determines the size of the Scanner thread pool.
* Default value: 48

### `download_low_speed_limit_kbps`

### `download_low_speed_time`

### `download_worker_count`

### `drop_tablet_worker_count`

### `enable_metric_calculator`

### `enable_partitioned_aggregation`

* Type: bool
* Description: Whether the BE node implements the aggregation operation by PartitionAggregateNode, if false, AggregateNode will be executed to complete the aggregation. It is not recommended to set it to false in non-special demand scenarios.
* Default value: true

### `enable_prefetch`
* Type: bool
* Description: When using PartitionedHashTable for aggregation and join calculations, whether to perform HashBuket prefetch. Recommended to be set to true
* Default value: true

### `enable_quadratic_probing`

* Type: bool
* Description: When a Hash conflict occurs when using PartitionedHashTable, enable to use the square detection method to resolve the Hash conflict. If the value is false, linear detection is used to resolve the Hash conflict. For the square detection method, please refer to: [quadratic_probing](https://en.wikipedia.org/wiki/Quadratic_probing)
* Default value: true

### `enable_system_metrics`

### `enable_token_check`

### `es_http_timeout_ms`

### `es_scroll_keepalive`

### `etl_thread_pool_queue_size`

### `etl_thread_pool_size`

### `exchg_node_buffer_size_bytes`

* Type: int32
* Description: The size of the Buffer queue of the ExchangeNode node, in bytes. After the amount of data sent from the Sender side is larger than the Buffer size of ExchangeNode, subsequent data sent will block until the Buffer frees up space for writing.
* Default value: 10485760

### `file_descriptor_cache_capacity`

### `file_descriptor_cache_clean_interval`

### `flush_thread_num_per_store`

### `force_recovery`

### `fragment_pool_queue_size`

### `fragment_pool_thread_num`

### `heartbeat_service_port`
* Type: int32
* Description: Heartbeat service port (thrift) on BE, used to receive heartbeat from FE
* Default value: 9050

### `heartbeat_service_thread_count`

* Type: int32
* Description: The number of threads that execute the heartbeat service on BE。 the default is 1, it is not recommended to modify
* Default value: 1

### `ignore_broken_disk`

### `ignore_load_tablet_failure`
When BE starts, it will check all the paths under the `storage_root_path` in  configuration.

- `ignore_broken_disk=true`

  If the path does not exist or the file under the path cannot be read or written (broken disk), it will be ignored. If there are any other available paths, the startup will not be interrupted.

- `ignore_broken_disk=false`

  If the path does not exist or the file under the path cannot be read or written (bad disk), the startup will fail and exit.

The default value is `false`.

### ignore_rowset_stale_unconsistent_delete

* Type: boolean
* Description:It is used to decide whether to delete the outdated merged rowset if it cannot form a consistent version path.
* Default: false

The merged expired rowset version path will be deleted after half an hour. In abnormal situations, deleting these versions will result in the problem that the consistent path of the query cannot be constructed. When the configuration is false, the program check is strict and the program will directly report an error and exit.
When configured as true, the program will run normally and ignore this error. In general, ignoring this error will not affect the query, only when the merged version is dispatched by fe, -230 error will appear.

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

### `index_stream_cache_capacity`

### `load_data_reserve_hours`

### `load_error_log_reserve_hours`

### `load_process_max_memory_limit_bytes`

### `load_process_max_memory_limit_percent`

### `log_buffer_level`

### `madvise_huge_pages`

### `make_snapshot_worker_count`

### `max_client_cache_size_per_host`

### `max_compaction_threads`

* Type: int32
* Description: The maximum of thread number in compaction thread pool.
* Default value: 10

### `max_consumer_num_per_group`

### `max_cumulative_compaction_num_singleton_deltas`

### `max_download_speed_kbps`

### `max_free_io_buffers`

### `max_garbage_sweep_interval`

### `max_memory_sink_batch_count`

### `max_percentage_of_error_disk`

* Type: int32
* Description: The storage engine allows the percentage of damaged hard disks to exist. After the damaged hard disk exceeds the changed ratio, BE will automatically exit.
* Default value: 0

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

### `max_tablet_version_num`

* Type: int
* Description: Limit the number of versions of a single tablet. It is used to prevent a large number of version accumulation problems caused by too frequent import or untimely compaction. When the limit is exceeded, the import task will be rejected.
* Default value: 500

### `mem_limit`

* Type: string
* Description: Limit the percentage of the server's maximum memory used by the BE process. It is used to prevent BE memory from occupying to many the machine's memory. This parameter must be greater than 0. When the percentage is greater than 100%, the value will default to 100%.
* Default value: 80%

### `memory_limitation_per_thread_for_schema_change`

### `memory_maintenance_sleep_time_s`

### `memory_max_alignment`

### `min_buffer_size`

### `min_compaction_failure_interval_sec`

* Type: int32
* Description: During the cumulative compaction process, when the selected tablet fails to be merged successfully, it will wait for a period of time before it may be selected again. The waiting period is the value of this configuration.
* Default value: 600
* Unit: seconds

### `min_compaction_threads`

* Type: int32
* Description: The minimum of thread number in compaction thread pool.
* Default value: 10

### `min_cumulative_compaction_num_singleton_deltas`

### `min_file_descriptor_number`

### `min_garbage_sweep_interval`

### `mmap_buffers`

### `num_cores`

* Type: int32
* Description: The number of CPU cores that BE can use. When the value is 0, BE will obtain the number of CPU cores of the machine from /proc/cpuinfo.
* Default value: 0

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

* Type: int32
* Description: The port used in UT. Meaningless in the actual environment and can be ignored.
* Default value: 20001

### `pprof_profile_dir`

### `priority_networks`

### `priority_queue_remaining_tasks_increased_frequency`

### `publish_version_worker_count`

### `pull_load_task_dir`

### `push_worker_count_high_priority`

### `push_worker_count_normal_priority`

### `push_write_mbytes_per_sec`

+ Type: int32
+ Description: Load data speed control, the default is 10MB per second. Applicable to all load methods.
+ Unit: MB
+ Default value: 10

### `query_scratch_dirs`

+ Type: string
+ Description: The directory selected by BE to store temporary data during spill to disk. which is similar to the storage path configuration, multiple directories are separated by ;.
+ Default value: ${DORIS_HOME}

### `read_size`

### `release_snapshot_worker_count`

### `report_disk_state_interval_seconds`

### `report_tablet_interval_seconds`

### `report_task_interval_seconds`

### `result_buffer_cancelled_interval_time`

### `routine_load_thread_pool_size`

### `row_nums_check`

### `row_step_for_compaction_merge_log`

* Type: int64
* Description: Merge log will be printed for each "row_step_for_compaction_merge_log" rows merged during compaction. If the value is set to 0, merge log will not be printed.
* Default value: 0
* Dynamically modify: true

### `scan_context_gc_interval_min`

### `sleep_one_second`

+ Type: int32
+ Description: Global variables, used for BE thread sleep for 1 seconds, should not be modified
+ Default value: 1

### `small_file_dir`

### `snapshot_expire_time_sec`

### `status_report_interval`

### `storage_flood_stage_left_capacity_bytes`

### `storage_flood_stage_usage_percent`

### `storage_medium_migrate_count`

### `storage_page_cache_limit`

### `index_page_cache_percentage`
* Type: int32
* Description: Index page cache as a percentage of total storage page cache, value range is [0, 100]
* Default value: 10

### `storage_root_path`

* Type: string

* Description: data root path, separate by ';'.you can specify the storage medium of each root path, HDD or SSD. you can add capacity limit at the end of each root path, seperate by ','
eg: storage_root_path=/home/disk1/doris.HDD,50;/home/disk2/doris.SSD,1;/home/disk2/doris

    * 1./home/disk1/doris.HDD, capacity limit is 50GB, HDD;
    * 2./home/disk2/doris.SSD, capacity limit is 1GB, SSD;
    * 3./home/disk2/doris, capacity limit is disk capacity, HDD(default)

* Default: ${DORIS_HOME}

### `storage_strict_check_incompatible_old_format`
* Type: bool
* Description: Used to check incompatible old format strictly
* Default value: true
* Dynamically modify: false

This config is used to check incompatible old format hdr_ format whether doris uses strict way. When config is true, 
process will log fatal and exit. When config is false, process will only log warning.

### `streaming_load_max_mb`

* Type: int64
* Description: Used to limit the maximum amount of csv data allowed in one Stream load. The unit is MB.
* Default value: 10240
* Dynamically modify: yes

Stream Load is generally suitable for loading data less than a few GB, not suitable for loading` too large data.

### `streaming_load_json_max_mb`

* Type: int64
* Description: it is used to limit the maximum amount of json data allowed in one Stream load. The unit is MB.
* Default value: 100
* Dynamically modify: yes

Some data formats, such as JSON, cannot be split. Doris must read all the data into the memory before parsing can begin. Therefore, this value is used to limit the maximum amount of data that can be loaded in a single Stream load.

### `streaming_load_rpc_max_alive_time_sec`

### `sync_tablet_meta`

### `sys_log_dir`

### `sys_log_level`

* Type: string
* Description: Storage directory of BE log data
* Default: ${DORIS_HOME}/log

### `sys_log_roll_mode`

### `sys_log_roll_num`

### `sys_log_verbose_level`

### `sys_log_verbose_modules`

### `tablet_map_shard_size`

### `tablet_meta_checkpoint_min_interval_secs`

### `tablet_meta_checkpoint_min_new_rowsets_num`

### `tablet_scan_frequency_time_node_interval_second`

* Type: int64
* Description: Time interval to record the metric 'query_scan_count' and timestamp in second for the purpose of  calculating tablet scan frequency during a latest period of time at the present.
* Default: 300

### `tablet_stat_cache_update_interval_second`

### `tablet_rowset_stale_sweep_time_sec`

* Type: int64
* Description: It is used to control the expiration time of cleaning up the merged rowset version. When the current time now() minus the max created rowset‘s create time in a version path is greater than tablet_rowset_stale_sweep_time_sec, the current path is cleaned up and these merged rowsets are deleted, the unit is second.
* Default: 1800

When writing is too frequent and the disk time is insufficient, you can configure less tablet_rowset_stale_sweep_time_sec. However, if this time is less than 5 minutes, it may cause fe to query the version that has been merged, causing a query -230 error.

### `tablet_writer_open_rpc_timeout_sec`

### `tablet_writer_ignore_eovercrowded`

* Type: bool
* Description: Used to ignore brpc error '[E1011]The server is overcrowded' when writing data. 
* Default value: false

When meet '[E1011]The server is overcrowded' error, you can tune the configuration `brpc_socket_max_unwritten_bytes`, but it can't be modified at runtime. Set it to `true` to avoid writing failed temporarily. Notice that, it only effects `write`, other rpc requests will still check if overcrowded.

### `tc_free_memory_rate`

### `tc_max_total_thread_cache_bytes`

* Type: int64
* Description: Used to limit the total thread cache size in tcmalloc. This limit is not a hard limit, so the actual thread cache usage may exceed this limit. For details, please refer to [TCMALLOC\_MAX\_TOTAL\_THREAD\_CACHE\_BYTES](https://gperftools.github.io/gperftools/tcmalloc.html)
* Default: 1073741824

If the system is found to be in a high-stress scenario and a large number of threads are found in the tcmalloc lock competition phase through the BE thread stack, such as a large number of `SpinLock` related stacks, you can try increasing this parameter to improve system performance. [Reference] (https://github.com/gperftools/gperftools/issues/1111)

### `tc_use_memory_min`

### `thrift_client_retry_interval_ms`

* Type: int64
* Description: Used to set retry interval for thrift client in be to avoid avalanche disaster in fe thrift server, the unit is ms.
* Default: 1000

### `thrift_connect_timeout_seconds`

### `thrift_rpc_timeout_ms`

### `thrift_server_type_of_fe`

This configuration indicates the service model used by FE's Thrift service. The type is string and is case-insensitive. This parameter needs to be consistent with the setting of fe's thrift_server_type parameter. Currently there are two values for this parameter, `THREADED` and `THREAD_POOL`.

If the parameter is `THREADED`, the model is a non-blocking I/O model,

If the parameter is `THREAD_POOL`, the model is a blocking I/O model.

### `total_permits_for_compaction_score`

* Type: int64
* Description: The upper limit of "permits" held by all compaction tasks. This config can be set to limit memory consumption for compaction.
* Default: 10000
* Dynamically modify: true

### `trash_file_expire_time_sec`

### `txn_commit_rpc_timeout_ms`

### `txn_map_shard_size`

### `txn_shard_size`

### `unused_rowset_monitor_interval`

### `upload_worker_count`

### `use_mmap_allocate_chunk`

### `user_function_dir`

### `webserver_num_workers`

### `webserver_port`

* Type: int32
* Description: Service port of http server on BE
* Default: 8040

### `write_buffer_size`

### `zone_map_row_num_threshold`

* Type: int32
* Description: If the number of rows in a page is less than this value, no zonemap will be created to reduce data expansion
* Default: 20
