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
    curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}&persis=true
    ```

## Examples

1. Modify `max_base_compaction_threads` statically

     By adding in the `be.conf` file:

     ```max_base_compaction_threads=5```

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

Default: 3

The number of threads making schema changes

### `generate_compaction_tasks_min_interval_ms`

Default: 10 (ms)

Minimal interval (ms) to generate compaction tasks

### `enable_vectorized_compaction`

Default: true

Whether to enable vectorized compaction

### `base_compaction_interval_seconds_since_last_operation`

Default: 86400

One of the triggering conditions of BaseCompaction: the interval since the last BaseCompaction

### `base_compaction_num_cumulative_deltas`

Default: 5

One of the triggering conditions of BaseCompaction: The limit of the number of Cumulative files to be reached. After reaching this limit, BaseCompaction will be triggered

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

Default: 5（MB）

Maximum disk write speed per second of BaseCompaction task

### `base_cumulative_delta_ratio`

Default: 0.3  （30%）

One of the trigger conditions of BaseCompaction: Cumulative file size reaches the proportion of Base file

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

### `transfer_large_data_by_brpc`

* Type: bool
* Description: This configuration is used to control whether to serialize the protoBuf request and embed the Tuple/Block data into the controller attachment and send it through http brpc when the length of the Tuple/Block data is greater than 1.8G. To avoid errors when the length of the protoBuf request exceeds 2G: Bad request, error_text=[E1003]Fail to compress request. In the past version, after putting Tuple/Block data in the attachment, it was sent through the default baidu_std brpc, but when the attachment exceeds 2G, it will be truncated. There is no 2G limit for sending through http brpc.
* Default value: true

### `brpc_num_threads`

This configuration is mainly used to modify the number of bthreads for brpc. The default value is set to -1, which means the number of bthreads is #cpu-cores.

User can set this configuration to a larger value to get better QPS performance. For more information, please refer to `https://github.com/apache/incubator-brpc/blob/master/docs/cn/benchmark.md`

### `brpc_port`

* Type: int32
* Description: The port of BRPC on BE, used for communication between BEs
* Default value: 9060

### `buffer_pool_clean_pages_limit`

default: 50%

Clean up pages that may be saved by the buffer pool

### `buffer_pool_limit`

* Type: string
* Description: The largest allocatable memory of the buffer pool
* Default value: 20%

The maximum amount of memory available in the BE buffer pool. The buffer pool is a new memory management structure of BE, which manages the memory by the buffer page and enables spill data to disk. The memory for all concurrent queries will be allocated from the buffer pool. The current buffer pool only works on **AggregationNode** and **ExchangeNode**.

### `check_auto_compaction_interval_seconds`

* Type: int32
* Description: Check the configuration of auto compaction in seconds when auto compaction disabled.
* Default value: 5

### `check_consistency_worker_count`

Default: 1

The number of worker threads to calculate the checksum of the tablet

### `chunk_reserved_bytes_limit`

Default: 20%

The reserved bytes limit of Chunk Allocator, usually set as a percentage of mem_limit. defaults to bytes if no unit is given, the number of bytes must be a multiple of 2. must larger than 0. and if larger than physical memory size, it will be set to physical memory size. increase this variable can improve performance, but will acquire more free memory which can not be used by other modules.

### `clear_transaction_task_worker_count`

Default: 1

Number of threads used to clean up transactions

### `clone_worker_count`

Default: 3

Number of threads used to perform cloning tasks

### `cluster_id`

* Type: int32
* Description: Configure the cluster id to which the BE belongs.
* Default value: -1

This value is usually delivered by the FE to the BE by the heartbeat, no need to configure. When it is confirmed that a BE belongs to a certain Drois cluster, it can be configured. The cluster_id file under the data directory needs to be modified to make sure same as this parament.

### `column_dictionary_key_ratio_threshold`

Default: 0

The value ratio of string type, less than this ratio, using dictionary compression algorithm

### `column_dictionary_key_size_threshold`

Default: 0

Dictionary compression column size, less than this value using dictionary compression algorithm

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

tablet_score = compaction_tablet_scan_frequency_factor * tablet_scan_frequency + compaction_tablet_compaction_score_factor * compaction_score

### `compaction_task_num_per_disk`

* Type: int32
* Description: The number of compaction tasks which execute in parallel for a disk(HDD).
* Default value: 2

### `compaction_task_num_per_fast_disk`

* Type: int32
* Description: The number of compaction tasks which execute in parallel for a fast disk(SSD).
* Default value: 4

### `compress_rowbatches`

* Type: bool
* Description: enable to use Snappy compression algorithm for data compression when serializing RowBatch
* Default value: true

### `create_tablet_worker_count`

Default: 3

Number of worker threads for BE to create a tablet

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

Default: 104857600

One of the trigger conditions of BaseCompaction: Singleton file size limit, 100MB

### cumulative_compaction_trace_threshold

* Type: int32
* Description: Threshold to logging cumulative compaction's trace information, in seconds
* Default value: 10

Similar to `base_compaction_trace_threshold`.

### disable_compaction_trace_log

* Type: bool
* Description: disable the trace log of compaction
* Default value: true

If set to true, the `cumulative_compaction_trace_threshold` and `base_compaction_trace_threshold` won't work and log is disabled.

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

Default: 3

Number of threads performing data deletion tasks

### `disable_mem_pools`

Default: false

Whether to disable the memory cache pool, it is not disabled by default

### `disable_storage_page_cache`

* Type: bool
* Description: Disable to use page cache for index caching, this configuration only takes effect in BETA storage format, usually it is recommended to false
* Default value: false

### `disk_stat_monitor_interval`

Default: 5（s）

Disk status check interval

### `doris_cgroups`

Default: empty

Cgroups assigned to doris

### `doris_max_pushdown_conjuncts_return_rate`

* Type: int32
* Description: When BE performs HashJoin, it will adopt a dynamic partitioning method to push the join condition to OlapScanner. When the data scanned by OlapScanner is larger than 32768 rows, BE will check the filter condition. If the filter rate of the filter condition is lower than this configuration, Doris will stop using the dynamic partition clipping condition for data filtering.
* Default value: 90

### `doris_max_scan_key_num`

* Type: int
* Description: Used to limit the maximum number of scan keys that a scan node can split in a query request. When a conditional query request reaches the scan node, the scan node will try to split the conditions related to the key column in the query condition into multiple scan key ranges. After that, these scan key ranges will be assigned to multiple scanner threads for data scanning. A larger value usually means that more scanner threads can be used to increase the parallelism of the scanning operation. However, in high concurrency scenarios, too many threads may bring greater scheduling overhead and system load, and will slow down the query response speed. An empirical value is 50. This configuration can be configured separately at the session level. For details, please refer to the description of `max_scan_key_num` in [Variables](../../../advanced/variables).
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

Default: 16384

The maximum number of data rows returned by each scanning thread in a single execution

### `doris_scanner_thread_pool_queue_size`

* Type: int32
* Description: The queue length of the Scanner thread pool. In Doris' scanning tasks, each Scanner will be submitted as a thread task to the thread pool waiting to be scheduled, and after the number of submitted tasks exceeds the length of the thread pool queue, subsequent submitted tasks will be blocked until there is a empty slot in the queue. 
* Default value: 102400

### `doris_scanner_thread_pool_thread_num`

* Type: int32
* Description: The number of threads in the Scanner thread pool. In Doris' scanning tasks, each Scanner will be submitted as a thread task to the thread pool to be scheduled. This parameter determines the size of the Scanner thread pool.
* Default value: 48

### `download_low_speed_limit_kbps`

Default: 50 (KB/s)

Minimum download speed

### `download_low_speed_time`

Default: 300（s）

Download time limit, 300 seconds by default

### `download_worker_count`

Default: 1

The number of download threads, the default is 1

### `drop_tablet_worker_count`

Default: 3

Number of threads to delete tablet

### `enable_metric_calculator`

Default: true

If set to true, the metric calculator will run to collect BE-related indicator information, if set to false, it will not run

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

Default: true

User control to turn on and off system indicators.

### `enable_token_check`

Default: true

Used for forward compatibility, will be removed later.

### `es_http_timeout_ms`

Default: 5000 （ms）

The timeout period for connecting to ES via http, the default is 5 seconds.

### `enable_stream_load_record`

Default: false

Whether to enable stream load record function, the default is false，Disable stream load record.

### `es_scroll_keepalive`

Default: 5m

es scroll Keeplive hold time, the default is 5 minutes

### `etl_thread_pool_queue_size`

Default: 256

The size of the ETL thread pool

### `etl_thread_pool_size`

### `exchg_node_buffer_size_bytes`

* Type: int32
* Description: The size of the Buffer queue of the ExchangeNode node, in bytes. After the amount of data sent from the Sender side is larger than the Buffer size of ExchangeNode, subsequent data sent will block until the Buffer frees up space for writing.
* Default value: 10485760

### `file_descriptor_cache_capacity`

Default: 32768

File handle cache capacity, 32768 file handles are cached by default.

### `cache_clean_interval`

Default: 1800(s)

File handle cache cleaning interval, used to clean up file handles that have not been used for a long time.
Also the clean interval of Segment Cache.

### `flush_thread_num_per_store`

Default: 2

The number of threads used to refresh the memory table per store

### `force_recovery`

### `fragment_pool_queue_size`

Default: 2048

The upper limit of query requests that can be processed on a single node

### `fragment_pool_thread_num_min`

Default: 64

### `fragment_pool_thread_num_max`

Default: 256

The above two parameters are to set the number of query threads. By default, a minimum of 64 threads will be started, subsequent query requests will dynamically create threads, and a maximum of 256 threads will be created.

### `heartbeat_service_port`
* Type: int32
* Description: Heartbeat service port (thrift) on BE, used to receive heartbeat from FE
* Default value: 9050

### `heartbeat_service_thread_count`

* Type: int32
* Description: The number of threads that execute the heartbeat service on BE. the default is 1, it is not recommended to modify
* Default value: 1

### `ignore_broken_disk`

Default: false

When BE start, If there is a broken disk, BE process will exit by default.Otherwise, we will ignore the broken disk

### `ignore_load_tablet_failure`
When BE starts, it will check all the paths under the storage_root_path in configuration.

`ignore_broken_disk=true`

If the path does not exist or the file under the path cannot be read or written (broken disk), it will be ignored. If there are any other available paths, the startup will not be interrupted.

`ignore_broken_disk=false`

If the path does not exist or the file under the path cannot be read or written (bad disk), the startup will fail and exit.

The default value is false.
```
load tablets from header failed, failed tablets size: xxx, path=xxx
```

Indicates how many tablets in the data directory failed to load. At the same time, the log will also contain specific information about the tablet that failed to load. At this time, manual intervention is required to troubleshoot the cause of the error. After investigation, there are usually two ways to recover:

1. The tablet information cannot be repaired. If the other copies are normal, you can delete the wrong tablet with the `meta_tool` tool.
2. Set `ignore_load_tablet_failure` to true, BE will ignore these faulty tablets and start normally

### ignore_rowset_stale_unconsistent_delete

* Type: boolean
* Description:It is used to decide whether to delete the outdated merged rowset if it cannot form a consistent version path.
* Default: false

The merged expired rowset version path will be deleted after half an hour. In abnormal situations, deleting these versions will result in the problem that the consistent path of the query cannot be constructed. When the configuration is false, the program check is strict and the program will directly report an error and exit.
When configured as true, the program will run normally and ignore this error. In general, ignoring this error will not affect the query, only when the merged version is dispatched by fe, -230 error will appear.

### inc_rowset_expired_sec

Default: 1800 （s）

Import activated data, storage engine retention time, used for incremental cloning

### `index_stream_cache_capacity`

Default: 10737418240

BloomFilter/Min/Max and other statistical information cache capacity

### `kafka_api_version_request`

Default: true

If the dependent Kafka version is lower than 0.10.0.0, this value should be set to false.

### `kafka_broker_version_fallback`

Default: 0.10.0

If the dependent Kafka version is lower than 0.10.0.0, the value set by the fallback version kafka_broker_version_fallback will be used if the value of kafka_api_version_request is set to false, and the valid values are: 0.9.0.x, 0.8.x.y.

### `load_data_reserve_hours`

Default: 4（hour）

Used for mini load. The mini load data file will be deleted after this time

### `load_error_log_reserve_hours`

Default: 48 （hour）

The load error log will be deleted after this time

### `load_process_max_memory_limit_bytes`

Default: 107374182400

The upper limit of memory occupied by all imported threads on a single node, default value: 100G

Set these default values very large, because we don't want to affect load performance when users upgrade Doris. If necessary, the user should set these configurations correctly.

### `load_process_max_memory_limit_percent`

Default: 50 (%)

The percentage of the upper memory limit occupied by all imported threads on a single node, the default is 50%

Set these default values very large, because we don't want to affect load performance when users upgrade Doris. If necessary, the user should set these configurations correctly

### `load_process_soft_mem_limit_percent`

Default: 50 (%)

The soft limit refers to the proportion of the load memory limit of a single node. For example, the load memory limit for all load tasks is 20GB, and the soft limit defaults to 50% of this value, that is, 10GB. When the load memory usage exceeds the soft limit, the job with the largest memory consuption will be selected to be flushed to release the memory space, the default is 50%

### `log_buffer_level`

Default: empty

The log flushing strategy is kept in memory by default

### `madvise_huge_pages`

Default: false

Whether to use linux memory huge pages, not enabled by default

### `make_snapshot_worker_count`

Default: 5

Number of threads making snapshots

### `max_client_cache_size_per_host`

Default: 10

The maximum number of client caches per host. There are multiple client caches in BE, but currently we use the same cache size configuration. If necessary, use different configurations to set up different client-side caches

### `max_base_compaction_threads`

* Type: int32
* Description: The maximum of thread number in base compaction thread pool.
* Default value: 4

### `max_cumu_compaction_threads`

* Type: int32
* Description: The maximum of thread number in cumulative compaction thread pool.
* Default value: 10

### `max_consumer_num_per_group`

Default: 3

The maximum number of consumers in a data consumer group, used for routine load

### `min_cumulative_compaction_num_singleton_deltas`

Default: 5

Cumulative compaction strategy: the minimum number of incremental files

### `max_cumulative_compaction_num_singleton_deltas`

Default: 1000

Cumulative compaction strategy: the maximum number of incremental files

### `max_download_speed_kbps`

Default: 50000 （KB/s）

Maximum download speed limit

### `max_free_io_buffers`

Default: 128

For each io buffer size, the maximum number of buffers that IoMgr will reserve ranges from 1024B to 8MB buffers, up to about 2GB buffers.

### `max_garbage_sweep_interval`

Default: 3600

The maximum interval for disk garbage cleaning, the default is one hour

### `max_memory_sink_batch_count`

Default: 20

The maximum external scan cache batch count, which means that the cache max_memory_cache_batch_count * batch_size row, the default is 20, and the default value of batch_size is 1024, which means that 20 * 1024 rows will be cached

### `max_percentage_of_error_disk`

* Type: int32
* Description: The storage engine allows the percentage of damaged hard disks to exist. After the damaged hard disk exceeds the changed ratio, BE will automatically exit.
* Default value: 0

### `max_pushdown_conditions_per_column`

* Type: int
* Description: Used to limit the maximum number of conditions that can be pushed down to the storage engine for a single column in a query request. During the execution of the query plan, the filter conditions on some columns can be pushed down to the storage engine, so that the index information in the storage engine can be used for data filtering, reducing the amount of data that needs to be scanned by the query. Such as equivalent conditions, conditions in IN predicates, etc. In most cases, this parameter only affects queries containing IN predicates. Such as `WHERE colA IN (1,2,3,4, ...)`. A larger number means that more conditions in the IN predicate can be pushed to the storage engine, but too many conditions may cause an increase in random reads, and in some cases may reduce query efficiency. This configuration can be individually configured for session level. For details, please refer to the description of `max_pushdown_conditions_per_column` in [Variables](../../advanced/variables.md).
* Default value: 1024

* Example

    The table structure is `id INT, col2 INT, col3 varchar (32), ...`.

    The query is `... WHERE id IN (v1, v2, v3, ...)`

    If the number of conditions in the IN predicate exceeds the configuration, try to increase the configuration value and observe whether the query response has improved.

### `max_runnings_transactions_per_txn_map`

Default: 100

Max number of txns for every txn_partition_map in txn manager, this is a self protection to avoid too many txns saving in manager

### `max_send_batch_parallelism_per_job`

* Type: int
* Description: Max send batch parallelism for OlapTableSink. The value set by the user for `send_batch_parallelism` is not allowed to exceed `max_send_batch_parallelism_per_job`, if exceed, the value of `send_batch_parallelism` would be `max_send_batch_parallelism_per_job`.
* Default value: 5

### `max_tablet_num_per_shard`

Default: 1024

The number of sliced tablets, plan the layout of the tablet, and avoid too many tablet subdirectories in the repeated directory

### `max_tablet_version_num`

* Type: int
* Description: Limit the number of versions of a single tablet. It is used to prevent a large number of version accumulation problems caused by too frequent import or untimely compaction. When the limit is exceeded, the import task will be rejected.
* Default value: 500

### `mem_limit`

* Type: string
* Description: Limit the percentage of the server's maximum memory used by the BE process. It is used to prevent BE memory from occupying to many the machine's memory. This parameter must be greater than 0. When the percentage is greater than 100%, the value will default to 100%.
* Default value: 80%

### `memory_limitation_per_thread_for_schema_change`

Default: 2 （G）

Maximum memory allowed for a single schema change task

### `memory_maintenance_sleep_time_s`

Default: 10

Sleep time (in seconds) between memory maintenance iterations

### `memory_max_alignment`

Default: 16

Maximum alignment memory

### `read_size`

Default: 8388608

The read size is the read size sent to the os. There is a trade-off between latency and the whole process, getting to keep the disk busy but not introducing seeks. For 8 MB reads, random io and sequential io have similar performance

### `min_buffer_size`

Default: 1024

Minimum read buffer size (in bytes)

### `min_compaction_failure_interval_sec`

* Type: int32
* Description: During the cumulative compaction process, when the selected tablet fails to be merged successfully, it will wait for a period of time before it may be selected again. The waiting period is the value of this configuration.
* Default value: 600
* Unit: seconds

### `min_compaction_threads`

* Type: int32
* Description: The minimum of thread number in compaction thread pool.
* Default value: 10

### `min_file_descriptor_number`

Default: 60000

The lower limit required by the file handle limit of the BE process

### `min_garbage_sweep_interval`

Default: 180

The minimum interval between disk garbage cleaning, time seconds

### `mmap_buffers`

Default: false

Whether to use mmap to allocate memory, not used by default

### `num_cores`

* Type: int32
* Description: The number of CPU cores that BE can use. When the value is 0, BE will obtain the number of CPU cores of the machine from /proc/cpuinfo.
* Default value: 0

### `num_disks`

Defalut: 0

Control the number of disks on the machine. If it is 0, it comes from the system settings

### `num_threads_per_core`

Default: 3

Control the number of threads that each core runs. Usually choose 2 times or 3 times the number of cores. This keeps the core busy without causing excessive jitter

### `num_threads_per_disk`

Default: 0

The maximum number of threads per disk is also the maximum queue depth of each disk

### `number_slave_replica_download_threads`

Default: 64

Number of threads for slave replica synchronize data, used for single replica load.

### `number_tablet_writer_threads`

Default: 16

Number of tablet write threads

### `path_gc_check`

Default: true

Whether to enable the recycle scan data thread check, it is enabled by default

### `path_gc_check_interval_second`

Default: 86400

Recycle scan data thread check interval, in seconds

### `path_gc_check_step`

Default: 1000

### `path_gc_check_step_interval_ms`

Default: 10 (ms)

### `path_scan_interval_second`

Default: 86400

### `pending_data_expire_time_sec`

Default: 1800 

The maximum duration of unvalidated data retained by the storage engine, the default unit: seconds

### `periodic_counter_update_period_ms`

Default: 500

Update rate counter and sampling counter cycle, default unit: milliseconds

### `plugin_path`

Default: ${DORIS_HOME}/plugin

pliugin path

### `port`

* Type: int32
* Description: The port used in UT. Meaningless in the actual environment and can be ignored.
* Default value: 20001

### `pprof_profile_dir`

Default : ${DORIS_HOME}/log

pprof profile save directory

### `priority_networks`

Default: empty

Declare a selection strategy for those servers with many IPs. Note that at most one ip should match this list. This is a semicolon-separated list in CIDR notation, such as 10.10.10.0/24. If there is no IP matching this rule, one will be randomly selected

### `priority_queue_remaining_tasks_increased_frequency`

Default: 512

 the increased frequency of priority for remaining tasks in BlockingPriorityQueue

### `publish_version_worker_count`

Default: 8

the count of thread to publish version

### `pull_load_task_dir`

Default: ${DORIS_HOME}/var/pull_load

Pull the directory of the laod task

### `push_worker_count_high_priority`

Default: 3

Import the number of threads for processing HIGH priority tasks

### `push_worker_count_normal_priority`

Default: 3

Import the number of threads for processing NORMAL priority tasks

### `query_scratch_dirs`

+ Type: string
+ Description: The directory selected by BE to store temporary data during spill to disk. which is similar to the storage path configuration, multiple directories are separated by ;.
+ Default value: ${DORIS_HOME}

### `release_snapshot_worker_count`

Default: 5

Number of threads releasing snapshots

### `report_disk_state_interval_seconds`

Default: 60

The interval time for the agent to report the disk status to FE, unit (seconds)

### `report_tablet_interval_seconds`

Default: 60

The interval time for the agent to report the olap table to the FE, in seconds

### `report_task_interval_seconds`

Default: 10

The interval time for the agent to report the task signature to FE, unit (seconds)

### `result_buffer_cancelled_interval_time`

Default: 300

Result buffer cancellation time (unit: second)

### `routine_load_thread_pool_size`

Default: 10

The thread pool size of the routine load task. This should be greater than the FE configuration'max_concurrent_task_num_per_be' (default 5)

### `row_nums_check`

Default: true

Check row nums for BE/CE and schema change. true is open, false is closed

### `row_step_for_compaction_merge_log`

* Type: int64
* Description: Merge log will be printed for each "row_step_for_compaction_merge_log" rows merged during compaction. If the value is set to 0, merge log will not be printed.
* Default value: 0
* Dynamically modify: true

### `scan_context_gc_interval_min`

Default: 5

This configuration is used for the context gc thread scheduling cycle. Note: The unit is minutes, and the default is 5 minutes

### `send_batch_thread_pool_thread_num`

* Type: int32
* Description: The number of threads in the SendBatch thread pool. In NodeChannels' sending data tasks, the SendBatch operation of each NodeChannel will be submitted as a thread task to the thread pool to be scheduled. This parameter determines the size of the SendBatch thread pool.
* Default value: 64

### `send_batch_thread_pool_queue_size`

* Type: int32
* Description: The queue length of the SendBatch thread pool. In NodeChannels' sending data tasks,  the SendBatch operation of each NodeChannel will be submitted as a thread task to the thread pool waiting to be scheduled, and after the number of submitted tasks exceeds the length of the thread pool queue, subsequent submitted tasks will be blocked until there is a empty slot in the queue.
* Default value: 102400

### `download_cache_thread_pool_thread_num`

* Type: int32
* Description: The number of threads in the DownloadCache thread pool. In the download cache task of FileCache, the download cache operation will be submitted to the thread pool as a thread task and wait to be scheduled. This parameter determines the size of the DownloadCache thread pool.
* Default value: 48

### `download_cache_thread_pool_queue_size`

* Type: int32
* Description: The number of threads in the DownloadCache thread pool. In the download cache task of FileCache, the download cache operation will be submitted to the thread pool as a thread task and wait to be scheduled. After the number of submitted tasks exceeds the length of the thread pool queue, subsequent submitted tasks will be blocked until there is a empty slot in the queue.
* Default value: 102400

### `download_cache_buffer_size`

* Type: int64
* Description: The size of the buffer used to receive data when downloading the cache.
* Default value: 10485760

### `single_replica_load_brpc_port`

* Type: int32
* Description: The port of BRPC on BE, used for single replica load. There is a independent BRPC thread pool for the communication between the Master replica and Slave replica during single replica load, which prevents data synchronization between the replicas from preempt the thread resources for data distribution and query tasks when the load concurrency is large.
* Default value: 9070

### `single_replica_load_brpc_num_threads`

* Type: int32
* Description: This configuration is mainly used to modify the number of bthreads for single replica load brpc. When the load concurrency increases, you can adjust this parameter to ensure that the Slave replica synchronizes data files from the Master replica timely.
* Default value: 64

### `single_replica_load_download_port`

* Type: int32
* Description: The port of http for segment download on BE, used for single replica load. There is a independent HTTP thread pool for the Slave replica to download segments during single replica load, which prevents data synchronization between the replicas from preempt the thread resources for other http tasks when the load concurrency is large.
* Default value: 8050

### `single_replica_load_download_num_workers`

* Type: int32
* Description: This configuration is mainly used to modify the number of http threads for segment download, used for single replica load. When the load concurrency increases, you can adjust this parameter to ensure that the Slave replica synchronizes data files from the Master replica timely.
* Default value: 64

### `slave_replica_writer_rpc_timeout_sec`

* Type: int32
* Description: This configuration is mainly used to modify timeout of brpc between master replica and slave replica, used for single replica load.
* Default value: 60

### `sleep_one_second`

+ Type: int32
+ Description: Global variables, used for BE thread sleep for 1 seconds, should not be modified
+ Default value: 1

### `small_file_dir`

Default: ${DORIS_HOME}/lib/small_file/

Directory for saving files downloaded by SmallFileMgr

### `snapshot_expire_time_sec`

Default: 172800

Snapshot file cleaning interval, default value: 48 hours

### `status_report_interval`

Default: 5

Interval between profile reports; unit: seconds

### `storage_flood_stage_left_capacity_bytes`

Default: 1073741824

The min bytes that should be left of a data dir，default value:1G

### `storage_flood_stage_usage_percent`

Default: 90 （90%）

The storage_flood_stage_usage_percent and storage_flood_stage_left_capacity_bytes configurations limit the maximum usage of the capacity of the data directory.

### `storage_medium_migrate_count`

Default: 1

the count of thread to clone

### `storage_page_cache_limit`

Default: 20%

Cache for storage page size

### `storage_page_cache_shard_size`

Default: 16

Shard size of StoragePageCache, the value must be power of two. It's recommended to set it to a value close to the number of BE cores in order to reduce lock contentions.

### `index_page_cache_percentage`
* Type: int32
* Description: Index page cache as a percentage of total storage page cache, value range is [0, 100]
* Default value: 10

### `storage_root_path`

* Type: string

* Description: data root path, separate by ';'.you can specify the storage medium of each root path, HDD or SSD. you can add capacity limit at the end of each root path, separate by ','.  
  If the user does not use a mix of SSD and HDD disks, they do not need to configure the configuration methods in Example 1 and Example 2 below, but only need to specify the storage directory; they also do not need to modify the default storage media configuration of FE.  

    eg.1: `storage_root_path=/home/disk1/doris.HDD;/home/disk2/doris.SSD;/home/disk2/doris`
  
    * 1./home/disk1/doris.HDD, indicates that the storage medium is HDD;
    * 2./home/disk2/doris.SSD, indicates that the storage medium is SSD;
    * 3./home/disk2/doris, indicates that the storage medium is HDD by default
  
    eg.2: `storage_root_path=/home/disk1/doris,medium:hdd;/home/disk2/doris,medium:ssd`
  
    * 1./home/disk1/doris,medium:hdd，indicates that the storage medium is HDD;
    * 2./home/disk2/doris,medium:ssd，indicates that the storage medium is SSD;

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

Default: 1200

The lifetime of TabletsChannel. If the channel does not receive any data at this time, the channel will be deleted, unit: second

### `sync_tablet_meta`

Default: false

Whether the storage engine opens sync and keeps it to the disk

### `sys_log_dir`

* Type: string
* Description: Storage directory of BE log data
* Default: ${DORIS_HOME}/log

### `sys_log_level`

INFO

Log Level: INFO < WARNING < ERROR < FATAL

### `sys_log_roll_mode`

Default: SIZE-MB-1024

The size of the log split, one log file is split every 1G

### `sys_log_roll_num`

Default: 10

Number of log files kept

### `sys_log_verbose_level`

Defaultl: 10

Log display level, used to control the log output at the beginning of VLOG in the code

### `sys_log_verbose_modules`

Default: empty

Log printing module, writing olap will only print the log under the olap module

### `tablet_map_shard_size`

Default: 1

tablet_map_lock fragment size, the value is 2^n, n=0,1,2,3,4, this is for better tablet management

### `tablet_meta_checkpoint_min_interval_secs`

Default: 600（s）

The polling interval of the TabletMeta Checkpoint thread

### `tablet_meta_checkpoint_min_new_rowsets_num`

### `tablet_scan_frequency_time_node_interval_second`

* Type: int64
* Description: Time interval to record the metric 'query_scan_count' and timestamp in second for the purpose of  calculating tablet scan frequency during a latest period of time at the present.
* Default: 300

### `tablet_stat_cache_update_interval_second`

default: 10

The minimum number of Rowsets for TabletMeta Checkpoint

### `tablet_rowset_stale_sweep_time_sec`

* Type: int64
* Description: It is used to control the expiration time of cleaning up the merged rowset version. When the current time now() minus the max created rowset‘s create time in a version path is greater than tablet_rowset_stale_sweep_time_sec, the current path is cleaned up and these merged rowsets are deleted, the unit is second.
* Default: 1800

When writing is too frequent and the disk time is insufficient, you can configure less tablet_rowset_stale_sweep_time_sec. However, if this time is less than 5 minutes, it may cause fe to query the version that has been merged, causing a query -230 error.

### `tablet_writer_open_rpc_timeout_sec`

Default: 300

Update interval of tablet state cache, unit: second

The RPC timeout for sending a Batch (1024 lines) during import. The default is 60 seconds. Since this RPC may involve writing multiple batches of memory, the RPC timeout may be caused by writing batches, so this timeout can be adjusted to reduce timeout errors (such as send batch fail errors). Also, if you increase the write_buffer_size configuration, you need to increase this parameter as well.

### `tablet_writer_ignore_eovercrowded`

* Type: bool
* Description: Used to ignore brpc error '[E1011]The server is overcrowded' when writing data. 
* Default value: false

When meet '[E1011]The server is overcrowded' error, you can tune the configuration `brpc_socket_max_unwritten_bytes`, but it can't be modified at runtime. Set it to `true` to avoid writing failed temporarily. Notice that, it only effects `write`, other rpc requests will still check if overcrowded.

### `tc_free_memory_rate`

Default: 20   (%)

Available memory, value range: [0-100]

### `tc_max_total_thread_cache_bytes`

* Type: int64
* Description: Used to limit the total thread cache size in tcmalloc. This limit is not a hard limit, so the actual thread cache usage may exceed this limit. For details, please refer to [TCMALLOC\_MAX\_TOTAL\_THREAD\_CACHE\_BYTES](https://gperftools.github.io/gperftools/tcmalloc.html)
* Default: 1073741824

If the system is found to be in a high-stress scenario and a large number of threads are found in the tcmalloc lock competition phase through the BE thread stack, such as a large number of `SpinLock` related stacks, you can try increasing this parameter to improve system performance. [Reference](https://github.com/gperftools/gperftools/issues/1111)

### `tc_use_memory_min`

Default: 10737418240

The minimum memory of TCmalloc, when the memory used is less than this, it is not returned to the operating system

### `thrift_client_retry_interval_ms`

* Type: int64
* Description: Used to set retry interval for thrift client in be to avoid avalanche disaster in fe thrift server, the unit is ms.
* Default: 1000

### `thrift_connect_timeout_seconds`

Default: 3

The default thrift client connection timeout time (unit: seconds)

### `thrift_rpc_timeout_ms`

Default: 5000

thrift default timeout time, default: 5 seconds

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

Default: 259200

The interval for cleaning the recycle bin is 72 hours. When the disk space is insufficient, the file retention period under trash may not comply with this parameter

### `txn_commit_rpc_timeout_ms`

Default: 10000

txn submit rpc timeout, the default is 10 seconds

### `txn_map_shard_size`

Default: 128

txn_map_lock fragment size, the value is 2^n, n=0,1,2,3,4. This is an enhancement to improve the performance of managing txn

### `txn_shard_size`

Default: 1024

txn_lock shard size, the value is 2^n, n=0,1,2,3,4, this is an enhancement function that can improve the performance of submitting and publishing txn

### `unused_rowset_monitor_interval`

Default: 30

Time interval for clearing expired Rowset, unit: second

### `upload_worker_count`

Default: 1

Maximum number of threads for uploading files

### `user_function_dir`

${DORIS_HOME}/lib/udf

udf function directory

### `webserver_num_workers`

Default: 48

Webserver default number of worker threads

### `webserver_port`

* Type: int32
* Description: Service port of http server on BE
* Default: 8040

### `write_buffer_size`

Default: 104857600

The size of the buffer before flashing

Imported data is first written to a memory block on the BE, and only written back to disk when this memory block reaches the threshold. The default size is 100MB. too small a threshold may result in a large number of small files on the BE. This threshold can be increased to reduce the number of files. However, too large a threshold may cause RPC timeouts

### `zone_map_row_num_threshold`

* Type: int32
* Description: If the number of rows in a page is less than this value, no zonemap will be created to reduce data expansion
* Default: 20

### `aws_log_level`

* Type: int32

* Description: log level of AWS SDK, 
  ```
     Off = 0,
     Fatal = 1,
     Error = 2,
     Warn = 3,
     Info = 4,
     Debug = 5,
     Trace = 6
  ```
  
* Default: 3

### `enable_tcmalloc_hook`

* Type: bool
* Description: Whether Hook TCmalloc new/delete, currently consume/release tls mem tracker in Hook.
* Default: true

### `mem_tracker_consume_min_size_bytes`

* Type: int32
* Description: The minimum length of TCMalloc Hook when consume/release MemTracker. Consume size smaller than this value will continue to accumulate to avoid frequent calls to consume/release of MemTracker. Decreasing this value will increase the frequency of consume/release. Increasing this value will cause MemTracker statistics to be inaccurate. Theoretically, the statistical value of a MemTracker differs from the true value = ( mem_tracker_consume_min_size_bytes * the number of BE threads where the MemTracker is located).
* Default: 1048576

### `max_segment_num_per_rowset`

* Type: int32
* Description: Used to limit the number of segments in the newly generated rowset when importing. If the threshold is exceeded, the import will fail with error -238. Too many segments will cause compaction to take up a lot of memory and cause OOM errors.
* Default value: 200

### `remote_storage_read_buffer_mb`

* Type: int32
* Description: The cache size used when reading files on hdfs or object storage.
* Default value: 16MB

Increasing this value can reduce the number of calls to read remote data, but it will increase memory overhead.

### `external_table_connect_timeout_sec`

* Type: int32
* Description: The timeout when establishing connection with external table such as ODBC table.
* Default value: 5 seconds

### `segment_cache_capacity`

* Type: int32
* Description: The maximum number of Segments cached by Segment Cache.
* Default value: 1000000

The default value is currently only an empirical value, and may need to be modified according to actual scenarios. Increasing this value can cache more segments and avoid some IO. Decreasing this value will reduce memory usage.

### `auto_refresh_brpc_channel`

* Type: bool
* Description: When obtaining a brpc connection, judge the availability of the connection through hand_shake rpc, and re-establish the connection if it is not available 。
* Default value: false

### `high_priority_flush_thread_num_per_store`

* Type: int32
* Description: The number of flush threads per store path allocated for the high priority import task.
* Default value: 1

### `routine_load_consumer_pool_size`

* Type: int32
* Description: The number of caches for the data consumer used by the routine load.
* Default: 10

### `load_task_high_priority_threshold_second`

* Type: int32
* Description: When the timeout of an import task is less than this threshold, Doris will consider it to be a high priority task. High priority tasks use a separate pool of flush threads.
* Default: 120

### `min_load_rpc_timeout_ms`

* Type: int32
* Description: The minimum timeout for each rpc in the load job.
* Default: 20

Translated with www.DeepL.com/Translator (free version)

### `doris_scan_range_max_mb`
* Type: int32
* Description: The maximum amount of data read by each OlapScanner.
* Default: 1024

### `enable_quick_compaction`
* Type: bool
* Description: enable quick compaction,It is mainly used in the scenario of frequent import of small amount of data. The problem of -235 can be effectively avoided by merging the imported versions in time through the mechanism of rapid compaction. The definition of small amount of data is currently defined according to the number of rows
* Default: true

### `quick_compaction_max_rows`
* Type: int32
* Description: When the number of imported rows is less than this value, it is considered that this import is an import of small amount of data, which will be selected during quick compaction
* Default: 1000

### `quick_compaction_batch_size`
* Type: int32
* Description: trigger time, when import times reach quick_compaction_batch_size will trigger immediately 
* Default: 10

### `quick_compaction_min_rowsets`
* Type: int32
* Description: at least the number of versions to be compaction, and the number of rowsets with a small amount of data in the selection. If it is greater than this value, the real compaction will be carried out
* Default: 10

### `generate_cache_cleaner_task_interval_sec`
* Type：int64
* Description：Cleaning interval of cache files, in seconds
* Default：43200（12 hours）

### `file_cache_type`
* Type：string
* Description：Type of cache file. whole_ file_ Cache: download the entire segment file, sub_ file_ Cache: the segment file is divided into multiple files by size.
* Default：""

### `max_sub_cache_file_size`
* Type：int64
* Description：Cache files using sub_ file_ The maximum size of the split file during cache, unit: B
* Default：104857600（100MB）

### `file_cache_alive_time_sec`
* Type：int64
* Description：Save time of cache file, in seconds
* Default：604800（1 week）
