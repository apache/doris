---
{
    "title": "BE Configuration",
    "language": "en",
    "toc_min_heading_level": 2,
    "toc_max_heading_level": 4
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
    curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}
    ```

   In version 0.13 and before, the configuration items modified in this way will become invalid after the BE process restarts. In 0.14 and later versions, the modified configuration can be persisted through the following command. The modified configuration items are stored in the `be_custom.conf` file.

    ```
    curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}\&persist=true
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

### Services

#### `be_port`

* Type: int32
* Description:  The port of the thrift server on BE which used to receive requests from FE
* Default value: 9060

#### `heartbeat_service_port`

* Type: int32
* Description: Heartbeat service port (thrift) on BE, used to receive heartbeat from FE
* Default value: 9050

#### `webserver_port`

* Type: int32
* Description: Service port of http server on BE
* Default value: 8040

#### `brpc_port`

* Type: int32
* Description: The port of BRPC on BE, used for communication between BEs
* Default value: 8060

#### `arrow_flight_sql_port`

* Type: int32
* Description: The port of Arrow Flight SQL server on BE, used for communication between Arrow Flight Client and BE
* Default value: -1

#### `enable_https`

* Type: bool
* Description: Whether https is supported. If so, configure `ssl_certificate_path` and `ssl_private_key_path` in be.conf.
* Default value: false

#### `priority_networks`

* Description: Declare a selection strategy for those servers with many IPs. Note that at most one ip should match this list. This is a semicolon-separated list in CIDR notation, such as 10.10.10.0/24. If there is no IP matching this rule, one will be randomly selected
* Default value: blank

#### `storage_root_path`

* Type: string

* Description: data root path, separate by ';'.you can specify the storage medium of each root path, HDD or SSD. you can add capacity limit at the end of each root path, separate by ','.If the user does not use a mix of SSD and HDD disks, they do not need to configure the configuration methods in Example 1 and Example 2 below, but only need to specify the storage directory; they also do not need to modify the default storage media configuration of FE.

  eg.1: `storage_root_path=/home/disk1/doris.HDD;/home/disk2/doris.SSD;/home/disk2/doris`

    - 1./home/disk1/doris.HDD, indicates that the storage medium is HDD;
    - 2./home/disk2/doris.SSD, indicates that the storage medium is SSD;
    - 3./home/disk2/doris, indicates that the storage medium is HDD by default

  eg.2: `storage_root_path=/home/disk1/doris,medium:hdd;/home/disk2/doris,medium:ssd`

    - 1./home/disk1/doris,medium:hdd，indicates that the storage medium is HDD;
    - 2./home/disk2/doris,medium:ssd，indicates that the storage medium is SSD;

* Default value: ${DORIS_HOME}/storage

#### `heartbeat_service_thread_count`

* Type: int32
* Description: The number of threads that execute the heartbeat service on BE. the default is 1, it is not recommended to modify
* Default value: 1

#### `ignore_broken_disk`

* Type: bool
* Description: When BE starts, check ``storage_root_path`` All paths under configuration.

    -  `ignore_broken_disk=true`

  If the path does not exist or the file (bad disk) cannot be read or written under the path, the path will be ignored. If there are other available paths, the startup will not be interrupted.

    -  `ignore_broken_disk=false`

  If the path does not exist or the file (bad disk) cannot be read or written under the path, the system will abort the startup failure and exit.

* Default value: false

#### `mem_limit`

* Type: string
* Description: Limit the percentage of the server's maximum memory used by the BE process. It is used to prevent BE memory from occupying too the machine's memory. This parameter must be greater than 0. When the percentage is greater than 100%, the value will default to 100%.
* Default value: 90%

#### `cluster_id`

* Type: int32
* Description: Configure the cluster id to which the BE belongs.
    - This value is usually delivered by the FE to the BE by the heartbeat, no need to configure. When it is confirmed that a BE belongs to a certain Doris cluster, it can be configured. The cluster_id file under the data directory needs to be modified to make sure same as this parament.
* Default value: -1

#### `custom_config_dir`

* Description: Configure the location of the `be_custom.conf` file. The default is in the `conf/` directory.
    - In some deployment environments, the `conf/` directory may be overwritten due to system upgrades. This will cause the user modified configuration items to be overwritten. At this time, we can store `be_custom.conf` in another specified directory to prevent the configuration file from being overwritten.
* Default value: blank

#### `trash_file_expire_time_sec`

* Description: The interval for cleaning the recycle bin is 72 hours. When the disk space is insufficient, the file retention period under trash may not comply with this parameter
* Default value: 259200

#### `es_http_timeout_ms`

* Description: The timeout period for connecting to ES via http.
* Default value: 5000 (ms)

#### `es_scroll_keepalive`

* Description: es scroll keep-alive hold time
* Default value: 5 (m)

#### `external_table_connect_timeout_sec`

* Type: int32
* Description: The timeout when establishing connection with external table such as ODBC table.
* Default value: 5 seconds

#### `status_report_interval`

* Description: Interval between profile reports
* Default value: 5 seconds

#### `brpc_max_body_size`

* Description: This configuration is mainly used to modify the parameter `max_body_size` of brpc.

  - Sometimes the query fails and an error message of `body_size is too large` will appear in the BE log. This may happen when the SQL mode is "multi distinct + no group by + more than 1T of data".This error indicates that the packet size of brpc exceeds the configured value. At this time, you can avoid this error by increasing the configuration.

#### `brpc_socket_max_unwritten_bytes`

* Description: This configuration is mainly used to modify the parameter `socket_max_unwritten_bytes` of brpc.
  - Sometimes the query fails and an error message of `The server is overcrowded` will appear in the BE log. This means there are too many messages to buffer at the sender side, which may happen when the SQL needs to send large bitmap value. You can avoid this error by increasing the configuration.

#### `transfer_large_data_by_brpc`

* Type: bool
* Description: This configuration is used to control whether to serialize the protoBuf request and embed the Tuple/Block data into the controller attachment and send it through http brpc when the length of the Tuple/Block data is greater than 1.8G. To avoid errors when the length of the protoBuf request exceeds 2G: Bad request, error_text=[E1003]Fail to compress request. In the past version, after putting Tuple/Block data in the attachment, it was sent through the default baidu_std brpc, but when the attachment exceeds 2G, it will be truncated. There is no 2G limit for sending through http brpc.
* Default value: true

#### `brpc_num_threads`

* Description: This configuration is mainly used to modify the number of bthreads for brpc. The default value is set to -1, which means the number of bthreads is #cpu-cores.
  - User can set this configuration to a larger value to get better QPS performance. For more information, please refer to `https://github.com/apache/incubator-brpc/blob/master/docs/cn/benchmark.md`
* Default value: -1

#### `thrift_rpc_timeout_ms`

* Description: thrift default timeout time
* Default value: 60000

#### `thrift_client_retry_interval_ms`

* Type: int64
* Description: Used to set retry interval for thrift client in be to avoid avalanche disaster in fe thrift server, the unit is ms.
* Default value: 1000

#### `thrift_connect_timeout_seconds`

* Description: The default thrift client connection timeout time
* Default value: 3 (m)

#### `thrift_server_type_of_fe`

* Type: string
* Description:This configuration indicates the service model used by FE's Thrift service. The type is string and is case-insensitive. This parameter needs to be consistent with the setting of fe's thrift_server_type parameter. Currently there are two values for this parameter, `THREADED` and `THREAD_POOL`.

    - If the parameter is `THREADED`, the model is a non-blocking I/O model.

    - If the parameter is `THREAD_POOL`, the model is a blocking I/O model.

#### `txn_commit_rpc_timeout_ms`

* Description:txn submit rpc timeout
* Default value: 60,000 (ms)

#### `txn_map_shard_size`

* Description: txn_map_lock fragment size, the value is 2^n, n=0,1,2,3,4. This is an enhancement to improve the performance of managing txn
* Default value: 128

#### `txn_shard_size`

* Description: txn_lock shard size, the value is 2^n, n=0,1,2,3,4, this is an enhancement function that can improve the performance of submitting and publishing txn
* Default value: 1024

#### `unused_rowset_monitor_interval`

* Description: Time interval for clearing expired Rowset
* Default value: 30 (s)

#### `max_client_cache_size_per_host`

* Description: The maximum number of client caches per host. There are multiple client caches in BE, but currently we use the same cache size configuration. If necessary, use different configurations to set up different client-side caches
* Default value: 10

#### `string_type_length_soft_limit_bytes`

* Type: int32
* Description: The soft limit of the maximum length of String type.
* Default value: 1,048,576

#### `big_column_size_buffer`

* Type: int64
* Description: When using the odbc external table, if a column type of the odbc source table is HLL, CHAR or VARCHAR, and the length of the column value exceeds this value, the query will report an error 'column value length longer than buffer length'. You can increase this value
* Default value: 65535

#### `small_column_size_buffer`

* Type: int64
* Description:  When using the odbc external table, if a column type of the odbc source table is not HLL, CHAR or VARCHAR, and the length of the column value exceeds this value, the query will report an error 'column value length longer than buffer length'. You can increase this value
* Default value: 100

#### `jsonb_type_length_soft_limit_bytes`

* Type: int32
* Description: The soft limit of the maximum length of JSONB type.
* Default value: 1,048,576

### Query

#### `fragment_pool_queue_size`

* Description: The upper limit of query requests that can be processed on a single node
* Default value: 4096

#### `fragment_pool_thread_num_min`

* Description: Query the number of threads. By default, the minimum number of threads is 64.
* Default value: 64

#### `fragment_pool_thread_num_max`

* Description: Follow up query requests create threads dynamically, with a maximum of 512 threads created.
* Default value: 2048

#### `doris_max_pushdown_conjuncts_return_rate`

* Type: int32
* Description:  When BE performs HashJoin, it will adopt a dynamic partitioning method to push the join condition to OlapScanner. When the data scanned by OlapScanner is larger than 32768 rows, BE will check the filter condition. If the filter rate of the filter condition is lower than this configuration, Doris will stop using the dynamic partition clipping condition for data filtering.
* Default value: 90

#### `doris_max_scan_key_num`

* Type: int
* Description: Used to limit the maximum number of scan keys that a scan node can split in a query request. When a conditional query request reaches the scan node, the scan node will try to split the conditions related to the key column in the query condition into multiple scan key ranges. After that, these scan key ranges will be assigned to multiple scanner threads for data scanning. A larger value usually means that more scanner threads can be used to increase the parallelism of the scanning operation. However, in high concurrency scenarios, too many threads may bring greater scheduling overhead and system load, and will slow down the query response speed. An empirical value is 50. This configuration can be configured separately at the session level. For details, please refer to the description of `max_scan_key_num` in [Variables](../../advanced/variables.md).
  - When the concurrency cannot be improved in high concurrency scenarios, try to reduce this value and observe the impact.
* Default value: 48

#### `doris_scan_range_row_count`

* Type: int32
* Description: When BE performs data scanning, it will split the same scanning range into multiple ScanRanges. This parameter represents the scan data range of each ScanRange. This parameter can limit the time that a single OlapScanner occupies the io thread.
* Default value: 524288

#### `doris_scanner_queue_size`

* Type: int32
* Description: The length of the RowBatch buffer queue between TransferThread and OlapScanner. When Doris performs data scanning, it is performed asynchronously. The Rowbatch scanned by OlapScanner will be placed in the scanner buffer queue, waiting for the upper TransferThread to take it away.
* Default value: 1024

#### `doris_scanner_row_num`

* Description: The maximum number of data rows returned by each scanning thread in a single execution
* Default value: 16384

#### `doris_scanner_row_bytes`

* Description: single read execute fragment row bytes
    - Note: If there are too many columns in the table, you can adjust this config if you encounter a `select *` stuck
* Default value: 10485760

#### `doris_scanner_thread_pool_queue_size`

* Type: int32
* Description: The queue length of the Scanner thread pool. In Doris' scanning tasks, each Scanner will be submitted as a thread task to the thread pool waiting to be scheduled, and after the number of submitted tasks exceeds the length of the thread pool queue, subsequent submitted tasks will be blocked until there is a empty slot in the queue.
* Default value: 102400

#### `doris_scanner_thread_pool_thread_num`

* Type: int32
* Description: The number of threads in the Scanner thread pool. In Doris' scanning tasks, each Scanner will be submitted as a thread task to the thread pool to be scheduled. This parameter determines the size of the Scanner thread pool.
* Default value: 48

#### `doris_max_remote_scanner_thread_pool_thread_num`

* Type: int32
* Description: Max thread number of Remote scanner thread pool. Remote scanner thread pool is used for scan task of all external data sources.
* Default: 512

#### `enable_prefetch`

* Type: bool
* Description: When using PartitionedHashTable for aggregation and join calculations, whether to perform HashBucket prefetch. Recommended to be set to true
* Default value: true

#### `enable_quadratic_probing`

* Type: bool
* Description: When a Hash conflict occurs when using PartitionedHashTable, enable to use the square detection method to resolve the Hash conflict. If the value is false, linear detection is used to resolve the Hash conflict. For the square detection method, please refer to: [quadratic_probing](https://en.wikipedia.org/wiki/Quadratic_probing)
* Default value: true

#### `exchg_node_buffer_size_bytes`

* Type: int32
* Description: The size of the Buffer queue of the ExchangeNode node, in bytes. After the amount of data sent from the Sender side is larger than the Buffer size of ExchangeNode, subsequent data sent will block until the Buffer frees up space for writing.
* Default value: 10485760

#### `max_pushdown_conditions_per_column`

* Type: int
* Description: Used to limit the maximum number of conditions that can be pushed down to the storage engine for a single column in a query request. During the execution of the query plan, the filter conditions on some columns can be pushed down to the storage engine, so that the index information in the storage engine can be used for data filtering, reducing the amount of data that needs to be scanned by the query. Such as equivalent conditions, conditions in IN predicates, etc. In most cases, this parameter only affects queries containing IN predicates. Such as `WHERE colA IN (1,2,3,4, ...)`. A larger number means that more conditions in the IN predicate can be pushed to the storage engine, but too many conditions may cause an increase in random reads, and in some cases may reduce query efficiency. This configuration can be individually configured for session level. For details, please refer to the description of `max_pushdown_conditions_per_column` in [Variables](../../advanced/variables.md).
* Default value: 1024

* Example
  - The table structure is' id INT, col2 INT, col3 varchar (32),... '.
  - The query request is'WHERE id IN (v1, v2, v3, ...)
#### `max_send_batch_parallelism_per_job`

* Type: int
* Description: Max send batch parallelism for OlapTableSink. The value set by the user for `send_batch_parallelism` is not allowed to exceed `max_send_batch_parallelism_per_job`, if exceed, the value of `send_batch_parallelism` would be `max_send_batch_parallelism_per_job`.
* Default value: 5

#### `doris_scan_range_max_mb`

* Type: int32
* Description: The maximum amount of data read by each OlapScanner.
* Default value: 1024

### compaction

#### `disable_auto_compaction`

* Type: bool
* Description: Whether disable automatic compaction task
  - Generally it needs to be turned off. When you want to manually operate the compaction task in the debugging or test environment, you can turn on the configuration.
* Default value: false

#### `enable_vertical_compaction`

* Type: bool
* Description: Whether to enable vertical compaction
* Default value: true

#### `vertical_compaction_num_columns_per_group`

* Type: int32
* Description: In vertical compaction, column number for every group
* Default value: 5

#### `vertical_compaction_max_row_source_memory_mb`

* Type: int32
* Description: In vertical compaction, max memory usage for row_source_buffer,The unit is MB.
* Default value: 200

#### `vertical_compaction_max_segment_size`

* Type: int32
* Description: In vertical compaction, max dest segment file size, The unit is m bytes.
* Default value: 268435456

#### `enable_ordered_data_compaction`

* Type: bool
* Description: Whether to enable ordered data compaction
* Default value: true

#### `ordered_data_compaction_min_segment_size`

* Type: int32
* Description: In ordered data compaction, min segment size for input rowset, The unit is m bytes.
* Default value: 10485760

#### `max_base_compaction_threads`

* Type: int32
* Description: The maximum of thread number in base compaction thread pool.
* Default value: 4

#### `generate_compaction_tasks_interval_ms`

* Description: Minimal interval (ms) to generate compaction tasks
* Default value: 10 (ms)

#### `base_compaction_min_rowset_num`

* Description: One of the triggering conditions of BaseCompaction: The limit of the number of Cumulative files to be reached. After reaching this limit, BaseCompaction will be triggered
* Default value: 5

#### `base_compaction_min_data_ratio`

* Description: One of the trigger conditions of BaseCompaction: Cumulative file size reaches the proportion of Base file
* Default value: 0.3 (30%)

#### `total_permits_for_compaction_score`

* Type: int64
* Description: The upper limit of "permits" held by all compaction tasks. This config can be set to limit memory consumption for compaction.
* Default value: 10000
* Dynamically modifiable: Yes

#### `compaction_promotion_size_mbytes`

* Type: int64
* Description: The total disk size of the output rowset of cumulative compaction exceeds this configuration size, and the rowset will be used for base compaction. The unit is m bytes.
  - Generally, if the configuration is less than 2G, in order to prevent the cumulative compression time from being too long, resulting in the version backlog.
* Default value: 1024

#### `compaction_promotion_ratio`

* Type: double
* Description: When the total disk size of the cumulative compaction output rowset exceeds the configuration ratio of the base version rowset, the rowset will be used for base compaction.
  - Generally, it is recommended that the configuration should not be higher than 0.1 and lower than 0.02.
* Default value: 0.05

#### `compaction_promotion_min_size_mbytes`

* Type: int64
* Description: If the total disk size of the output rowset of the cumulative compaction is lower than this configuration size, the rowset will not undergo base compaction and is still in the cumulative compaction process. The unit is m bytes.
  - Generally, the configuration is within 512m. If the configuration is too large, the size of the early base version is too small, and base compaction has not been performed.
* Default value: 128

#### `compaction_min_size_mbytes`

* Type: int64
* Description: When the cumulative compaction is merged, the selected rowsets to be merged have a larger disk size than this configuration, then they are divided and merged according to the level policy. When it is smaller than this configuration, merge directly. The unit is m bytes.
  - Generally, the configuration is within 128m. Over configuration will cause more cumulative compaction write amplification.
* Default value: 64

#### `default_rowset_type`

* Type: string
* Description: Identifies the storage format selected by BE by default. The configurable parameters are: "**ALPHA**", "**BETA**". Mainly play the following two roles
  - When the storage_format of the table is set to Default, select the storage format of BE through this configuration.
  - Select the storage format of when BE performing Compaction
* Default value: BETA

#### `cumulative_compaction_min_deltas`

* Description: Cumulative compaction strategy: the minimum number of incremental files
* Default value: 5

#### `cumulative_compaction_max_deltas`

* Description: Cumulative compaction strategy: the maximum number of incremental files
* Default value: 1000

#### `base_compaction_trace_threshold`

* Type: int32
* Description: Threshold to logging base compaction's trace information, in seconds
* Default value: 10

Base compaction is a long time cost background task, this configuration is the threshold to logging trace information. Trace information in log file looks like:

```
W0610 11:26:33.804431 56452 storage_engine.cpp:552] execute base compaction cost 0.00319222
BaseCompaction:546859:
  - filtered_rows: 0
   - input_row_num: 10
   - input_rowsets_count: 10
   - input_rowsets_data_size: 2.17 KB
   - input_segments_num: 10
   - merge_rowsets_latency: 100000.510ms
   - merged_rows: 0
   - output_row_num: 10
   - output_rowset_data_size: 224.00 B
   - output_segments_num: 1
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
```

#### `cumulative_compaction_trace_threshold`

* Type: int32
* Description: Threshold to logging cumulative compaction's trace information, in seconds
  - Similar to `base_compaction_trace_threshold`.
* Default value: 2

#### `compaction_task_num_per_disk`

* Type: int32
* Description: The number of compaction tasks which execute in parallel for a disk(HDD).
* Default value: 4

#### `compaction_task_num_per_fast_disk`

* Type: int32
* Description: The number of compaction tasks which execute in parallel for a fast disk(SSD).
* Default value: 8

#### `cumulative_compaction_rounds_for_each_base_compaction_round`

* Type: int32
* Description: How many rounds of cumulative compaction for each round of base compaction when compaction tasks generation.
* Default value: 9

#### `cumulative_compaction_policy`

* Type: string
* Description: Configure the merge strategy in the cumulative compression phase. Currently, two merge strategies are implemented, num_based and size_based
  - For details, "ordinary" is the initial version of the cumulative compression consolidation policy. After a cumulative compression, the base compression process is directly performed. size_The general policy is the optimized version of the ordinary policy. Version merging can only be performed when the disk volume of the rowset is the same order of magnitude. After merging, qualified rowsets are promoted to the base compaction stage. In the case of a large number of small batch imports, it can reduce the write magnification of base compact, balance the read magnification and space magnification, and reduce the data of file versions.
* Default value: size_based

#### `max_cumu_compaction_threads`

* Type: int32
* Description: The maximum of thread number in cumulative compaction thread pool.
* Default value: 10

#### `enable_segcompaction`

* Type: bool
* Description: Enable to use segment compaction during loading to avoid -238 error
* Default value: false

#### `segcompaction_batch_size`

* Type: int32
* Description: Max number of segments allowed in a single segment compaction task.
* Default value: 10

#### `segcompaction_candidate_max_rows`

* Type: int32
* Description: Max row count allowed in a single source segment, bigger segments will be skipped.
* Default value: 1048576

#### `segcompaction_candidate_max_bytes`

* Type: int64
* Description: Max file size allowed in a single source segment, bigger segments will be skipped.
* Default value: 104857600

#### `segcompaction_task_max_rows`

* Type: int32
* Description: Max total row count allowed in a single segcompaction task.
* Default value: 1572864

#### `segcompaction_task_max_bytes`

* Type: int64
* Description: Max total file size allowed in a single segcompaction task.
* Default value: 157286400

#### `segcompaction_num_threads`

* Type: int32
* Description: Global segcompaction thread pool size.
* Default value: 5

#### `disable_compaction_trace_log`

* Type: bool
* Description: disable the trace log of compaction
  - If set to true, the `cumulative_compaction_trace_threshold` and `base_compaction_trace_threshold` won't work and log is disabled.
* Default value: true

#### `pick_rowset_to_compact_interval_sec`

* Type: int64
* Description: select the time interval in seconds for rowset to be compacted.
* Default value: 86400

#### `max_single_replica_compaction_threads`

* Type: int32
* Description: The maximum of thread number in single replica compaction thread pool.
* Default value: 10

#### `update_replica_infos_interval_seconds`

* Description: Minimal interval (s) to update peer replica infos
* Default value: 60 (s)


### Load

#### `enable_stream_load_record`

* Type: bool
* Description:Whether to enable stream load record function, the default is false.
* Default value: false

#### `load_data_reserve_hours`

* Description: Used for mini load. The mini load data file will be deleted after this time
* Default value: 4 (h)

#### `push_worker_count_high_priority`

* Description: Import the number of threads for processing HIGH priority tasks
* Default value: 3

#### `push_worker_count_normal_priority`

* Description: Import the number of threads for processing NORMAL priority tasks
* Default value: 3

#### `enable_single_replica_load`

* Description: Whether to enable the single-copy data import function
* Default value: true

#### `load_error_log_reserve_hours`

* Description: The load error log will be deleted after this time
* Default value: 48 (h)

#### `load_error_log_limit_bytes`

* Description: The loading error logs larger than this value will be truncated
* Default value: 209715200 (byte)

#### `load_process_max_memory_limit_percent`

* Description: The percentage of the upper memory limit occupied by all imported threads on a single node, the default is 50%
  - Set these default values very large, because we don't want to affect load performance when users upgrade Doris. If necessary, the user should set these configurations correctly
* Default value: 50 (%)

#### `load_process_soft_mem_limit_percent`

* Description: The soft limit refers to the proportion of the load memory limit of a single node. For example, the load memory limit for all load tasks is 20GB, and the soft limit defaults to 50% of this value, that is, 10GB. When the load memory usage exceeds the soft limit, the job with the largest memory consumption will be selected to be flushed to release the memory space, the default is 50%
* Default value: 50 (%)

#### `routine_load_thread_pool_size`

* Description: The thread pool size of the routine load task. This should be greater than the FE configuration'max_concurrent_task_num_per_be'
* Default value: 10

#### `slave_replica_writer_rpc_timeout_sec`

* Type: int32
* Description: This configuration is mainly used to modify timeout of brpc between master replica and slave replica, used for single replica load.
* Default value: 60

#### `max_segment_num_per_rowset`

* Type: int32
* Description: Used to limit the number of segments in the newly generated rowset when importing. If the threshold is exceeded, the import will fail with error -238. Too many segments will cause compaction to take up a lot of memory and cause OOM errors.
* Default value: 200

#### `high_priority_flush_thread_num_per_store`

* Type: int32
* Description: The number of flush threads per store path allocated for the high priority import task.
* Default value: 1

#### `routine_load_consumer_pool_size`

* Type: int32
* Description: The number of caches for the data consumer used by the routine load.
* Default value: 10

#### `multi_table_batch_plan_threshold`

* Type: int32
* Description: For single-stream-multi-table load. When receive a batch of messages from kafka, if the size of batch is more than this threshold, we will request plans for all related tables.
* Default value: 200

#### `single_replica_load_download_num_workers`

* Type: int32
* Description:This configuration is mainly used to modify the number of http worker threads for segment download, used for single replica load. When the load concurrency increases, you can adjust this parameter to ensure that the Slave replica synchronizes data files from the Master replica timely. If needed, `webserver_num_workers` should also be increased for better IO performance.
* Default value: 64

#### `load_task_high_priority_threshold_second`

* Type: int32
* Description: When the timeout of an import task is less than this threshold, Doris will consider it to be a high priority task. High priority tasks use a separate pool of flush threads.
* Default: 120

#### `min_load_rpc_timeout_ms`

* Type: int32
* Description: The minimum timeout for each rpc in the load job.
* Default: 20

#### `kafka_api_version_request`

* Type: bool
* Description: If the dependent Kafka version is lower than 0.10.0.0, this value should be set to false.
* Default: true

#### `kafka_broker_version_fallback`

* Description: If the dependent Kafka version is lower than 0.10.0.0, the value set by the fallback version kafka_broker_version_fallback will be used if the value of kafka_api_version_request is set to false, and the valid values are: 0.9.0.x, 0.8.x.y.
* Default: 0.10.0

#### `max_consumer_num_per_group`

* Description: The maximum number of consumers in a data consumer group, used for routine load
* Default: 3

#### `streaming_load_max_mb`

* Type: int64
* Description: Used to limit the maximum amount of csv data allowed in one Stream load.
  - Stream Load is generally suitable for loading data less than a few GB, not suitable for loading too large data.
* Default value: 10240 (MB)
* Dynamically modifiable: Yes

#### `streaming_load_json_max_mb`

* Type: int64
* Description: it is used to limit the maximum amount of json data allowed in one Stream load. The unit is MB.
  - Some data formats, such as JSON, cannot be split. Doris must read all the data into the memory before parsing can begin. Therefore, this value is used to limit the maximum amount of data that can be loaded in a single Stream load.
* Default value: 100
* Dynamically modifiable: Yes

#### `olap_table_sink_send_interval_microseconds`

* Description: While loading data, there's a polling thread keep sending data to corresponding BE from Coordinator's sink node. This thread will check whether there's data to send every `olap_table_sink_send_interval_microseconds` microseconds.
* Default value: 1000

#### `olap_table_sink_send_interval_auto_partition_factor`

* Description: If we load data to a table which enabled auto partition. the interval of `olap_table_sink_send_interval_microseconds` is too slow. In that case the real interval will multiply this factor.
* Default value: 0.001

### Thread

#### `delete_worker_count`

* Description: Number of threads performing data deletion tasks
* Default value: 3

#### `clear_transaction_task_worker_count`

* Description: Number of threads used to clean up transactions
* Default value: 1

#### `clone_worker_count`

* Description: Number of threads used to perform cloning tasks
* Default value: 3

#### `be_service_threads`

* Type: int32
* Description: The number of execution threads of the thrift server service on BE which represents the number of threads that can be used to execute FE requests.
* Default value: 64

#### `download_worker_count`

* Description: The number of download threads.
* Default value: 1

#### `drop_tablet_worker_count`

* Description: Number of threads to delete tablet
* Default value: 3

#### `flush_thread_num_per_store`

* Description: The number of threads used to refresh the memory table per store
* Default value: 2

#### `num_threads_per_core`

* Description: Control the number of threads that each core runs. Usually choose 2 times or 3 times the number of cores. This keeps the core busy without causing excessive jitter
* Default value: 3

#### `num_threads_per_disk`

* Description: The maximum number of threads per disk is also the maximum queue depth of each disk
* Default value: 0

#### `number_slave_replica_download_threads`

* Description: Number of threads for slave replica synchronize data, used for single replica load.
* Default value: 64

#### `publish_version_worker_count`

* Description: the count of thread to publish version
* Default value: 8

#### `upload_worker_count`

* Description: Maximum number of threads for uploading files
* Default value: 1

#### `webserver_num_workers`

* Description: Webserver default number of worker threads
* Default value: 48

#### `send_batch_thread_pool_thread_num`

* Type: int32
* Description: The number of threads in the SendBatch thread pool. In NodeChannels' sending data tasks, the SendBatch operation of each NodeChannel will be submitted as a thread task to the thread pool to be scheduled. This parameter determines the size of the SendBatch thread pool.
* Default value: 64

#### `send_batch_thread_pool_queue_size`

* Type: int32
* Description: The queue length of the SendBatch thread pool. In NodeChannels' sending data tasks,  the SendBatch operation of each NodeChannel will be submitted as a thread task to the thread pool waiting to be scheduled, and after the number of submitted tasks exceeds the length of the thread pool queue, subsequent submitted tasks will be blocked until there is a empty slot in the queue.
* Default value: 102400

#### `make_snapshot_worker_count`

* Description: Number of threads making snapshots
* Default value: 5

#### `release_snapshot_worker_count`

* Description: Number of threads releasing snapshots
* Default value: 5

### Memory

#### `disable_mem_pools`

* Type: bool
* Description: Whether to disable the memory cache pool.
* Default value: false

#### `buffer_pool_clean_pages_limit`

* Description: Clean up pages that may be saved by the buffer pool
* Default value: 50%

#### `buffer_pool_limit`

* Type: string
* Description: The largest allocatable memory of the buffer pool
  - The maximum amount of memory available in the BE buffer pool. The buffer pool is a new memory management structure of BE, which manages the memory by the buffer page and enables spill data to disk. The memory for all concurrent queries will be allocated from the buffer pool. The current buffer pool only works on **AggregationNode** and **ExchangeNode**.
* Default value: 20%

#### `chunk_reserved_bytes_limit`

* Description: The reserved bytes limit of Chunk Allocator, usually set as a percentage of mem_limit. defaults to bytes if no unit is given, the number of bytes must be a multiple of 2. must larger than 0. and if larger than physical memory size, it will be set to physical memory size. increase this variable can improve performance, but will acquire more free memory which can not be used by other modules.
* Default value: 20%

#### `madvise_huge_pages`

* Type: bool
* Description: Whether to use linux memory huge pages.
* Default value: false

#### `max_memory_sink_batch_count`

* Description: The maximum external scan cache batch count, which means that the cache max_memory_cache_batch_count * batch_size row, the default is 20, and the default value of batch_size is 1024, which means that 20 * 1024 rows will be cached
* Default value: 20

#### `memory_max_alignment`

* Description: Maximum alignment memory
* Default value: 16

#### `mmap_buffers`

* Description: Whether to use mmap to allocate memory
* Default value: false

#### `memtable_mem_tracker_refresh_interval_ms`

* Description: Interval in milliseconds between memtable flush mgr refresh iterations
* Default value: 100

#### `zone_map_row_num_threshold`

* Type: int32
* Description: If the number of rows in a page is less than this value, no zonemap will be created to reduce data expansion
* Default value: 20

#### `enable_tcmalloc_hook`

* Type: bool
* Description: Whether Hook TCmalloc new/delete, currently consume/release tls mem tracker in Hook.
* Default value: true

#### `memory_mode`

* Type: string
* Description: Control gc of tcmalloc, in performance mode Doris releases memory of tcmalloc cache when usage >= 90% * mem_limit, otherwise, doris releases memory of tcmalloc cache when usage >= 50% * mem_limit;
* Default value: performance

#### `max_sys_mem_available_low_water_mark_bytes`

* Type: int64
* Description: The maximum low water mark of the system `/proc/meminfo/MemAvailable`, Unit byte, default 1.6G, actual low water mark=min(1.6G, MemTotal * 10%), avoid wasting too much memory on machines with large memory larger than 16G. Turn up max. On machines with more than 16G memory, more memory buffers will be reserved for Full GC. Turn down max. will use as much memory as possible.
* Default value: 1717986918

#### `memory_limitation_per_thread_for_schema_change_bytes`

* Description: Maximum memory allowed for a single schema change task
* Default value: 2147483648 (2GB)

#### `mem_tracker_consume_min_size_bytes`

* Type: int32
* Description: The minimum length of TCMalloc Hook when consume/release MemTracker. Consume size smaller than this value will continue to accumulate to avoid frequent calls to consume/release of MemTracker. Decreasing this value will increase the frequency of consume/release. Increasing this value will cause MemTracker statistics to be inaccurate. Theoretically, the statistical value of a MemTracker differs from the true value = ( mem_tracker_consume_min_size_bytes * the number of BE threads where the MemTracker is located).
* Default value: 1,048,576

#### `cache_clean_interval`

* Description: File handle cache cleaning interval, used to clean up file handles that have not been used for a long time.Also the clean interval of Segment Cache.
* Default value: 1800 (s)

#### `min_buffer_size`

* Description: Minimum read buffer size
* Default value: 1024 (byte)

#### `write_buffer_size`

* Description: The size of the buffer before flashing
  - Imported data is first written to a memory block on the BE, and only written back to disk when this memory block reaches the threshold. The default size is 100MB. too small a threshold may result in a large number of small files on the BE. This threshold can be increased to reduce the number of files. However, too large a threshold may cause RPC timeouts
* Default value: 104,857,600

#### `remote_storage_read_buffer_mb`

* Type: int32
* Description: The cache size used when reading files on hdfs or object storage.
  - Increasing this value can reduce the number of calls to read remote data, but it will increase memory overhead.
* Default value: 16 (MB)

#### `file_cache_type`

* Type: string
* Description: Type of cache file.`whole_file_cache`: download the entire segment file, `sub_file_cache`: the segment file is divided into multiple files by size. if set "", no cache, please set this parameter when caching is required.
* Default value: ""

#### `file_cache_alive_time_sec`

* Type: int64
* Description: Save time of cache file
* Default value: 604800 (1 week)

#### `file_cache_max_size_per_disk`

* Type: int64
* Description: The cache occupies the disk size. Once this setting is exceeded, the cache that has not been accessed for the longest time will be deleted. If it is 0, the size is not limited. unit is bytes.
* Default value: 0

#### `max_sub_cache_file_size`

* Type: int64
* Description: Cache files using sub_ file_ The maximum size of the split file during cache
* Default value: 104857600 (100MB)

#### `download_cache_thread_pool_thread_num`

* Type: int32
* Description: The number of threads in the DownloadCache thread pool. In the download cache task of FileCache, the download cache operation will be submitted to the thread pool as a thread task and wait to be scheduled. This parameter determines the size of the DownloadCache thread pool.
* Default value: 48


#### `download_cache_thread_pool_queue_size`

* Type: int32
* Description: The number of threads in the DownloadCache thread pool. In the download cache task of FileCache, the download cache operation will be submitted to the thread pool as a thread task and wait to be scheduled. After the number of submitted tasks exceeds the length of the thread pool queue, subsequent submitted tasks will be blocked until there is a empty slot in the queue.
* Default value: 102400

#### `generate_cache_cleaner_task_interval_sec`

* Type：int64
* Description：Cleaning interval of cache files, in seconds
* Default：43200（12 hours）

#### `path_gc_check`

* Type：bool
* Description：Whether to enable the recycle scan data thread check
* Default：true

#### `path_gc_check_interval_second`

* Description：Recycle scan data thread check interval
* Default：86400 (s)

#### `path_gc_check_step`

* Default：1000

#### `path_gc_check_step_interval_ms`

* Default：10 (ms)

#### `path_scan_interval_second`

* Default：86400

#### `scan_context_gc_interval_min`

* Description：This configuration is used for the context gc thread scheduling cycle. Note: The unit is minutes, and the default is 5 minutes
* Default：5

### Storage

#### `default_num_rows_per_column_file_block`

* Type: int32
* Description: Configure how many rows of data are contained in a single RowBlock.
* Default value: 1024

#### `disable_storage_page_cache`

* Type: bool
* Description: Disable to use page cache for index caching, this configuration only takes effect in BETA storage format, usually it is recommended to false
* Default value: false

#### `disk_stat_monitor_interval`

* Description: Disk status check interval
* Default value: 5（s）

#### `max_free_io_buffers`

* Description: For each io buffer size, the maximum number of buffers that IoMgr will reserve ranges from 1024B to 8MB buffers, up to about 2GB buffers.
* Default value: 128

#### `max_garbage_sweep_interval`

* Description: The maximum interval for disk garbage cleaning
* Default value: 3600 (s)

#### `max_percentage_of_error_disk`

* Type: int32
* Description: The storage engine allows the percentage of damaged hard disks to exist. After the damaged hard disk exceeds the changed ratio, BE will automatically exit.
* Default value: 0

#### `read_size`

* Description: The read size is the read size sent to the os. There is a trade-off between latency and the whole process, getting to keep the disk busy but not introducing seeks. For 8 MB reads, random io and sequential io have similar performance.
* Default value: 8388608

#### `min_garbage_sweep_interval`

* Description: The minimum interval between disk garbage cleaning
* Default value: 180 (s)

#### `pprof_profile_dir`

* Description: pprof profile save directory
* Default value: ${DORIS_HOME}/log

#### `small_file_dir`

* Description: Save files downloaded by SmallFileMgr
* Default value: ${DORIS_HOME}/lib/small_file/

#### `user_function_dir`

* Description: udf function directory
* Default value: ${DORIS_HOME}/lib/udf

#### `storage_flood_stage_left_capacity_bytes`

* Description: The min bytes that should be left of a data dir.
* Default value: 1073741824

#### `storage_flood_stage_usage_percent`

* Description: The storage_flood_stage_usage_percent and storage_flood_stage_left_capacity_bytes configurations limit the maximum usage of the capacity of the data directory.
* Default value: 90 （90%）

#### `storage_medium_migrate_count`

* Description: the count of thread to clone
* Default value: 1

#### `storage_page_cache_limit`

* Description: Cache for storage page size
* Default value: 20%

#### `storage_page_cache_shard_size`

* Description: Shard size of StoragePageCache, the value must be power of two. It's recommended to set it to a value close to the number of BE cores in order to reduce lock contentions.
* Default value: 16

#### `index_page_cache_percentage`

* Type: int32
* Description: Index page cache as a percentage of total storage page cache, value range is [0, 100]
* Default value: 10

#### `segment_cache_capacity`
* Type: int32
* Description: Max number of segment cache (the key is rowset id) entries. -1 is for backward compatibility as fd_number * 2/5.
* Default value: -1

#### `storage_strict_check_incompatible_old_format`

* Type: bool
* Description: Used to check incompatible old format strictly
* Default value: true
* Dynamically modify: false

#### `sync_tablet_meta`

* Description: Whether the storage engine opens sync and keeps it to the disk
* Default value: false

#### `pending_data_expire_time_sec`

* Description: The maximum duration of unvalidated data retained by the storage engine
* Default value: 1800 (s)

#### `ignore_rowset_stale_unconsistent_delete`

* Type: boolean
* Description:It is used to decide whether to delete the outdated merged rowset if it cannot form a consistent version path.
  - The merged expired rowset version path will be deleted after half an hour. In abnormal situations, deleting these versions will result in the problem that the consistent path of the query cannot be constructed. When the configuration is false, the program check is strict and the program will directly report an error and exit.When configured as true, the program will run normally and ignore this error. In general, ignoring this error will not affect the query, only when the merged version is dispatched by fe, -230 error will appear.
* Default value: false

#### `create_tablet_worker_count`

* Description: Number of worker threads for BE to create a tablet
* Default value: 3

#### `check_consistency_worker_count`

* Description: The number of worker threads to calculate the checksum of the tablet
* Default value: 1

#### `max_tablet_version_num`

* Type: int
* Description: Limit the number of versions of a single tablet. It is used to prevent a large number of version accumulation problems caused by too frequent import or untimely compaction. When the limit is exceeded, the import task will be rejected.
* Default value: 500

#### `number_tablet_writer_threads`

* Description: Number of tablet write threads
* Default value: 16

#### `tablet_map_shard_size`

* Description: tablet_map_lock fragment size, the value is 2^n, n=0,1,2,3,4, this is for better tablet management
* Default value: 4

#### `tablet_meta_checkpoint_min_interval_secs`

* Description: TabletMeta Checkpoint线程轮询的时间间隔
* Default value: 600 (s)

#### `tablet_meta_checkpoint_min_new_rowsets_num`

* Description: The minimum number of Rowsets for storing TabletMeta Checkpoints
* Default value: 10

#### `tablet_stat_cache_update_interval_second`

* Description: Update interval of tablet state cache
* Default value:300 (s)

#### `tablet_rowset_stale_sweep_time_sec`

* Type: int64
* Description: It is used to control the expiration time of cleaning up the merged rowset version. When the current time now() minus the max created rowset‘s create time in a version path is greater than tablet_rowset_stale_sweep_time_sec, the current path is cleaned up and these merged rowsets are deleted, the unit is second.
  - When writing is too frequent, Fe may not be able to query the merged version, resulting in a query -230 error. This problem can be avoided by increasing this parameter.
* Default value: 300

#### `tablet_writer_open_rpc_timeout_sec`

* Description: Update interval of tablet state cache
    - The RPC timeout for sending a Batch (1024 lines) during import. The default is 60 seconds. Since this RPC may involve writing multiple batches of memory, the RPC timeout may be caused by writing batches, so this timeout can be adjusted to reduce timeout errors (such as send batch fail errors). Also, if you increase the write_buffer_size configuration, you need to increase this parameter as well.
* Default value: 60

#### `tablet_writer_ignore_eovercrowded`

* Type: bool
* Description: Used to ignore brpc error '[E1011]The server is overcrowded' when writing data.
* Default value: false

#### `streaming_load_rpc_max_alive_time_sec`

* Description: The lifetime of TabletsChannel. If the channel does not receive any data at this time, the channel will be deleted.
* Default value: 1200

#### `alter_tablet_worker_count`

* Description: The number of threads making schema changes
* Default value: 3

#### `alter_index_worker_count`

* Description: The number of threads making index change
* Default value: 3

#### `ignore_load_tablet_failure`

* Type: bool
* Description: It is used to decide whether to ignore errors and continue to start be in case of tablet loading failure
* Default value: false

When BE starts, a separate thread will be started for each data directory to load the meta information of the tablet header. In the default configuration, if a data directory fails to load a tablet, the startup process will terminate. At the same time, it will be displayed in the ` be The following error message is seen in the INFO ` log:

```
load tablets from header failed, failed tablets size: xxx, path=xxx
```

Indicates how many tablets failed to load in the data directory. At the same time, the log will also contain specific information about the tablets that failed to load. At this time, manual intervention is required to troubleshoot the cause of the error. After troubleshooting, there are usually two ways to restore:
  - The tablet information cannot be repaired. If the other copies are normal, you can delete the wrong tablet with the `meta_tool` tool.
  - Set `ignore_load_tablet_failure` to true, BE will ignore these faulty tablets and start normally

#### `report_disk_state_interval_seconds`

* Description: The interval time for the agent to report the disk status to FE
* Default value: 60 (s)

#### `result_buffer_cancelled_interval_time`

* Description: Result buffer cancellation time
* Default value: 300 (s)

#### `snapshot_expire_time_sec`

* Description: Snapshot file cleaning interval.
* Default value:172800 (48 hours)

#### `compress_rowbatches`

* Type: bool
* Description: enable to use Snappy compression algorithm for data compression when serializing RowBatch
* Default value: true

<version since="1.2">

#### `jvm_max_heap_size`

* Type: string
* Description: The maximum size of JVM heap memory used by BE, which is the `-Xmx` parameter of JVM
* Default value: 1024M

</version>

### Log

#### `sys_log_dir`

* Type: string
* Description: Storage directory of BE log data
* Default value: ${DORIS_HOME}/log

#### `sys_log_level`

* Description: Log Level: INFO < WARNING < ERROR < FATAL
* Default value: INFO

#### `sys_log_roll_mode`

* Description: The size of the log split, one log file is split every 1G
* Default value: SIZE-MB-1024

#### `sys_log_roll_num`

* Description: Number of log files kept
* Default value: 10

#### `sys_log_verbose_level`

* Description: Log display level, used to control the log output at the beginning of VLOG in the code
* Default value: 10

#### `sys_log_verbose_modules`

* Description: Log printing module, writing olap will only print the log under the olap module
* Default value: empty

#### `aws_log_level`

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
* Default value: 3

#### `log_buffer_level`

* Description: The log flushing strategy is kept in memory by default
* Default value: empty

### Else

#### `report_tablet_interval_seconds`

* Description: The interval time for the agent to report the olap table to the FE
* Default value: 60 (s)

#### `report_task_interval_seconds`

* Description: The interval time for the agent to report the task signature to FE
* Default value: 10 (s)

#### `periodic_counter_update_period_ms`

* Description: Update rate counter and sampling counter cycle
* Default value: 500 (ms)

#### `enable_metric_calculator`

* Description: If set to true, the metric calculator will run to collect BE-related indicator information, if set to false, it will not run
* Default value: true

#### `enable_system_metrics`

* Description: User control to turn on and off system indicators.
* Default value: true

#### `enable_token_check`

* Description: Used for forward compatibility, will be removed later.
* Default value: true

#### `max_runnings_transactions_per_txn_map`

* Description: Max number of txns for every txn_partition_map in txn manager, this is a self protection to avoid too many txns saving in manager
* Default value: 2000

#### `max_download_speed_kbps`

* Description: Maximum download speed limit
* Default value: 50000 （kb/s）

#### `download_low_speed_time`

* Description: Download time limit
* Default value: 300 (s)

#### `download_low_speed_limit_kbps`

* Description: Minimum download speed
* Default value: 50 (KB/s)

#### `doris_cgroups`

* Description: Cgroups assigned to doris
* Default value: empty

#### `priority_queue_remaining_tasks_increased_frequency`

* Description: the increased frequency of priority for remaining tasks in BlockingPriorityQueue
* Default value: 512

<version since="1.2">

#### `jdbc_drivers_dir`

* Description: Default dirs to put jdbc drivers.
* Default value: `${DORIS_HOME}/jdbc_drivers`

#### `enable_simdjson_reader`

* Description: Whether enable simdjson to parse json while stream load
* Default value: true

</version>

#### `enable_query_memory_overcommit`

* Description: If true, when the process does not exceed the soft mem limit, the query memory will not be limited; when the process memory exceeds the soft mem limit, the query with the largest ratio between the currently used memory and the exec_mem_limit will be canceled. If false, cancel query when the memory used exceeds exec_mem_limit.
* Default value: true

#### `user_files_secure_path`

* Description: The storage directory for files queried by `local` table valued functions.
* Default value: `${DORIS_HOME}`

#### `brpc_streaming_client_batch_bytes`

* Description: The batch size for sending data by brpc streaming client
* Default value: 262144

#### `grace_shutdown_wait_seconds`

* Description: In cloud native deployment scenario, BE will be add to cluster and remove from cluster very frequently. User's query will fail if there is a fragment is running on the shuting down BE. Users could use stop_be.sh --grace, then BE will wait all running queries to stop to avoiding running query failure, but if the waiting time exceed the limit, then be will exit directly. During this period, FE will not send any queries to BE and waiting for all running queries to stop.
* Default value: 120

#### `enable_java_support`

* Description: BE Whether to enable the use of java-jni. When enabled, mutual calls between c++ and java are allowed. Currently supports hudi, java-udf, jdbc, max-compute, paimon, preload, avro
* Default value: true

#### `group_commit_wal_path`

* The `WAL` directory of group commit.
* Default: A directory named `wal` is created under each directory of the `storage_root_path`. Configuration examples:
  ```
  group_commit_wal_path=/data1/storage/wal;/data2/storage/wal;/data3/storage/wal
  ```

#### `group_commit_memory_rows_for_max_filter_ratio`

* Description: The `max_filter_ratio` limit can only work if the total rows of `group commit` is less than this value. See [Group Commit](../../data-operate/import/import-way/group-commit-manual.md) for more details
* Default: 10000
