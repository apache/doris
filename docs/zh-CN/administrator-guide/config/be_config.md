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

BE 的配置文件 `be.conf` 通常存放在 BE 部署路径的 `conf/` 目录下。 而在 0.14 版本中会引入另一个配置文件 `be_custom.conf`。该配置文件用于记录用户在运行是动态配置并持久化的配置项。

BE 进程启动后，会先读取 `be.conf` 中的配置项，之后再读取 `be_custom.conf` 中的配置项。`be_custom.conf` 中的配置项会覆盖 `be.conf` 中相同的配置项。

## 查看配置项

用户可以通过访问 BE 的 Web 页面查看当前配置项：

`http://be_host:be_webserver_port/varz`

## 设置配置项

BE 的配置项有两种方式进行配置：

1. 静态配置

	在 `conf/be.conf` 文件中添加和设置配置项。`be.conf` 中的配置项会在 BE 进行启动时被读取。没有在 `be.conf` 中的配置项将使用默认值。

2. 动态配置

	BE 启动后，可以通过一下命令动态设置配置项。

	```
	curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}'
	```

	在 0.13 版本及之前，通过该方式修改的配置项将在 BE 进程重启后失效。在 0.14 及之后版本中，可以通过以下命令持久化修改后的配置。修改后的配置项存储在 `be_custom.conf` 文件中。
	
	```
	curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}&persist=true'
	```

## 应用举例

1. 静态方式修改 `max_compaction_concurrency`

	通过在 `be.conf` 文件中添加：

	```max_compaction_concurrency=5```

	之后重启 BE 进程以生效该配置。

2. 动态方式修改 `streaming_load_max_mb`

	BE 启动后，通过下面命令动态设置配置项 `streaming_load_max_mb`:

	```curl -X POST http://{be_ip}:{be_http_port}/api/update_config?streaming_load_max_mb=1024```

	返回值如下，则说明设置成功。

	```
	{
	    "status": "OK",
	    "msg": ""
	}
	```

	BE 重启后该配置将失效。如果想持久化修改结果，使用如下命令：
	
	```
	curl -X POST http://{be_ip}:{be_http_port}/api/update_config?streaming_load_max_mb=1024\&persist=true
	```

## 配置项列表

### `alter_tablet_worker_count`

### `base_compaction_check_interval_seconds`

### `base_compaction_interval_seconds_since_last_operation`

### `base_compaction_num_cumulative_deltas`

### `base_compaction_num_threads_per_disk`

### `base_compaction_write_mbytes_per_sec`

### `base_cumulative_delta_ratio`

### `base_compaction_trace_threshold`

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

### `be_port`

* 类型：int32
* 描述：BE 上 thrift server 的端口号，用于接收来自 FE 的请求
* 默认值：9060

### `be_service_threads`
* 类型：int32
* 描述：BE 上 thrift server service的执行线程数，代表可以用于执行FE请求的线程数。
* 默认值：64

### `brpc_max_body_size`

这个配置主要用来修改 brpc 的参数 `max_body_size`。

有时查询失败，在 BE 日志中会出现 `body_size is too large` 的错误信息。这可能发生在 SQL 模式为 multi distinct + 无 group by + 超过1T 数据量的情况下。这个错误表示 brpc 的包大小超过了配置值。此时可以通过调大该配置避免这个错误。


### `brpc_socket_max_unwritten_bytes`

这个配置主要用来修改 brpc  的参数 `socket_max_unwritten_bytes`。

有时查询失败，BE 日志中会出现 `The server is overcrowded` 的错误信息，表示连接上有过多的未发送数据。当查询需要发送较大的bitmap字段时，可能会遇到该问题，此时可能通过调大该配置避免该错误。


### `brpc_num_threads`

该配置主要用来修改brpc中bthreads的数量. 该配置的默认值被设置为-1, 这意味着bthreads的数量将被设置为机器的cpu核数。

用户可以将该配置的值调大来获取更好的QPS性能。更多的信息可以参考`https://github.com/apache/incubator-brpc/blob/master/docs/cn/benchmark.md`。

### `brpc_port`

* 类型：int32
* 描述：BE 上的 brpc 的端口，用于 BE 之间通讯
* 默认值：8060

### `buffer_pool_clean_pages_limit`

### `buffer_pool_limit`

* 类型：string
* 描述：buffer pool之中最大的可分配内存
* 默认值：80G

BE缓存池最大的内存可用量，buffer pool是BE新的内存管理结构，通过buffer page来进行内存管理，并能够实现数据的落盘。并发的所有查询的内存申请都会通过buffer pool来申请。当前buffer pool仅作用在**AggregationNode**与**ExchangeNode**。

### `check_auto_compaction_interval_seconds`

* 类型：int32
* 描述：当自动执行compaction的功能关闭时，检查自动compaction开关是否被开启的时间间隔。
* 默认值：5

### `check_consistency_worker_count`

### `chunk_reserved_bytes_limit`

### `clear_transaction_task_worker_count`

### `clone_worker_count`

### `cluster_id`

* 类型：int32

* 描述：配置BE的所属于的集群id。

* 默认值：-1

该值通常由FE通过心跳向BE下发，不需要额外进行配置。当确认某BE属于某一个确定的Drois集群时，可以进行配置，同时需要修改数据目录下的cluster_id文件，使二者相同。

### `column_dictionary_key_ratio_threshold`

### `column_dictionary_key_size_threshold`

### `compaction_tablet_compaction_score_factor`

* 类型：int32
* 描述：选择tablet进行compaction时，计算 tablet score 的公式中 compaction score的权重。
* 默认值：1

### `compaction_tablet_scan_frequency_factor`

* 类型：int32
* 描述：选择tablet进行compaction时，计算 tablet score 的公式中 tablet scan frequency 的权重。
* 默认值：0

选择一个tablet执行compaction任务时，可以将tablet的scan频率作为一个选择依据，对当前最近一段时间频繁scan的tablet优先执行compaction。
tablet score可以通过以下公式计算：

tablet_score = compaction_tablet_scan_frequency_factor * tablet_scan_frequency + compaction_tablet_scan_frequency_factor * compaction_score

### `compaction_task_num_per_disk`

* 类型：int32
* 描述：每个磁盘可以并发执行的compaction任务数量。
* 默认值：2

### `compress_rowbatches`
* 类型：bool

* 描述：序列化RowBatch时是否使用Snappy压缩算法进行数据压缩

* 默认值：true

### `create_tablet_worker_count`

### `cumulative_compaction_rounds_for_each_base_compaction_round`

* 类型：int32
* 描述：Compaction任务的生产者每次连续生产多少轮cumulative compaction任务后生产一轮base compaction。
* 默认值：9

### `disable_auto_compaction`

* 类型：bool
* 描述：关闭自动执行compaction任务
* 默认值：false

一般需要为关闭状态，当调试或测试环境中想要手动操作compaction任务时，可以对该配置进行开启

### `cumulative_compaction_budgeted_bytes`

### `cumulative_compaction_check_interval_seconds`

### `cumulative_compaction_num_threads_per_disk`

### `cumulative_compaction_skip_window_seconds`

### `cumulative_compaction_trace_threshold`

* 类型：int32
* 描述：打印cumulative compaction的trace信息的阈值，单位秒
* 默认值：2

与base_compaction_trace_threshold类似。

### `cumulative_compaction_policy`

* 类型：string
* 描述：配置 cumulative compaction 阶段的合并策略，目前实现了两种合并策略，num_based和size_based
* 默认值：size_based

详细说明，ordinary，是最初版本的cumulative compaction合并策略，做一次cumulative compaction之后直接base compaction流程。size_based，通用策略是ordinary策略的优化版本，仅当rowset的磁盘体积在相同数量级时才进行版本合并。合并之后满足条件的rowset进行晋升到base compaction阶段。能够做到在大量小批量导入的情况下：降低base compact的写入放大率，并在读取放大率和空间放大率之间进行权衡，同时减少了文件版本的数据。

### `cumulative_size_based_promotion_size_mbytes`

* 类型：int64
* 描述：在size_based策略下，cumulative compaction的输出rowset总磁盘大小超过了此配置大小，该rowset将用于base compaction。单位是m字节。
* 默认值：1024

一般情况下，配置在2G以内，为了防止cumulative compaction时间过长，导致版本积压。

### `cumulative_size_based_promotion_ratio`

* 类型：double
* 描述：在size_based策略下，cumulative compaction的输出rowset总磁盘大小超过base版本rowset的配置比例时，该rowset将用于base compaction。
* 默认值：0.05

一般情况下，建议配置不要高于0.1，低于0.02。

### `cumulative_size_based_promotion_min_size_mbytes`

* 类型：int64
* 描述：在size_based策略下，cumulative compaction的输出rowset总磁盘大小低于此配置大小，该rowset将不进行base compaction，仍然处于cumulative compaction流程中。单位是m字节。
* 默认值：64

一般情况下，配置在512m以内，配置过大会导致base版本早期的大小过小，一直不进行base compaction。

### `cumulative_size_based_compaction_lower_size_mbytes`

* 类型：int64
* 描述：在size_based策略下，cumulative compaction进行合并时，选出的要进行合并的rowset的总磁盘大小大于此配置时，才按级别策略划分合并。小于这个配置时，直接执行合并。单位是m字节。
* 默认值：64

一般情况下，配置在128m以内，配置过大会导致cumulative compaction写放大较多。

### `custom_config_dir`

配置 `be_custom.conf` 文件的位置。默认为 `conf/` 目录下。

在某些部署环境下，`conf/` 目录可能因为系统的版本升级被覆盖掉。这会导致用户在运行是持久化修改的配置项也被覆盖。这时，我们可以将 `be_custom.conf` 存储在另一个指定的目录中，以防止配置文件被覆盖。

### `default_num_rows_per_column_file_block`
* 类型：int32
* 描述：配置单个RowBlock之中包含多少行的数据。
* 默认值：1024

### `default_rowset_type`
* 类型：string
* 描述：标识BE默认选择的存储格式，可配置的参数为："**ALPHA**", "**BETA**"。主要起以下两个作用
1. 当建表的storage_format设置为Default时，通过该配置来选取BE的存储格式。
2. 进行Compaction时选择BE的存储格式
* 默认值：BETA

### `delete_worker_count`

### `disable_mem_pools`

### `disable_storage_page_cache`

* 类型：bool
* 描述：是否进行使用page cache进行index的缓存，该配置仅在BETA存储格式时生效
* 默认值：false

### `disk_stat_monitor_interval`

### `doris_cgroups`

### `doris_max_pushdown_conjuncts_return_rate`

* 类型：int32
* 描述：BE在进行HashJoin时，会采取动态分区裁剪的方式将join条件下推到OlapScanner上。当OlapScanner扫描的数据大于32768行时，BE会进行过滤条件检查，如果该过滤条件的过滤率低于该配置，则Doris会停止使用动态分区裁剪的条件进行数据过滤。
* 默认值：90


### `doris_max_scan_key_num`

* 类型：int
* 描述：用于限制一个查询请求中，scan node 节点能拆分的最大 scan key 的个数。当一个带有条件的查询请求到达 scan node 节点时，scan node 会尝试将查询条件中 key 列相关的条件拆分成多个 scan key range。之后这些 scan key range 会被分配给多个 scanner 线程进行数据扫描。较大的数值通常意味着可以使用更多的 scanner 线程来提升扫描操作的并行度。但在高并发场景下，过多的线程可能会带来更大的调度开销和系统负载，反而会降低查询响应速度。一个经验数值为 50。该配置可以单独进行会话级别的配置，具体可参阅 [变量](../variables.md) 中 `max_scan_key_num` 的说明。
* 默认值：1024

当在高并发场景下发下并发度无法提升时，可以尝试降低该数值并观察影响。

### `doris_scan_range_row_count`

* 类型：int32
* 描述：BE在进行数据扫描时，会将同一个扫描范围拆分为多个ScanRange。该参数代表了每个ScanRange代表扫描数据范围。通过该参数可以限制单个OlapScanner占用io线程的时间。
* 默认值：524288

### `doris_scanner_queue_size`

* 类型：int32
* 描述：TransferThread与OlapScanner之间RowBatch的缓存队列的长度。Doris进行数据扫描时是异步进行的，OlapScanner扫描上来的Rowbatch会放入缓存队列之中，等待上层TransferThread取走。
* 默认值：1024

### `doris_scanner_row_num`

### `doris_scanner_thread_pool_queue_size`

* 类型：int32
* 描述：Scanner线程池的队列长度。在Doris的扫描任务之中，每一个Scanner会作为一个线程task提交到线程池之中等待被调度，而提交的任务数目超过线程池队列的长度之后，后续提交的任务将阻塞直到队列之中有新的空缺。
* 默认值：102400

### `doris_scanner_thread_pool_thread_num`

* 类型：int32
* 描述：Scanner线程池线程数目。在Doris的扫描任务之中，每一个Scanner会作为一个线程task提交到线程池之中等待被调度，该参数决定了Scanner线程池的大小。
* 默认值：48

### `download_low_speed_limit_kbps`

### `download_low_speed_time`

### `download_worker_count`

### `drop_tablet_worker_count`

### `enable_metric_calculator`

### `enable_partitioned_aggregation`

* 类型：bool
* 描述：BE节点是否通过PartitionAggregateNode来实现聚合操作，如果false的话将会执行AggregateNode完成聚合。非特殊需求场景不建议设置为false。
* 默认值：true

### `enable_prefetch`

* 类型：bool
* 描述：当使用PartitionedHashTable进行聚合和join计算时，是否进行HashBuket的预取，推荐设置为true。
* 默认值：true

### `enable_quadratic_probing`

* 类型：bool
* 描述：当使用PartitionedHashTable时发生Hash冲突时，是否采用平方探测法来解决Hash冲突。该值为false的话，则选用线性探测发来解决Hash冲突。关于平方探测法可参考：[quadratic_probing](https://en.wikipedia.org/wiki/Quadratic_probing)
* 默认值：true


### `enable_system_metrics`

### `enable_token_check`

### `es_http_timeout_ms`

### `es_scroll_keepalive`

### `etl_thread_pool_queue_size`

### `etl_thread_pool_size`

### `exchg_node_buffer_size_bytes`

* 类型：int32
* 描述：ExchangeNode节点Buffer队列的大小，单位为byte。来自Sender端发送的数据量大于ExchangeNode的Buffer大小之后，后续发送的数据将阻塞直到Buffer腾出可写入的空间。
* 默认值：10485760

### `file_descriptor_cache_capacity`

### `file_descriptor_cache_clean_interval`

### `flush_thread_num_per_store`

### `force_recovery`

### `fragment_pool_queue_size`

### `fragment_pool_thread_num`

### `heartbeat_service_port`

* 类型：int32
* 描述：BE 上心跳服务端口（thrift），用于接收来自 FE 的心跳
* 默认值：9050

### `heartbeat_service_thread_count`

* 类型：int32
* 描述：执行BE上心跳服务的线程数，默认为1，不建议修改
* 默认值：1

### `ignore_broken_disk`

​	当BE启动时，会检查``storage_root_path`` 配置下的所有路径。

 - `ignore_broken_disk=true`

   如果路径不存在或路径下无法进行读写文件(坏盘)，将忽略此路径，如果有其他可用路径则不中断启动。

 - `ignore_broken_disk=false`

   如果路径不存在或路径下无法进行读写文件(坏盘)，将中断启动失败退出。

​    默认为false

### `ignore_load_tablet_failure`

* 类型：bool
* 描述：用来决定在有tablet 加载失败的情况下是否忽略错误，继续启动be
* 默认值：false

BE启动时，会对每个数据目录单独启动一个线程进行 tablet header 元信息的加载。默认配置下，如果某个数据目录有 tablet 加载失败，则启动进程会终止。同时会在 `be.INFO` 日志中看到如下错误信息：

```
load tablets from header failed, failed tablets size: xxx, path=xxx
```

表示该数据目录共有多少 tablet 加载失败。同时，日志中也会有加载失败的 tablet 的具体信息。此时需要人工介入来对错误原因进行排查。排查后，通常有两种方式进行恢复：

1. tablet 信息不可修复，在确保其他副本正常的情况下，可以通过 `meta_tool` 工具将错误的tablet删除。
2. 将 `ignore_load_tablet_failure` 设置为 true，则 BE 会忽略这些错误的 tablet，正常启动。

### `ignore_rowset_stale_unconsistent_delete`

* 类型：bool
* 描述：用来决定当删除过期的合并过的rowset后无法构成一致的版本路径时，是否仍要删除。
* 默认值：false

合并的过期 rowset 版本路径会在半个小时后进行删除。在异常下，删除这些版本会出现构造不出查询一致路径的问题，当配置为false时，程序检查比较严格，程序会直接报错退出。
当配置为true时，程序会正常运行，忽略这个错误。一般情况下，忽略这个错误不会对查询造成影响，仅会在fe下发了合并过的版本时出现-230错误。

### `inc_rowset_expired_sec`

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

* 类型：int32
* 描述：Compaction线程池中线程数量的最大值。
* 默认值：10

### `max_consumer_num_per_group`

### `max_cumulative_compaction_num_singleton_deltas`

### `max_download_speed_kbps`

### `max_free_io_buffers`

### `max_garbage_sweep_interval`

### `max_memory_sink_batch_count`

### `max_percentage_of_error_disk`

* 类型：int32
* 描述：存储引擎允许存在损坏硬盘的百分比，损坏硬盘超过改比例后，BE将会自动退出。
* 默认值：0

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

### `max_tablet_version_num`

* 类型：int
* 描述：限制单个 tablet 最大 version 的数量。用于防止导入过于频繁，或 compaction 不及时导致的大量 version 堆积问题。当超过限制后，导入任务将被拒绝。
* 默认值：500

### `mem_limit`

* 类型：string
* 描述：限制BE进程使用服务器最大内存百分比。用于防止BE内存挤占太多的机器内存，该参数必须大于0，当百分大于100%之后，该值会默认为100%。
* 默认值：80%

### `memory_limitation_per_thread_for_schema_change`

### `memory_maintenance_sleep_time_s`

### `memory_max_alignment`

### `min_buffer_size`

### `min_compaction_failure_interval_sec`

* 类型：int32
* 描述：在 cumulative compaction 过程中，当选中的 tablet 没能成功的进行版本合并，则会等待一段时间后才会再次有可能被选中。等待的这段时间就是这个配置的值。
* 默认值：600
* 单位：秒

### `min_compaction_threads`

* 类型：int32
* 描述：Compaction线程池中线程数量的最小值。
* 默认值：10

### `min_cumulative_compaction_num_singleton_deltas`

### `min_file_descriptor_number`

### `min_garbage_sweep_interval`

### `mmap_buffers`

### `num_cores`

* 类型：int32
* 描述：BE可以使用CPU的核数。当该值为0时，BE将从/proc/cpuinfo之中获取本机的CPU核数。
* 默认值：0

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

* 类型：int32
* 描述：BE单测时使用的端口，在实际环境之中无意义，可忽略。
* 默认值：20001

### `pprof_profile_dir`

### `priority_networks`

### `priority_queue_remaining_tasks_increased_frequency`

### `publish_version_worker_count`

### `pull_load_task_dir`

### `push_worker_count_high_priority`

### `push_worker_count_normal_priority`

### `push_write_mbytes_per_sec`

+ 类型：int32
+ 描述：导入数据速度控制，默认最快每秒10MB。适用于所有的导入方式。
+ 单位：MB
+ 默认值：10

### `query_scratch_dirs`

* 类型：string
* 描述：BE进行数据落盘时选取的目录来存放临时数据，与存储路径配置类似，多目录之间用;分隔。
* 默认值：${DORIS_HOME}

### `read_size`

### `release_snapshot_worker_count`

### `report_disk_state_interval_seconds`

### `report_tablet_interval_seconds`

### `report_task_interval_seconds`

### `result_buffer_cancelled_interval_time`

### `routine_load_thread_pool_size`

### `row_nums_check`

### `row_step_for_compaction_merge_log`

* 类型：int64
* 描述：Compaction执行过程中，每次合并row_step_for_compaction_merge_log行数据会打印一条LOG。如果该参数被设置为0，表示merge过程中不需要打印LOG。
* 默认值： 0
* 可动态修改：是

### `scan_context_gc_interval_min`

### `serialize_batch`

### `sleep_one_second`
+ 类型：int32
+ 描述：全局变量，用于BE线程休眠1秒，不应该被修改
+ 默认值：1

### `small_file_dir`

### `snapshot_expire_time_sec`

### `status_report_interval`

### `storage_flood_stage_left_capacity_bytes`

### `storage_flood_stage_usage_percent`

### `storage_medium_migrate_count`

### `storage_page_cache_limit`

### `index_page_cache_percentage`
* 类型：int32
* 描述：索引页缓存占总页面缓存的百分比，取值为[0, 100]。
* 默认值：10

### `storage_root_path`

* 类型：string
* 描述：BE数据存储的目录,多目录之间用;分隔。可以通过路径区别存储目录的介质，HDD或SSD。可以添加容量限制在每个路径的末尾，通过,隔开。
eg：storage_root_path=/home/disk1/doris.HDD,50;/home/disk2/doris.SSD,1;/home/disk2/doris

	* 1./home/disk1/doris.HDD, 存储限制为50GB, HDD;
	* 2./home/disk2/doris.SSD，存储限制为1GB，SSD；
	* 3./home/disk2/doris，存储限制为磁盘容量，默认为HDD

* 默认值：${DORIS_HOME}

### `storage_strict_check_incompatible_old_format`
* 类型：bool
* 描述：用来检查不兼容的旧版本格式时是否使用严格的验证方式
* 默认值： true
* 可动态修改：否

配置用来检查不兼容的旧版本格式时是否使用严格的验证方式，当含有旧版本的 hdr 格式时，使用严谨的方式时，程序会
打出 fatal log 并且退出运行；否则，程序仅打印 warn log.

### `streaming_load_max_mb`

* 类型：int64
* 描述：用于限制数据格式为 csv 的一次 Stream load 导入中，允许的最大数据量。单位 MB。
* 默认值： 10240
* 可动态修改：是

Stream Load 一般适用于导入几个GB以内的数据，不适合导入过大的数据。

### `streaming_load_json_max_mb`

* 类型：int64
* 描述：用于限制数据格式为 json 的一次 Stream load 导入中，允许的最大数据量。单位 MB。
* 默认值： 100
* 可动态修改：是

一些数据格式，如 JSON，无法进行拆分处理，必须读取全部数据到内存后才能开始解析，因此，这个值用于限制此类格式数据单次导入最大数据量。

### `streaming_load_rpc_max_alive_time_sec`

### `sync_tablet_meta`

### `sys_log_dir`

* 类型：string
* 描述：BE日志数据的存储目录
* 默认值：${DORIS_HOME}/log

### `sys_log_level`

### `sys_log_roll_mode`

### `sys_log_roll_num`

### `sys_log_verbose_level`

### `sys_log_verbose_modules`

### `tablet_map_shard_size`

### `tablet_meta_checkpoint_min_interval_secs`

### `tablet_meta_checkpoint_min_new_rowsets_num`

### `tablet_scan_frequency_time_node_interval_second`

* 类型：int64
* 描述：用来表示记录 metric 'query_scan_count' 的时间间隔。为了计算当前一段时间的tablet的scan频率，需要每隔一段时间记录一次 metric 'query_scan_count'。
* 默认值：300

### `tablet_stat_cache_update_interval_second`

### `tablet_rowset_stale_sweep_time_sec`

* 类型：int64
* 描述：用来表示清理合并版本的过期时间，当当前时间 now() 减去一个合并的版本路径中rowset最近创建创建时间大于tablet_rowset_stale_sweep_time_sec时，对当前路径进行清理，删除这些合并过的rowset, 单位为s。
* 默认值：1800

当写入过于频繁，磁盘时间不足时，可以配置较少这个时间。不过这个时间过短小于5分钟时，可能会引发fe查询不到已经合并过的版本，引发查询-230错误。

### `tablet_writer_open_rpc_timeout_sec`

### `tablet_writer_ignore_eovercrowded`

* 类型：bool
* 描述：写入时可忽略brpc的'[E1011]The server is overcrowded'错误。
* 默认值：false

当遇到'[E1011]The server is overcrowded'的错误时，可以调整配置项`brpc_socket_max_unwritten_bytes`，但这个配置项不能动态调整。所以可通过设置此项为`true`来临时避免写失败。注意，此配置项只影响写流程，其他的rpc请求依旧会检查是否overcrowded。

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

### `thrift_server_type_of_fe`

该配置表示FE的Thrift服务使用的服务模型, 类型为string, 大小写不敏感,该参数需要和fe的thrift_server_type参数的设置保持一致。目前该参数的取值有两个,`THREADED`和`THREAD_POOL`。

若该参数为`THREADED`, 该模型为非阻塞式I/O模型，

若该参数为`THREAD_POOL`, 该模型为阻塞式I/O模型。

### `total_permits_for_compaction_score`

* 类型：int64
* 描述：被所有的compaction任务所能持有的 "permits" 上限，用来限制compaction占用的内存。
* 默认值：10000
* 可动态修改：是

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
* 类型：int32
* 描述：BE 上的 http server 的服务端口
* 默认值：8040

### `write_buffer_size`

### `zone_map_row_num_threshold`

* 类型： int32
* 描述: 如果一个page中的行数小于这个值就不会创建zonemap，用来减少数据膨胀
* 默认值: 20

