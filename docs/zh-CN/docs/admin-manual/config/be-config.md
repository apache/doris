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

BE 的配置文件 `be.conf` 通常存放在 BE 部署路径的 `conf/` 目录下。 而在 0.14 版本中会引入另一个配置文件 `be_custom.conf`。该配置文件用于记录用户在运行时动态配置并持久化的配置项。

BE 进程启动后，会先读取 `be.conf` 中的配置项，之后再读取 `be_custom.conf` 中的配置项。`be_custom.conf` 中的配置项会覆盖 `be.conf` 中相同的配置项。

## 查看配置项

用户可以通过访问 BE 的 Web 页面查看当前配置项：

`http://be_host:be_webserver_port/varz`

## 设置配置项

BE 的配置项有两种方式进行配置：

1. 静态配置

  在 `conf/be.conf` 文件中添加和设置配置项。`be.conf` 中的配置项会在 BE 进行启动时被读取。没有在 `be.conf` 中的配置项将使用默认值。

2. 动态配置

  BE 启动后，可以通过以下命令动态设置配置项。

  ```
  curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}'
  ```

  在 0.13 版本及之前，通过该方式修改的配置项将在 BE 进程重启后失效。在 0.14 及之后版本中，可以通过以下命令持久化修改后的配置。修改后的配置项存储在 `be_custom.conf` 文件中。

  ```
  curl -X POST http://{be_ip}:{be_http_port}/api/update_config?{key}={value}&persist=true
  ```

## 应用举例

1. 静态方式修改 `max_base_compaction_threads`

  通过在 `be.conf` 文件中添加：

  ```max_base_compaction_threads=5```

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

默认值：3

进行schema change的线程数

### `generate_compaction_tasks_min_interval_ms`

默认值：10 （ms）

生成compaction作业的最小间隔时间（ms）

### `enable_vectorized_compaction`

默认值：true

是否开启向量化compaction

### `base_compaction_interval_seconds_since_last_operation`

默认值：86400

BaseCompaction触发条件之一：上一次BaseCompaction距今的间隔

### `base_compaction_num_cumulative_deltas`

默认值：5

BaseCompaction触发条件之一：Cumulative文件数目要达到的限制，达到这个限制之后会触发BaseCompaction

### `base_compaction_write_mbytes_per_sec`

默认值：5（MB）

BaseCompaction任务每秒写磁盘最大速度

### `base_cumulative_delta_ratio`

默认值：0.3  （30%）

BaseCompaction触发条件之一：Cumulative文件大小达到Base文件的比例

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

### `transfer_large_data_by_brpc`

* 类型: bool
* 描述：该配置用来控制是否在 Tuple/Block data 长度大于1.8G时，将 protoBuf request 序列化后和 Tuple/Block data 一起嵌入到 controller attachment 后通过 http brpc 发送。为了避免 protoBuf request 的长度超过2G时的错误：Bad request, error_text=[E1003]Fail to compress request。在过去的版本中，曾将 Tuple/Block data 放入 attachment 后通过默认的 baidu_std brpc 发送，但 attachment 超过2G时将被截断，通过 http brpc 发送不存在2G的限制。
* 默认值：true

### `brpc_num_threads`

该配置主要用来修改brpc中bthreads的数量. 该配置的默认值被设置为-1, 这意味着bthreads的数量将被设置为机器的cpu核数。

用户可以将该配置的值调大来获取更好的QPS性能。更多的信息可以参考`https://github.com/apache/incubator-brpc/blob/master/docs/cn/benchmark.md`。

### `brpc_port`

* 类型：int32
* 描述：BE 上的 brpc 的端口，用于 BE 之间通讯
* 默认值：8060

### `buffer_pool_clean_pages_limit`

默认值：50%

清理可能被缓冲池保存的Page

### `buffer_pool_limit`

* 类型：string
* 描述：buffer pool之中最大的可分配内存
* 默认值：20%

BE缓存池最大的内存可用量，buffer pool是BE新的内存管理结构，通过buffer page来进行内存管理，并能够实现数据的落盘。并发的所有查询的内存申请都会通过buffer pool来申请。当前buffer pool仅作用在**AggregationNode**与**ExchangeNode**。

### `check_auto_compaction_interval_seconds`

* 类型：int32
* 描述：当自动执行compaction的功能关闭时，检查自动compaction开关是否被开启的时间间隔。
* 默认值：5

### `check_consistency_worker_count`

默认值：1

计算tablet的校验和(checksum)的工作线程数

### `chunk_reserved_bytes_limit`

默认值：20%

Chunk Allocator的reserved bytes限制，通常被设置为 mem_limit 的百分比。默认单位字节，值必须是2的倍数，且必须大于0，如果大于物理内存，将被设置为物理内存大小。增加这个变量可以提高性能，但是会获得更多其他模块无法使用的空闲内存。

### `clear_transaction_task_worker_count`

默认值：1

用于清理事务的线程数

### `clone_worker_count`

默认值：3

用于执行克隆任务的线程数

### `cluster_id`

* 类型：int32

* 描述：配置BE的所属于的集群id。

* 默认值：-1

该值通常由FE通过心跳向BE下发，不需要额外进行配置。当确认某BE属于某一个确定的Drois集群时，可以进行配置，同时需要修改数据目录下的cluster_id文件，使二者相同。

### `column_dictionary_key_ratio_threshold`

默认值：0

字符串类型的取值比例，小于这个比例采用字典压缩算法

### `column_dictionary_key_size_threshold`

默认值：0

字典压缩列大小，小于这个值采用字典压缩算法

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

tablet_score = compaction_tablet_scan_frequency_factor * tablet_scan_frequency + compaction_tablet_compaction_score_factor * compaction_score

### `compaction_task_num_per_disk`

* 类型：int32
* 描述：每个磁盘（HDD）可以并发执行的compaction任务数量。
* 默认值：2

### `compaction_task_num_per_fast_disk`

* 类型：int32
* 描述：每个高速磁盘（SSD）可以并发执行的compaction任务数量。
* 默认值：4

### `compress_rowbatches`
* 类型：bool

* 描述：序列化RowBatch时是否使用Snappy压缩算法进行数据压缩

* 默认值：true

### `create_tablet_worker_count`

默认值：3

BE创建tablet的工作线程数

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

默认值：104857600

BaseCompaction触发条件之一：Singleton文件大小限制，100MB

### `cumulative_compaction_trace_threshold`

* 类型：int32
* 描述：打印cumulative compaction的trace信息的阈值，单位秒
* 默认值：2

与base_compaction_trace_threshold类似。

### disable_compaction_trace_log

* 类型: bool
* 描述: 关闭compaction的trace日志
* 默认值: true

如果设置为true，`cumulative_compaction_trace_threshold` 和 `base_compaction_trace_threshold` 将不起作用。并且trace日志将关闭。

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

默认值：3

执行数据删除任务的线程数

### `disable_mem_pools`

默认值：false

是否禁用内存缓存池，默认不禁用

### `disable_storage_page_cache`

* 类型：bool
* 描述：是否进行使用page cache进行index的缓存，该配置仅在BETA存储格式时生效
* 默认值：false

### `disk_stat_monitor_interval`

默认值：5  （s）

磁盘状态检查时间间隔

### `doris_cgroups`

默认值：空

分配给doris的cgroups

### `doris_max_pushdown_conjuncts_return_rate`

* 类型：int32
* 描述：BE在进行HashJoin时，会采取动态分区裁剪的方式将join条件下推到OlapScanner上。当OlapScanner扫描的数据大于32768行时，BE会进行过滤条件检查，如果该过滤条件的过滤率低于该配置，则Doris会停止使用动态分区裁剪的条件进行数据过滤。
* 默认值：90


### `doris_max_scan_key_num`

* 类型：int
* 描述：用于限制一个查询请求中，scan node 节点能拆分的最大 scan key 的个数。当一个带有条件的查询请求到达 scan node 节点时，scan node 会尝试将查询条件中 key 列相关的条件拆分成多个 scan key range。之后这些 scan key range 会被分配给多个 scanner 线程进行数据扫描。较大的数值通常意味着可以使用更多的 scanner 线程来提升扫描操作的并行度。但在高并发场景下，过多的线程可能会带来更大的调度开销和系统负载，反而会降低查询响应速度。一个经验数值为 50。该配置可以单独进行会话级别的配置，具体可参阅 [变量](../../advanced/variables.md) 中 `max_scan_key_num` 的说明。
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

默认值：16384

每个扫描线程单次执行最多返回的数据行数

### `doris_scanner_thread_pool_queue_size`

* 类型：int32
* 描述：Scanner线程池的队列长度。在Doris的扫描任务之中，每一个Scanner会作为一个线程task提交到线程池之中等待被调度，而提交的任务数目超过线程池队列的长度之后，后续提交的任务将阻塞直到队列之中有新的空缺。
* 默认值：102400

### `doris_scanner_thread_pool_thread_num`

* 类型：int32
* 描述：Scanner线程池线程数目。在Doris的扫描任务之中，每一个Scanner会作为一个线程task提交到线程池之中等待被调度，该参数决定了Scanner线程池的大小。
* 默认值：48

### `download_low_speed_limit_kbps`

默认值：50 (KB/s)

下载最低限速

### `download_low_speed_time`

默认值：300 (s)

下载时间限制，默认300秒

### `download_worker_count`

默认值：1

下载线程数，默认1个

### `drop_tablet_worker_count`

默认值：3

删除tablet的线程数

### `enable_metric_calculator`

默认值：true

如果设置为 true，metric calculator 将运行，收集BE相关指标信息，如果设置成false将不运行

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

默认值：true

用户控制打开和关闭系统指标

### `enable_token_check`

默认值：true

用于向前兼容，稍后将被删除

### `enable_stream_load_record`

默认值：false

是否开启 stream load 操作记录，默认是不启用

### `es_http_timeout_ms`

默认值：5000 (ms)

通过http连接ES的超时时间，默认是5秒

### `es_scroll_keepalive`

默认值：5m

es scroll Keeplive保持时间，默认5分钟

### `etl_thread_pool_queue_size`

默认值：256

ETL线程池的大小

### `etl_thread_pool_size`

### `exchg_node_buffer_size_bytes`

* 类型：int32
* 描述：ExchangeNode节点Buffer队列的大小，单位为byte。来自Sender端发送的数据量大于ExchangeNode的Buffer大小之后，后续发送的数据将阻塞直到Buffer腾出可写入的空间。
* 默认值：10485760

### `file_descriptor_cache_capacity`

默认值：32768

文件句柄缓存的容量，默认缓存32768个文件句柄

### `cache_clean_interval`

默认值：1800 (s)

文件句柄缓存清理的间隔，用于清理长期不用的文件句柄。
同时也是Segment Cache的清理间隔时间。

### `flush_thread_num_per_store`

默认值：2

每个store用于刷新内存表的线程数

### `force_recovery`

### `fragment_pool_queue_size`

默认值：2048

单节点上能够处理的查询请求上限

### `fragment_pool_thread_num_min`

默认值：64

### `fragment_pool_thread_num_max`

默认值：256

查询线程数，默认最小启动64个线程，后续查询请求动态创建线程，最大创建256个线程

### `heartbeat_service_port`

* 类型：int32
* 描述：BE 上心跳服务端口（thrift），用于接收来自 FE 的心跳
* 默认值：9050

### `heartbeat_service_thread_count`

* 类型：int32
* 描述：执行BE上心跳服务的线程数，默认为1，不建议修改
* 默认值：1

### `ignore_broken_disk`

 当BE启动时，会检查``storage_root_path`` 配置下的所有路径。

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

默认值：1800 （s）

导入激活的数据，存储引擎保留的时间，用于增量克隆

### `index_stream_cache_capacity`

默认值：10737418240

BloomFilter/Min/Max等统计信息缓存的容量

### `kafka_api_version_request`

默认值：true

如果依赖的 kafka 版本低于0.10.0.0, 该值应该被设置为 false。

### `kafka_broker_version_fallback`

默认值：0.10.0

如果依赖的 kafka 版本低于0.10.0.0, 当 kafka_api_version_request 值为 false 的时候，将使用回退版本 kafka_broker_version_fallback 设置的值，有效值为：0.9.0.x、0.8.x.y。

### `load_data_reserve_hours`

默认值：4 （小时）

用于mini load。mini load数据文件将在此时间后被删除

### `load_error_log_reserve_hours`

默认值：48（小时）

load错误日志将在此时间后删除

### `load_process_max_memory_limit_bytes`

默认值：107374182400

单节点上所有的导入线程占据的内存上限，默认值：100G

将这些默认值设置得很大，因为我们不想在用户升级 Doris 时影响负载性能。 如有必要，用户应正确设置这些配置。

### `load_process_max_memory_limit_percent`

默认值：50

单节点上所有的导入线程占据的内存上限比例，默认50%

将这些默认值设置得很大，因为我们不想在用户升级 Doris 时影响负载性能。 如有必要，用户应正确设置这些配置。

### `load_process_soft_mem_limit_percent`

默认值：50

soft limit是指站单节点导入内存上限的比例。例如所有导入任务导入的内存上限是20GB，则soft limit默认为该值的50%，即10GB。导入内存占用超过soft limit时，会挑选占用内存最大的作业进行下刷以提前释放内存空间，默认50%

### `log_buffer_level`

默认值：空

日志刷盘的策略，默认保持在内存中

### `madvise_huge_pages`

默认值：false

是否使用linux内存大页，默认不启用

### `make_snapshot_worker_count`

默认值：5

制作快照的线程数

### `max_client_cache_size_per_host`

默认值：10

每个主机的最大客户端缓存数，BE 中有多种客户端缓存，但目前我们使用相同的缓存大小配置。 如有必要，使用不同的配置来设置不同的客户端缓存。

### `max_base_compaction_threads`

* 类型：int32
* 描述：Base Compaction线程池中线程数量的最大值。
* 默认值：4

### `max_cumu_compaction_threads`

* 类型：int32
* 描述：Cumulative Compaction线程池中线程数量的最大值。
* 默认值：10

### `max_consumer_num_per_group`

默认值：3

一个数据消费者组中的最大消费者数量，用于routine load

### `min_cumulative_compaction_num_singleton_deltas`

默认值：5

cumulative compaction策略：最小增量文件的数量

### `max_cumulative_compaction_num_singleton_deltas`

默认值：1000

cumulative compaction策略：最大增量文件的数量

### `max_download_speed_kbps`

默认值：50000 （kb/s）

最大下载速度限制

### `max_free_io_buffers`

默认值：128

对于每个 io 缓冲区大小，IoMgr 将保留的最大缓冲区数从 1024B 到 8MB 的缓冲区，最多约为 2GB 的缓冲区。

### `max_garbage_sweep_interval`

默认值：3600

磁盘进行垃圾清理的最大间隔，默认一个小时  

### `max_memory_sink_batch_count`

默认值：20

最大外部扫描缓存批次计数，表示缓存max_memory_cache_batch_count * batch_size row，默认为20，batch_size的默认值为1024，表示将缓存20 * 1024行

### `max_percentage_of_error_disk`

* 类型：int32
* 描述：存储引擎允许存在损坏硬盘的百分比，损坏硬盘超过改比例后，BE将会自动退出。
* 默认值：0

### `max_pushdown_conditions_per_column`

* 类型：int
* 描述：用于限制一个查询请求中，针对单个列，能够下推到存储引擎的最大条件数量。在查询计划执行的过程中，一些列上的过滤条件可以下推到存储引擎，这样可以利用存储引擎中的索引信息进行数据过滤，减少查询需要扫描的数据量。比如等值条件、IN 谓词中的条件等。这个参数在绝大多数情况下仅影响包含 IN 谓词的查询。如 `WHERE colA IN (1,2,3,4,...)`。较大的数值意味值 IN 谓词中更多的条件可以推送给存储引擎，但过多的条件可能会导致随机读的增加，某些情况下可能会降低查询效率。该配置可以单独进行会话级别的配置，具体可参阅 [变量](../../advanced/variables.md) 中 `max_pushdown_conditions_per_column ` 的说明。
* 默认值：1024

* 示例

    表结构为 `id INT, col2 INT, col3 varchar(32), ...`。
    
    查询请求为 `... WHERE id IN (v1, v2, v3, ...)`
    
    如果 IN 谓词中的条件数量超过了该配置，则可以尝试增加该配置值，观察查询响应是否有所改善。

### `max_runnings_transactions_per_txn_map`

默认值：100

txn 管理器中每个 txn_partition_map 的最大 txns 数，这是一种自我保护，以避免在管理器中保存过多的 txns

### `max_send_batch_parallelism_per_job`

* 类型：int
* 描述：OlapTableSink 发送批处理数据的最大并行度，用户为 `send_batch_parallelism` 设置的值不允许超过 `max_send_batch_parallelism_per_job` ，如果超过， `send_batch_parallelism` 将被设置为 `max_send_batch_parallelism_per_job` 的值。
* 默认值：5

### `max_tablet_num_per_shard`

默认：1024

每个shard的tablet数目，用于划分tablet，防止单个目录下tablet子目录过多

### `max_tablet_version_num`

* 类型：int
* 描述：限制单个 tablet 最大 version 的数量。用于防止导入过于频繁，或 compaction 不及时导致的大量 version 堆积问题。当超过限制后，导入任务将被拒绝。
* 默认值：500

### `mem_limit`

* 类型：string
* 描述：限制BE进程使用服务器最大内存百分比。用于防止BE内存挤占太多的机器内存，该参数必须大于0，当百分大于100%之后，该值会默认为100%。
* 默认值：80%

### `memory_limitation_per_thread_for_schema_change`

默认值：2 （GB）

单个schema change任务允许占用的最大内存

### `memory_maintenance_sleep_time_s`

默认值：10 

内存维护迭代之间的休眠时间（以秒为单位）

### `memory_max_alignment`

默认值：16

最大校对内存

### `read_size`

默认值：8388608

读取大小是发送到 os 的读取大小。 在延迟和整个过程之间进行权衡，试图让磁盘保持忙碌但不引入寻道。 对于 8 MB 读取，随机 io 和顺序 io 的性能相似

### `min_buffer_size`

默认值：1024

最小读取缓冲区大小（以字节为单位）

### `min_compaction_failure_interval_sec`

* 类型：int32
* 描述：在 cumulative compaction 过程中，当选中的 tablet 没能成功的进行版本合并，则会等待一段时间后才会再次有可能被选中。等待的这段时间就是这个配置的值。
* 默认值：5
* 单位：秒

### `min_compaction_threads`

* 类型：int32
* 描述：Compaction线程池中线程数量的最小值。
* 默认值：10

### `min_file_descriptor_number`

默认值：60000

BE进程的文件句柄limit要求的下限

### `min_garbage_sweep_interval`

默认值：180

磁盘进行垃圾清理的最小间隔，时间秒

### `mmap_buffers`

默认值：false

是否使用mmap分配内存，默认不使用

### `num_cores`

* 类型：int32
* 描述：BE可以使用CPU的核数。当该值为0时，BE将从/proc/cpuinfo之中获取本机的CPU核数。
* 默认值：0

### `num_disks`

默认值：0

控制机器上的磁盘数量。 如果为 0，则来自系统设置。

### `num_threads_per_core`

默认值：3

控制每个内核运行工作的线程数。 通常选择 2 倍或 3 倍的内核数量。 这使核心保持忙碌而不会导致过度抖动

### `num_threads_per_disk`

默认值：0

每个磁盘的最大线程数也是每个磁盘的最大队列深度

### `number_slave_replica_download_threads`

默认值: 64

每个BE节点上slave副本同步Master副本数据的线程数，用于单副本数据导入功能。

### `number_tablet_writer_threads`

默认值：16

tablet写线程数

### `path_gc_check`

默认值：true

是否启用回收扫描数据线程检查，默认启用

### `path_gc_check_interval_second`

默认值：86400

回收扫描数据线程检查时间间隔，单位秒

### `path_gc_check_step`

默认值：1000

### `path_gc_check_step_interval_ms`

默认值：10 (ms)

### `path_scan_interval_second`

默认值：86400

### `pending_data_expire_time_sec`

默认值：1800 

存储引擎保留的未生效数据的最大时长，默认单位：秒

### `periodic_counter_update_period_ms`

默认值：500

更新速率计数器和采样计数器的周期，默认单位：毫秒

### `plugin_path`

默认值：${DORIS_HOME}/plugin

插件路径

### `port`

* 类型：int32
* 描述：BE单测时使用的端口，在实际环境之中无意义，可忽略。
* 默认值：20001

### `pprof_profile_dir`

默认值：${DORIS_HOME}/log

pprof profile保存目录

### `priority_networks`

默认值：空

为那些有很多 ip 的服务器声明一个选择策略。 请注意，最多应该有一个 ip 与此列表匹配。 这是一个以分号分隔格式的列表，用 CIDR 表示法，例如 10.10.10.0/24 ， 如果没有匹配这条规则的ip，会随机选择一个。

### `priority_queue_remaining_tasks_increased_frequency`

默认值：512

 the increased frequency of priority for remaining tasks in BlockingPriorityQueue

### `publish_version_worker_count`

默认值：8

生效版本的线程数

### `pull_load_task_dir`

默认值：${DORIS_HOME}/var/pull_load

拉取laod任务的目录

### `push_worker_count_high_priority`

默认值：3

导入线程数，用于处理HIGH优先级任务

### `push_worker_count_normal_priority`

默认值：3

导入线程数，用于处理NORMAL优先级任务

### `query_scratch_dirs`

* 类型：string
* 描述：BE进行数据落盘时选取的目录来存放临时数据，与存储路径配置类似，多目录之间用;分隔。
* 默认值：${DORIS_HOME}

### `release_snapshot_worker_count`

默认值：5

释放快照的线程数

### `report_disk_state_interval_seconds`

默认值：60

代理向 FE 报告磁盘状态的间隔时间（秒）

### `report_tablet_interval_seconds`

默认值：60

代理向 FE 报告 olap 表的间隔时间（秒）

### `report_task_interval_seconds`

默认值：10

代理向 FE 报告任务签名的间隔时间（秒）

### `result_buffer_cancelled_interval_time`

默认值：300

结果缓冲区取消时间（单位：秒）

### `routine_load_thread_pool_size`

默认值：10

routine load任务的线程池大小。 这应该大于 FE 配置 'max_concurrent_task_num_per_be'（默认 5）

### `row_nums_check`

默认值：true

检查 BE/CE 和schema更改的行号。 true 是打开的，false 是关闭的。

### `row_step_for_compaction_merge_log`

* 类型：int64
* 描述：Compaction执行过程中，每次合并row_step_for_compaction_merge_log行数据会打印一条LOG。如果该参数被设置为0，表示merge过程中不需要打印LOG。
* 默认值： 0
* 可动态修改：是

### `scan_context_gc_interval_min`

默认值：5

此配置用于上下文gc线程调度周期 ， 注意：单位为分钟，默认为 5 分钟

### `send_batch_thread_pool_thread_num`

* 类型：int32
* 描述：SendBatch线程池线程数目。在NodeChannel的发送数据任务之中，每一个NodeChannel的SendBatch操作会作为一个线程task提交到线程池之中等待被调度，该参数决定了SendBatch线程池的大小。
* 默认值：64

### `send_batch_thread_pool_queue_size`

* 类型：int32
* 描述：SendBatch线程池的队列长度。在NodeChannel的发送数据任务之中，每一个NodeChannel的SendBatch操作会作为一个线程task提交到线程池之中等待被调度，而提交的任务数目超过线程池队列的长度之后，后续提交的任务将阻塞直到队列之中有新的空缺。
* 默认值：102400

### `download_cache_thread_pool_thread_num`

* 类型: int32
* 描述: DownloadCache线程池线程数目. 在FileCache的缓存下载任务之中, 缓存下载操作会作为一个线程task提交到线程池之中等待被调度，该参数决定了DownloadCache线程池的大小。
* 默认值：48

### `download_cache_thread_pool_queue_size`

* Type: int32
* 描述: DownloadCache线程池线程数目. 在FileCache的缓存下载任务之中, 缓存下载操作会作为一个线程task提交到线程池之中等待被调度，而提交的任务数目超过线程池队列的长度之后，后续提交的任务将阻塞直到队列之中有新的空缺。
* 默认值：102400

### `download_cache_buffer_size`

* 类型: int64
* 描述: 下载缓存时用于接收数据的buffer的大小。
* 默认值: 10485760

### `serialize_batch`

默认值：false

BE之间rpc通信是否序列化RowBatch，用于查询层之间的数据传输

### `single_replica_load_brpc_port`

* 类型: int32
* 描述: 单副本数据导入功能中，Master副本和Slave副本之间通信的RPC端口。Master副本flush完成之后通过RPC通知Slave副本同步数据，以及Slave副本同步数据完成后通过RPC通知Master副本。系统为单副本数据导入过程中Master副本和Slave副本之间通信开辟了独立的BRPC线程池，以避免导入并发较大时副本之间的数据同步抢占导入数据分发和查询任务的线程资源。
* 默认值: 9070

### `single_replica_load_brpc_num_threads`

* 类型: int32
* 描述: 单副本数据导入功能中，Master副本和Slave副本之间通信的线程数量。导入并发增大时，可以适当调大该参数来保证Slave副本及时同步Master副本数据。
* 默认值: 64

### `single_replica_load_download_port`

* 类型: int32
* 描述: 单副本数据导入功能中，Slave副本通过HTTP从Master副本下载数据文件的端口。系统为单副本数据导入过程中Slave副本从Master副本下载数据文件开辟了独立的HTTP线程池，以避免导入并发较大时Slave副本下载数据文件抢占其他http任务的线程资源。
* 默认值: 8050

### `single_replica_load_download_num_workers`

* 类型: int32
* 描述: 单副本数据导入功能中，Slave副本通过HTTP从Master副本下载数据文件的线程数。导入并发增大时，可以适当调大该参数来保证Slave副本及时同步Master副本数据。
* 默认值: 64

### `slave_replica_writer_rpc_timeout_sec`

* 类型: int32
* 描述: 单副本数据导入功能中，Master副本和Slave副本之间通信的RPC超时时间。
* 默认值: 60

### `sleep_one_second`
+ 类型：int32
+ 描述：全局变量，用于BE线程休眠1秒，不应该被修改
+ 默认值：1

### `small_file_dir`

默认值：${DORIS_HOME}/lib/small_file/

用于保存 SmallFileMgr 下载的文件的目录

### `snapshot_expire_time_sec`

默认值：172800

快照文件清理的间隔，默认值：48小时

### `status_report_interval`

默认值：5

配置文件报告之间的间隔；单位：秒

### `storage_flood_stage_left_capacity_bytes`

默认值：1073741824

数据目录应该剩下的最小存储空间，默认1G

### `storage_flood_stage_usage_percent`

默认值：90 （90%）

storage_flood_stage_usage_percent和storage_flood_stage_left_capacity_bytes两个配置限制了数据目录的磁盘容量的最大使用。 如果这两个阈值都达到，则无法将更多数据写入该数据目录。 数据目录的最大已用容量百分比

### `storage_medium_migrate_count`

默认值：1

要克隆的线程数

### `storage_page_cache_limit`

默认值：20%

缓存存储页大小

### `storage_page_cache_shard_size`

默认值: 16

StoragePageCache的分片大小，值为 2^n (n=0,1,2,...)。建议设置为接近BE CPU核数的值，可减少StoragePageCache的锁竞争。

### `index_page_cache_percentage`
* 类型：int32
* 描述：索引页缓存占总页面缓存的百分比，取值为[0, 100]。
* 默认值：10

### `storage_root_path`

* 类型：string

* 描述：BE数据存储的目录,多目录之间用英文状态的分号`;`分隔。可以通过路径区别存储目录的介质，HDD或SSD。可以添加容量限制在每个路径的末尾，通过英文状态逗号`,`隔开。  
  如果用户不是SSD和HDD磁盘混合使用的情况，不需要按照如下示例一和示例二的配置方法配置，只需指定存储目录即可；也不需要修改FE的默认存储介质配置  

  示例1如下：
  
  **注意：如果是SSD磁盘要在目录后面加上`.SSD`,HDD磁盘在目录后面加`.HDD`**

  `storage_root_path=/home/disk1/doris.HDD;/home/disk2/doris.SSD;/home/disk2/doris`

  **说明**

  * /home/disk1/doris.HDD，表示存储介质是HDD;
  * /home/disk2/doris.SSD，表示存储介质是SSD；
  * /home/disk2/doris，存储介质默认为HDD
  
  示例2如下：
      
  **注意：不论HHD磁盘目录还是SSD磁盘目录，都无需添加后缀，storage_root_path参数里指定medium即可**
  
  `storage_root_path=/home/disk1/doris,medium:hdd;/home/disk2/doris,medium:ssd`
  
  **说明**
  
  - /home/disk1/doris,medium:hdd，表示存储介质是HHD;
  - /home/disk2/doris,medium:ssd，表示存储介质是SSD;


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

默认值：1200

TabletsChannel 的存活时间。如果此时通道没有收到任何数据， 通道将被删除。

### `sync_tablet_meta`

默认值：false

存储引擎是否开sync保留到磁盘上

### `sys_log_dir`

* 类型：string
* 描述：BE日志数据的存储目录
* 默认值：${DORIS_HOME}/log

### `sys_log_level`

默认值：INFO

日志级别，INFO < WARNING < ERROR < FATAL

### `sys_log_roll_mode`

默认值：SIZE-MB-1024

日志拆分的大小，每1G拆分一个日志文件

### `sys_log_roll_num`

默认值：10

日志文件保留的数目

### `sys_log_verbose_level`

默认值：10

日志显示的级别，用于控制代码中VLOG开头的日志输出

### `sys_log_verbose_modules`

默认值：空

日志打印的模块，写olap就只打印olap模块下的日志

### `tablet_map_shard_size`

默认值：1

tablet_map_lock 分片大小，值为 2^n, n=0,1,2,3,4 ，这是为了更好地管理tablet

### `tablet_meta_checkpoint_min_interval_secs`

默认值：600 （秒）

TabletMeta Checkpoint线程轮询的时间间隔

### `tablet_meta_checkpoint_min_new_rowsets_num`

默认值：10

TabletMeta Checkpoint的最小Rowset数目

### `tablet_scan_frequency_time_node_interval_second`

* 类型：int64
* 描述：用来表示记录 metric 'query_scan_count' 的时间间隔。为了计算当前一段时间的tablet的scan频率，需要每隔一段时间记录一次 metric 'query_scan_count'。
* 默认值：300

### `tablet_stat_cache_update_interval_second`

默认值：300

tablet状态缓存的更新间隔，单位：秒

### `tablet_rowset_stale_sweep_time_sec`

* 类型：int64
* 描述：用来表示清理合并版本的过期时间，当当前时间 now() 减去一个合并的版本路径中rowset最近创建创建时间大于tablet_rowset_stale_sweep_time_sec时，对当前路径进行清理，删除这些合并过的rowset, 单位为s。
* 默认值：1800

当写入过于频繁，磁盘空间不足时，可以配置较少这个时间。不过这个时间过短小于5分钟时，可能会引发fe查询不到已经合并过的版本，引发查询-230错误。

### `tablet_writer_open_rpc_timeout_sec`

默认值：60

在远程BE 中打开tablet writer的 rpc 超时。 操作时间短，可设置短超时时间

导入过程中，发送一个 Batch（1024行）的 RPC 超时时间。默认 60 秒。因为该 RPC 可能涉及多个 分片内存块的写盘操作，所以可能会因为写盘导致 RPC 超时，可以适当调整这个超时时间来减少超时错误（如 send batch fail 错误）。同时，如果调大 write_buffer_size 配置，也需要适当调大这个参数

### `tablet_writer_ignore_eovercrowded`

* 类型：bool
* 描述：写入时可忽略brpc的'[E1011]The server is overcrowded'错误。
* 默认值：false

当遇到'[E1011]The server is overcrowded'的错误时，可以调整配置项`brpc_socket_max_unwritten_bytes`，但这个配置项不能动态调整。所以可通过设置此项为`true`来临时避免写失败。注意，此配置项只影响写流程，其他的rpc请求依旧会检查是否overcrowded。

### `tc_free_memory_rate`

默认值：20   (%)

可用内存，取值范围：[0-100]

### `tc_max_total_thread_cache_bytes`

* 类型：int64
* 描述：用来限制 tcmalloc 中总的线程缓存大小。这个限制不是硬限，因此实际线程缓存使用可能超过这个限制。具体可参阅 [TCMALLOC\_MAX\_TOTAL\_THREAD\_CACHE\_BYTES](https://gperftools.github.io/gperftools/tcmalloc.html)
* 默认值： 1073741824

如果发现系统在高压力场景下，通过 BE 线程堆栈发现大量线程处于 tcmalloc 的锁竞争阶段，如大量的 `SpinLock` 相关堆栈，则可以尝试增大该参数来提升系统性能。[参考](https://github.com/gperftools/gperftools/issues/1111)

### `tc_use_memory_min`

默认值：10737418240

TCmalloc 的最小内存，当使用的内存小于这个时，不返回给操作系统

### `thrift_client_retry_interval_ms`

* 类型：int64
* 描述：用来为be的thrift客户端设置重试间隔, 避免fe的thrift server发生雪崩问题，单位为ms。
* 默认值：1000

### `thrift_connect_timeout_seconds`

默认值：3

默认thrift客户端连接超时时间（单位：秒）

### `thrift_rpc_timeout_ms`

默认值：5000

thrift默认超时时间，默认：5秒

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

默认值：259200

回收站清理的间隔，72个小时，当磁盘空间不足时，trash下的文件保存期可不遵守这个参数

### `txn_commit_rpc_timeout_ms`

默认值：10000

txn 提交 rpc 超时，默认10秒

### `txn_map_shard_size`

默认值：128

txn_map_lock 分片大小，取值为2^n，n=0,1,2,3,4。这是一项增强功能，可提高管理 txn 的性能

### `txn_shard_size`

默认值：1024

txn_lock 分片大小，取值为2^n，n=0,1,2,3,4，  这是一项增强功能，可提高提交和发布 txn 的性能

### `unused_rowset_monitor_interval`

默认值：30

清理过期Rowset的时间间隔，单位：秒

### `upload_worker_count`

默认值：1

上传文件最大线程数

### `user_function_dir`

默认值：${DORIS_HOME}/lib/udf

udf函数目录

### `webserver_num_workers`

默认值：48

webserver默认工作线程数

### `webserver_port`
* 类型：int32
* 描述：BE 上的 http server 的服务端口
* 默认值：8040

### `write_buffer_size`

默认值：104857600

刷写前缓冲区的大小

导入数据在 BE 上会先写入到一个内存块，当这个内存块达到阈值后才会写回磁盘。默认大小是 100MB。过小的阈值可能导致 BE 上存在大量的小文件。可以适当提高这个阈值减少文件数量。但过大的阈值可能导致 RPC 超时

### `zone_map_row_num_threshold`

* 类型： int32
* 描述: 如果一个page中的行数小于这个值就不会创建zonemap，用来减少数据膨胀
* 默认值: 20

### `aws_log_level`

* 类型: int32
* 描述: AWS SDK 的日志级别
  ```
     Off = 0,
     Fatal = 1,
     Error = 2,
     Warn = 3,
     Info = 4,
     Debug = 5,
     Trace = 6
  ```
* 默认值: 3

### `enable_tcmalloc_hook`

* 类型：bool
* 描述：是否Hook TCmalloc new/delete，目前在Hook中统计thread local MemTracker。
* 默认值：true

### `mem_tracker_consume_min_size_bytes`

* 类型: int32
* 描述: TCMalloc Hook consume/release MemTracker时的最小长度，小于该值的consume size会持续累加，避免频繁调用MemTracker的consume/release，减小该值会增加consume/release的频率，增大该值会导致MemTracker统计不准，理论上一个MemTracker的统计值与真实值相差 = (mem_tracker_consume_min_size_bytes * 这个MemTracker所在的BE线程数)。
* 默认值: 1048576

### `max_segment_num_per_rowset`

* 类型: int32
* 描述: 用于限制导入时，新产生的rowset中的segment数量。如果超过阈值，导入会失败并报错 -238。过多的 segment 会导致compaction占用大量内存引发 OOM 错误。
* 默认值: 200

### `remote_storage_read_buffer_mb`

* 类型: int32
* 描述: 读取hdfs或者对象存储上的文件时，使用的缓存大小。
* 默认值: 16MB

增大这个值，可以减少远端数据读取的调用次数，但会增加内存开销。

### `external_table_connect_timeout_sec`

* 类型: int32
* 描述: 和外部表建立连接的超时时间。
* 默认值: 5秒

### `segment_cache_capacity`

* 类型: int32
* 描述: Segment Cache 缓存的 Segment 最大数量
* 默认值: 1000000

默认值目前只是一个经验值，可能需要根据实际场景修改。增大该值可以缓存更多的segment从而避免一些IO。减少该值则会降低内存使用。

### `auto_refresh_brpc_channel`

* 类型: bool
* 描述: 获取brpc连接时，通过hand_shake rpc 判断连接的可用性，如果不可用则重新建立连接 
* 默认值: false

### `high_priority_flush_thread_num_per_store`

* 类型：int32
* 描述：每个存储路径所分配的用于高优导入任务的 flush 线程数量。
* 默认值：1

### `routine_load_consumer_pool_size`

* 类型：int32
* 描述：routine load 所使用的 data consumer 的缓存数量。
* 默认值：10

### `load_task_high_priority_threshold_second`

* 类型：int32
* 描述：当一个导入任务的超时时间小于这个阈值是，Doris 将认为他是一个高优任务。高优任务会使用独立的 flush 线程池。
* 默认：120

### `min_load_rpc_timeout_ms`

* 类型：int32
* 描述：load 作业中各个rpc 的最小超时时间。
* 默认：20

### `doris_scan_range_max_mb`
* 类型: int32
* 描述: 每个OlapScanner 读取的最大数据量
* 默认值: 1024

### `string_type_length_soft_limit_bytes`
* 类型: int32
* 描述: String 类型最大长度的软限，单位是字节
* 默认值: 1048576

### `enable_quick_compaction`
* 类型: bool
* 描述: 是否开启quick_compaction,主要用在小数据量频繁导入的场景,通过快速compaction的机制及时合并导入版本可以有效避免-235的问题，小数据量的定义目前是根据行数来定义
* 默认值: true

### `quick_compaction_max_rows`
* 类型: int32
* 描述: 当导入的行数小于这个值认为这次导入是小数据量的导入，在快速合并时会被选中
* 默认值: 1000

### `quick_compaction_batch_size`
* 类型: int32
* 描述: 快速合并的触发时机，导入次数达到quick_compaction_batch_size时触发一次
* 默认值: 10

### `quick_compaction_min_rowsets`
* 类型: int32
* 描述: 最少进行合并的版本数，当选中的小数据量的rowset个数，大于这个值是才会进行真正的合并
* 默认值: 10

### `generate_cache_cleaner_task_interval_sec`
* 类型：int64
* 描述：缓存文件的清理间隔，单位：秒
* 默认值：43200（12小时）

### `file_cache_type`
* 类型：string
* 描述：缓存文件的类型。whole_file_cache：将segment文件整个下载，sub_file_cache：将segment文件按大小切分成多个文件。
* 默认值：""

### `max_sub_cache_file_size`
* 类型：int64
* 描述：缓存文件使用sub_file_cache时，切分文件的最大大小，单位B
* 默认值：104857600（100MB）

### `file_cache_alive_time_sec`
* 类型：int64
* 描述：缓存文件的保存时间，单位：秒
* 默认值：604800（1个星期）
