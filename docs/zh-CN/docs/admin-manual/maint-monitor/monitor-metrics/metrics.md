---
{
    "title": "监控指标",
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

# 监控指标

Doris 的 FE 进程和 BE 进程都提供了完备的监控指标。监控指标可以分为两类：

1. 进程监控：主要展示 Doris 进程本身的一些监控值。
2. 节点监控：主要展示 Doris 进程所在节点机器本身的监控，如 CPU、内存、IO、网络等等。

可以通过访问 FE 或 BE 节点的 http 端口获取当前监控。如：

```
curl http://fe_host:http_port/metrics
curl http://be_host:webserver_port/metrics
```

默认返回 Prometheus 兼容格式的监控指标，如：

```
doris_fe_cache_added{type="partition"} 0
doris_fe_cache_added{type="sql"} 0
doris_fe_cache_hit{type="partition"} 0
doris_fe_cache_hit{type="sql"} 0
doris_fe_connection_total 2
```

如需获取 Json 格式的监控指标，请访问：

```
curl http://fe_host:http_port/metrics?type=json
curl http://be_host:webserver_port/metrics?type=json
```

## 监控等级和最佳实践

**表格中的最后一列标注了监控项的重要等级。P0 表示最重要，数值越大，重要性越低。**

绝大多数监控指标类型为 Counter。即累计值。你可通过间隔采集（如每15秒）监控值，并计算单位时间的斜率，来获得有效信息。

如可以通过计算 `doris_fe_query_err` 的斜率来获取查询错误率（error per second）。

> 欢迎完善此表格以提供更全面有效的监控指标。

## FE 监控指标

### 进程监控

| 名称                                     | 标签                                     | 单位      | 含义                                                                                                        | 说明                                                     | 等级 |
|----------------------------------------|----------------------------------------|---------|-----------------------------------------------------------------------------------------------------------|--------------------------------------------------------|----|
| `doris_fe_cache_added`                 | {type="partition"}                     | Num     | 新增的 Partition Cache 数量累计值                                                                                 |                                                        |
|                                        | {type="sql"}                           | Num     | 新增的 SQL Cache 数量累计值                                                                                       |                                                        |
| `doris_fe_cache_hit`                   | {type="partition"}                     | Num     | 命中 Partition Cache 的计数                                                                                    |                                                        |
|                                        | {type="sql"}                           | Num     | 命中 SQL Cache 的计数                                                                                          |                                                        |
| `doris_fe_connection_total`            |                                        | Num     | 当前FE的MySQL端口连接数                                                                                           | 用于监控查询连接数。如果连接数超限，则新的连接将无法接入                           | P0 |
| `doris_fe_counter_hit_sql_block_rule`  |                                        | Num     | 被 SQL BLOCK RULE 拦截的查询数量                                                                                  |                                                        |    |
| `doris_fe_edit_log_clean`              | {type="failed"}                        | Num     | 清理历史元数据日志失败的次数                                                                                            | 不应失败，如失败，需人工介入                                         | P0 |
|                                        | {type="success"}                       | Num     | 清理历史元数据日志成功的次数                                                                                            |                                                        |
| `doris_fe_edit_log`                    | {type="accumulated_bytes"}             | 字节      | 元数据日志写入量的累计值                                                                                              | 通过计算斜率可以获得写入速率，来观察是否元数据写入有延迟                           | P0 |
|                                        | {type="current_bytes"}                 | 字节      | 元数据日志当前值                                                                                                  | 用于监控editlog 大小。如果大小超限，需人工介入                            | P0 |
|                                        | {type="read"}                          | Num     | 元数据日志读取次数的计数                                                                                              | 通过斜率观察元数据读取频率是否正常                                      | P0 |
|                                        | {type="write"}                         | Num     | 元数据日志写入次数的计数                                                                                              | 通过斜率观察元数据写入频率是否正常                                      | P0 |
|                                        | {type="current"}                       | Num     | 元数据日志当前数量                                                                                                 | 用于监控editlog 数量。如果数量超限，需人工介入                            | P0 |
| `doris_fe_editlog_write_latency_ms`    |                                        | 毫秒      | 元数据日志写入延迟的百分位统计。如 {quantile="0.75"} 表示 75 分位的写入延迟                                                         |                                                        |
| `doris_fe_image_clean`                 | {type="failed"}                        | Num     | 清理历史元数据镜像文件失败的次数                                                                                          | 不应失败，如失败，需人工介入                                         | P0 |
|                                        | {type="success"}                       | Num     | 清理历史元数据镜像文件成功的次数                                                                                          |                                                        |
| `doris_fe_image_push`                  | {type="failed"}                        | Num     | 将元数据镜像文件推送给其他FE节点的失败的次数                                                                                   |                                                        |
|                                        | {type="success"}                       | Num     | 将元数据镜像文件推送给其他FE节点的成功的次数                                                                                   |                                                        |
| `doris_fe_image_write`                 | {type="failed"}                        | Num     | 生成元数据镜像文件失败的次数                                                                                            | 不应失败，如失败，需人工介入                                         | P0 |
|                                        | {type="success"}                       | Num     | 生成元数据镜像文件成功的次数                                                                                            |                                                        |
| `doris_fe_job`                         |                                        | Num     | 当前不同作业类型以及不同作业状态的计数。如 {job="load", type="INSERT", state="LOADING"} 表示类型为 INSERT 的导入作业，处于 LOADING 状态的作业个数  | 可以根据需要，观察不同类型的作业在集群中的数量                                | P0 |
| `doris_fe_max_journal_id`              |                                        | Num     | 当前FE节点最大元数据日志ID。如果是Master FE，则是当前写入的最大ID，如果是非Master FE，则代表当前回放的元数据日志最大ID                                  | 用于观察多个FE之间的 id 是否差距过大。过大则表示元数据同步出现问题                   | P0 |
| `doris_fe_max_tablet_compaction_score` |                                        | Num     | 所有BE节点中最大的 compaction score 值。                                                                            | 该值可以观测当前集群最大的 compaction score，以判断是否过高。如过高则可能出现查询或写入延迟 | P0 |
| `doris_fe_qps`                         |                                        | Num/Sec | 当前FE每秒查询数量（仅统计查询请求）                                                                                       | QPS                                                    | P0 |
| `doris_fe_query_err`                   |                                        | Num     | 错误查询的累积值                                                                                                  |                                                        |
| `doris_fe_query_err_rate`              |                                        | Num/Sec | 每秒错误查询数                                                                                                   | 观察集群是否出现查询错误                                           | P0 |
| `doris_fe_query_latency_ms`            |                                        | 毫秒      | 查询请求延迟的百分位统计。如 {quantile="0.75"} 表示 75 分位的查询延迟                                                            | 详细观察各分位查询延迟                                            | P0 |
| `doris_fe_query_latency_ms_db`         |                                        | 毫秒      | 各个DB的查询请求延迟的百分位统计。如 {quantile="0.75",db="test"} 表示DB test 75 分位的查询延迟                                      | 详细观察各DB各分位查询延迟                                         | P0 |
| `doris_fe_query_olap_table`            |                                        | Num     | 查询内部表（OlapTable）的请求个数统计                                                                                   |                                                        |
| `doris_fe_query_total`                 |                                        | Num     | 所有查询请求的累积计数                                                                                               |                                                        |
| `doris_fe_report_queue_size`           |                                        | Num     | BE的各种定期汇报任务在FE端的队列长度                                                                                      | 该值反映了汇报任务在 Master FE 节点上的阻塞程度，数值越大，表示FE处理能力不足          | P0 |
| `doris_fe_request_total`               |                                        | Num     | 所有通过 MySQL 端口接收的操作请求（包括查询和其他语句）                                                                           |                                                        |
| `doris_fe_routine_load_error_rows`     |                                        | Num     | 统计集群内所有 Routine Load 作业的错误行数总和                                                                            |                                                        |
| `doris_fe_routine_load_receive_bytes`  |                                        | 字节      | 统计集群内所有 Routine Load 作业接收的数据量大小                                                                           |                                                        |
| `doris_fe_routine_load_rows`           |                                        | Num     | 统计集群内所有 Routine Load 作业接收的数据行数                                                                            |                                                        |
| `doris_fe_rps`                         |                                        | Num     | 当前FE每秒请求数量（包含查询以及其他各类语句）                                                                                  | 和 QPS 配合来查看集群处理请求的量                                    | P0 |
| `doris_fe_scheduled_tablet_num`        |                                        | Num     | Master FE节点正在调度的 tablet 数量。包括正在修复的副本和正在均衡的副本                                                              | 该数值可以反映当前集群，正在迁移的 tablet 数量。如果长时间有值，说明集群不稳定            | P0 |
| `doris_fe_tablet_max_compaction_score` |                                        | Num     | 各个BE节点汇报的 compaction core。如 {backend="172.21.0.1:9556"} 表示 "172.21.0.1:9556" 这个BE的汇报值                     |                                                        |
| `doris_fe_tablet_num`                  |                                        | Num     | 各个BE节点当前tablet总数。如 {backend="172.21.0.1:9556"} 表示 "172.21.0.1:9556" 这个BE的当前tablet数量                       | 可以查看 tablet 分布是否均匀以及绝对值是否合理                            | P0 |
| `doris_fe_tablet_status_count`         |                                        | Num     | 统计 Master FE 节点 Tablet调度器所调度的 tablet 数量的累计值。                                                              |                                                        |
|                                        | {type="added"}                         | Num     | 统计 Master FE 节点 Tablet调度器所调度的 tablet 数量的累计值。 "added" 表示被调度过的 tablet 数量                                    |                                                        |
|                                        | {type="in_sched"}                      | Num     | 同上。表示被重复调度的 tablet 数量                                                                                     | 该值如果增长较快，则说明有tablet长时间处于不健康状态，导致被调度器反复调度               |
|                                        | {type="not_ready"}                     | Num     | 同上。表示尚未满足调度触发条件的tablet数量。                                                                                 | 该值如果增长较快，说明有大量 tablet 处于不健康状态但又无法被调度                   |
|                                        | {type="total"}                         | Num     | 同上。表示累积的被检查过（但不一定被调度）的tablet数量。                                                                           |                                                        |
|                                        | {type="unhealthy"}                     | Num     | 同上。表示累积的被检查过的不健康的 tablet 数量。                                                                              |                                                        |
| `doris_fe_thread_pool`                 |                                        | Num     | 统计各类线程池的工作线程数和排队情况。`"active_thread_num"` 表示正在执行的任务数。`"pool_size"` 表示线程池总线程数量。`"task_in_queue"` 表示正在排队的任务数 |                                                        |
|                                        | {name="agent-task-pool"}               | Num     | Master FE 用于发送 Agent Task 到 BE的线程池                                                                        |                                                        |
|                                        | {name="connect-scheduler-check-timer"} | Num     | 用于检查MySQL空闲连接是否超时的线程池                                                                                     |                                                        |
|                                        | {name="connect-scheduler-pool"}        | Num     | 用于接收MySQL连接请求的线程池                                                                                         |                                                        |
|                                        | {name="mysql-nio-pool"}                | Num     | NIO MySQL Server 用于处理任务的线程池                                                                               |                                                        |
|                                        | {name="export-exporting-job-pool"}     | Num     | exporting状态的export作业的调度线程池                                                                                |                                                        |
|                                        | {name="export-pending-job-pool"}       | Num     | pending状态的export作业的调度线程池                                                                                  |                                                        |
|                                        | {name="heartbeat-mgr-pool"}            | Num     | Master FE 用于处理各个节点心跳的线程池                                                                                  |                                                        |
|                                        | {name="loading-load-task-scheduler"}   | Num     | Master FE 用于调度Broker Load作业中，loading task的调度线程池                                                           |                                                        |
|                                        | {name="pending-load-task-scheduler"}   | Num     | Master FE 用于调度Broker Load作业中，pending task的调度线程池                                                           |                                                        |
|                                        | {name="schema-change-pool"}            | Num     | Master FE 用于调度 schema change 作业的线程池                                                                       |                                                        |
|                                        | {name="thrift-server-pool"}            | Num     | FE 端ThriftServer的工作线程池。对应 fe.conf 中 `rpc_port`。用于和BE进行交互。                                                 |                                                        |
| `doris_fe_txn_counter`                 |                                        | Num     | 统计各个状态的导入事务的数量的累计值                                                                                        | 可以观测导入事务的执行情况。                                         | P0 |
|                                        | {type="begin"}                         | Num     | 提交的事务数量                                                                                                   |                                                        |
|                                        | {type="failed"}                        | Num     | 失败的事务数量                                                                                                   |                                                        |
|                                        | {type="reject"}                        | Num     | 被拒绝的事务数量。（如当前运行事务数大于阈值，则新的事务会被拒绝）                                                                         |                                                        |
|                                        | {type="succes"}                        | Num     | 成功的事务数量                                                                                                   |                                                        |
| `doris_fe_txn_status`                  |                                        | Num     | 统计当前处于各个状态的导入事务的数量。如 {type="committed"} 表示处于 committed 状态的事务的数量                                           | 可以观测各个状态下导入事务的数量，来判断是否有堆积                              | P0 |
| `doris_fe_query_instance_num`          |                                        | Num     | 指定用户当前正在请求的fragment instance数目。如 {user="test_u"} 表示用户 test_u 当前正在请求的 instance 数目                          | 该数值可以用于观测指定用户是否占用过多查询资源                                | P0 |
| `doris_fe_query_instance_begin`        |                                        | Num     | 指定用户请求开始的fragment instance数目。如 {user="test_u"} 表示用户 test_u 开始请求的 instance 数目                              | 该数值可以用于观测指定用户是否提交了过多查询                                 | P0 |
| `doris_fe_query_rpc_total`             |                                        | Num     | 发往指定BE的RPC次数。如 {be="192.168.10.1"} 表示发往ip为 192.168.10.1 的BE的RPC次数                                         | 该数值可以观测是否向某个BE提交了过多RPC                                 |    |
| `doris_fe_query_rpc_failed`            |                                        | Num     | 发往指定BE的RPC失败次数。如 {be="192.168.10.1"} 表示发往ip为 192.168.10.1 的BE的RPC失败次数                                     | 该数值可以观测某个BE是否存在RPC问题                                   |    |
| `doris_fe_query_rpc_size`              |                                        | Num     | 指定BE的RPC数据大小。如 {be="192.168.10.1"} 表示发往ip为 192.168.10.1 的BE的RPC数据字节数                                      | 该数值可以观测是否向某个BE提交了过大的RPC                                |    |
| `doris_fe_txn_exec_latency_ms`         |                                        | 毫秒      | 事务执行耗时的百分位统计。如 {quantile="0.75"} 表示 75 分位的事务执行耗时                                                          | 详细观察各分位事务执行耗时                                          | P0 |
| `doris_fe_txn_publish_latency_ms`      |                                        | 毫秒      | 事务publish耗时的百分位统计。如 {quantile="0.75"} 表示 75 分位的事务publish耗时                                                | 详细观察各分位事务publish耗时                                     | P0 |
| `doris_fe_txn_num`                     |                                        | Num     | 指定DB正在执行的事务数。如 {db="test"} 表示DB test 当前正在执行的事务数                                                           | 该数值可以观测某个DB是否提交了大量事务                                   | P0 |
| `doris_fe_publish_txn_num`             |                                        | Num     | 指定DB正在publish的事务数。如 {db="test"} 表示DB test 当前正在publish的事务数                                                 | 该数值可以观测某个DB的publish事务数量                                | P0 |
| `doris_fe_txn_replica_num`             |                                        | Num     | 指定DB正在执行的事务打开的副本数。如 {db="test"} 表示DB test 当前正在执行的事务打开的副本数                                                 | 该数值可以观测某个DB是否打开了过多的副本，可能会影响其他事务执行                      | P0 |
| `doris_fe_thrift_rpc_total`            |                                        | Num     | FE thrift接口各个方法接收的RPC请求次数。如 {method="report"} 表示 report 方法接收的RPC请求次数                                      | 该数值可以观测某个thrift rpc方法的负载                               |    |
| `doris_fe_thrift_rpc_latency_ms`       |                                        | 毫秒      | FE thrift接口各个方法接收的RPC请求耗时。如 {method="report"} 表示 report 方法接收的RPC请求耗时                                      | 该数值可以观测某个thrift rpc方法的负载                               |    |
| `doris_fe_external_schema_cache`       | {catalog="hive"}                       | Num     | 指定 External Catalog 对应的 schema cache 的数量                                                                  |                                                        |    |
| `doris_fe_hive_meta_cache`             | {catalog="hive"}                       | Num     |                                                                                                           |                                                        |    |
|                                        | `{type="partition_value"}`             | Num     | 指定 External Hive Metastore Catalog 对应的 partition value cache 的数量                                          |                                                        |    |
|                                        | `{type="partition"}`                   | Num     | 指定 External Hive Metastore Catalog 对应的 partition cache 的数量                                                |                                                        |    |
|                                        | `{type="file"}`                        | Num     | 指定 External Hive Metastore Catalog 对应的 file cache 的数量                                                     |                                                        |    |

### JVM 监控

| 名称                        | 标签             | 单位  | 含义                                                    | 说明                         | 等级 |
|---------------------------|----------------|-----|-------------------------------------------------------|----------------------------|----|
| `jvm_heap_size_bytes`     |                | 字节  | JVM 内存监控。标签包含 max, used, committed，分别对应最大值，已使用和已申请的内存 | 观测JVM内存使用情况                | P0 |
| `jvm_non_heap_size_bytes` |                | 字节  | JVM 堆外内存统计                                            |                            |
| `<GarbageCollector>`      |                |     | GC 监控。                                                | GarbageCollector指代具体的垃圾收集器 | P0 |
|                           | {type="count"} | Num | GC 次数累计值                                              |                            |
|                           | {type="time"}  | 毫秒  | GC 耗时累计值                                              |                            |
| `jvm_old_size_bytes`      |                | 字节  | JVM 老年代内存统计                                           |                            | P0 |
| `jvm_thread`              |                | Num | JVM 线程数统计                                             | 观测 JVM 线程数是否合理             | P0 |
| `jvm_young_size_bytes`    |                | 字节  | JVM 新生代内存统计                                           |                            | P0 |

### 机器监控

| 名称               | 标签                          | 单位                                | 含义                                                                                                       | 说明 | 等级 |
|------------------|-----------------------------|-----------------------------------|----------------------------------------------------------------------------------------------------------|----|----|
| `system_meminfo` |                             | 字节                                | FE节点机器的内存监控。采集自 `/proc/meminfo`。包括 `buffers`，`cached`, `memory_available`, `memory_free`, `memory_total` |    |
| `system_snmp`    |                             | FE节点机器的网络监控。采集自 `/proc/net/snmp`。 |                                                                                                          |    |
|                  | `{name="tcp_in_errs"}`      | Num                               | tcp包接收错误的次数                                                                                              |    |
|                  | `{name="tcp_in_segs"}`      | Num                               | tcp包发送的个数                                                                                                |    |
|                  | `{name="tcp_out_segs"}`     | Num                               | tcp包发送的个数                                                                                                |    |
|                  | `{name="tcp_retrans_segs"}` | Num                               | tcp包重传的个数                                                                                                |    |

## BE 监控指标

### 进程监控

| 名称                                                | 标签                                          | 单位   | 含义                                                                           | 说明                                                                  | 等级 |
|---------------------------------------------------|---------------------------------------------|------|------------------------------------------------------------------------------|---------------------------------------------------------------------|----|
| `doris_be_active_scan_context_count`              |                                             | Num  | 展示当前由外部直接打开的scanner的个数                                                       |                                                                     |
| `doris_be_add_batch_task_queue_size`              |                                             | Num  | 记录导入时，接收batch的线程池的队列大小                                                       | 如果大于0，则表示导入任务的接收端出现积压                                               | P0 |
| `agent_task_queue_size`                           |                                             | Num  | 展示各个 Agent Task 处理队列的长度，如 `{type="CREATE_TABLE"}` 表示 CREATE_TABLE 任务队列的长度    |                                                                     |
| `doris_be_brpc_endpoint_stub_count`               |                                             | Num  | 已创建的 brpc stub 的数量，这些 stub 用于 BE 之间的交互                                       |                                                                     |
| `doris_be_brpc_function_endpoint_stub_count`      |                                             | Num  | 已创建的 brpc stub 的数量，这些 stub 用于和 Remote RPC 之间交互                               |                                                                     |
| `doris_be_cache_capacity`                         |                                             |      | 记录指定 LRU Cache 的容量                                                           |                                                                     |
| `doris_be_cache_usage`                            |                                             |      | 记录指定 LRU Cache 的使用量                                                          | 用于观测内存占用情况                                                          | P0 |
| `doris_be_cache_usage_ratio`                      |                                             |      | 记录指定 LRU Cache 的使用率                                                          |                                                                     |
| `doris_be_cache_lookup_count`                     |                                             |      | 记录指定 LRU Cache 被查找的次数                                                        |                                                                     |
| `doris_be_cache_hit_count`                        |                                             |      | 记录指定 LRU Cache 的命中次数                                                         |                                                                     |
| `doris_be_cache_hit_ratio`                        |                                             |      | 记录指定 LRU Cache 的命中率                                                          | 用于观测cache是否有效                                                       | P0 |
|                                                   | {name="DataPageCache"}                      | Num  | DataPageCache 用于缓存数据的 Data Page                                              | 数据Cache，直接影响查询效率                                                    | P0 |
|                                                   | {name="IndexPageCache"}                     | Num  | IndexPageCache 用于缓存数据的 Index Page                                            | 索引Cache，直接影响查询效率                                                    | P0 |
|                                                   | {name="LastSuccessChannelCache"}         | Num  | LastSuccessChannelCache 用于缓存导入接收端的 LoadChannel                            |                                                                     |
|                                                   | {name="SegmentCache"}                       | Num  | SegmentCache 用于缓存已打开的 Segment，如索引信息                                          |                                                                     |
| `doris_be_chunk_pool_local_core_alloc_count`      |                                             | Num  | ChunkAllocator中，从绑定的 core 的内存队列中分配内存的次数                                      |                                                                     |
| `doris_be_chunk_pool_other_core_alloc_count`      |                                             | Num  | ChunkAllocator中，从其他的 core 的内存队列中分配内存的次数                                      |                                                                     |
| `doris_be_chunk_pool_reserved_bytes`              |                                             | 字节   | ChunkAllocator 中预留的内存大小                                                      |                                                                     |
| `doris_be_chunk_pool_system_alloc_cost_ns`        |                                             | 纳秒   | SystemAllocator 申请内存的耗时累计值                                                   | 通过斜率可以观测内存分配的耗时                                                     | P0 |
| `doris_be_chunk_pool_system_alloc_count`          |                                             | Num  | SystemAllocator 申请内存的次数                                                      |                                                                     |
| `doris_be_chunk_pool_system_free_cost_ns`         |                                             | 纳秒   | SystemAllocator 释放内存的耗时累计值                                                   | 通过斜率可以观测内存释放的耗时                                                     | P0 |
| `doris_be_chunk_pool_system_free_count`           |                                             | Num  | SystemAllocator 释放内存的次数                                                      |                                                                     |
| `doris_be_compaction_bytes_total`                 |                                             | 字节   | compaction处理的数据量的累计值                                                         | 记录的是 compaction 任务中，input rowset 的 disk size。通过斜率可以观测 compaction的速率 | P0 |
|                                                   | {type="base"}                               | 字节   | Base Compaction 的数据量累计                                                       |                                                                     |
|                                                   | {type="cumulative"}                         | 字节   | Cumulative Compaction 的数据量累计                                                 |                                                                     |
| `doris_be_compaction_deltas_total`                |                                             | Num  | compaction处理的 rowset 个数的累计值                                                  | 记录的是 compaction 任务中，input rowset 的 个数                               |
|                                                   | {type="base"}                               | Num  | Base Compaction 处理的 rowset 个数累计                                              |                                                                     |
|                                                   | {type="cumulative"}                         | Num  | Cumulative Compaction 处理的 rowset 个数累计                                        |                                                                     |
| `doris_be_disks_compaction_num`                   |                                             | Num  | 指定数据目录上正在执行的 compaction 任务数。如 `{path="/path1/"}` 表示`/path1` 目录上正在执行的任务数      | 用于观测各个磁盘上的 compaction 任务数是否合理                                       | P0 |
| `doris_be_disks_compaction_score`                 |                                             | Num  | 指定数据目录上正在执行的 compaction 令牌数。如 `{path="/path1/"}` 表示`/path1` 目录上正在执行的令牌数      |                                                                     |
| `doris_be_compaction_used_permits`                |                                             | Num  | Compaction 任务已使用的令牌数量                                                        | 用于反映Compaction的资源消耗量                                                |
| `doris_be_compaction_waitting_permits`            |                                             | Num  | 正在等待Compaction令牌的数量                                                          |                                                                     |
| `doris_be_data_stream_receiver_count`             |                                             | Num  | 数据接收端 Receiver 的数量                                                           | FIXME：向量化引擎此指标缺失                                                    |
| `doris_be_disks_avail_capacity`                   |                                             | 字节   | 指定数据目录所在磁盘的剩余空间。如 `{path="/path1/"}` 表示 `/path1` 目录所在磁盘的剩余空间                 |                                                                     | P0 |
| `doris_be_disks_local_used_capacity`              |                                             | 字节   | 指定数据目录所在磁盘的本地已使用空间                                                           |                                                                     |
| `doris_be_disks_remote_used_capacity`             |                                             | 字节   | 指定数据目录所在磁盘的对应的远端目录的已使用空间                                                     |                                                                     |
| `doris_be_disks_state`                            |                                             | 布尔   | 指定数据目录的磁盘状态。1 表示正常。0 表示异常                                                    |                                                                     |
| `doris_be_disks_total_capacity`                   |                                             | 字节   | 定数据目录所在磁盘的总容量                                                                | 配合 `doris_be_disks_avail_capacity` 计算磁盘使用率                          | P0 |
| `doris_be_engine_requests_total`                  |                                             | Num  | BE 上各类任务执行状态的累计值                                                             |                                                                     |
|                                                   | {status="failed",type="xxx"}                | Num  | xxx 类型的任务的失败次数的累计值                                                           |                                                                     |
|                                                   | {status="total",type="xxx"}                 | Num  | xxx 类型的任务的总次数的累计值。                                                           | 可以按需监控各类任务的失败次数                                                     | P0 |
|                                                   | `{status="skip",type="report_all_tablets"}` | Num  | xxx 类型任务被跳过执行的次数的累计值                                                         |                                                                     |
| `doris_be_fragment_endpoint_count`                |                                             | Num  | 同                                                                            | FIXME: 同 `doris_be_data_stream_receiver_count` 数目。并且向量化引擎缺失         |
| `doris_be_fragment_request_duration_us`           |                                             | 微秒   | 所有 fragment intance 的执行时间累计                                                  | 通过斜率观测 instance 的执行耗时                                               | P0 |
| `doris_be_fragment_requests_total`                |                                             | Num  | 执行过的 fragment instance 的数量累计                                                 |                                                                     |
| `doris_be_load_channel_count`                     |                                             | Num  | 当前打开的 load channel 个数                                                        | 数值越大，说明当前正在执行的导入任务越多                                                | P0 |
| `doris_be_local_bytes_read_total`                 |                                             | 字节   | 由 `LocalFileReader` 读取的字节数                                                   |                                                                     | P0 |
| `doris_be_local_bytes_written_total`              |                                             | 字节   | 由 `LocalFileWriter` 写入的字节数                                                   |                                                                     | P0 |
| `doris_be_local_file_reader_total`                |                                             | Num  | 打开的 `LocalFileReader` 的累计计数                                                  |                                                                     |
| `doris_be_local_file_open_reading`                |                                             | Num  | 当前打开的 `LocalFileReader` 个数                                                   |                                                                     |
| `doris_be_local_file_writer_total`                |                                             | Num  | 打开的 `LocalFileWriter` 的累计计数。                                                 |                                                                     |
| `doris_be_mem_consumption`                        |                                             | 字节   | 指定模块的当前内存开销。如 {type="compaction"} 表示 compaction 模块的当前总内存开销。                  | 值取自相同 type 的 MemTracker。FIXME                                       |
| `doris_be_memory_allocated_bytes`                 |                                             | 字节   | BE 进程物理内存大小，取自 `/proc/self/status/VmRSS`                                     |                                                                     | P0 |
| `doris_be_memory_jemalloc`                        |                                             | 字节   | Jemalloc stats, 取自 `je_mallctl`。                                             | 含义参考：https://jemalloc.net/jemalloc.3.html                           | P0 |
| `doris_be_memory_pool_bytes_total`                |                                             | 字节   | 所有 MemPool 当前占用的内存大小。统计值，不代表真实内存使用。                                          |                                                                     |
| `doris_be_memtable_flush_duration_us`             |                                             | 微秒   | memtable写入磁盘的耗时累计值                                                           | 通过斜率可以观测写入延迟                                                        | P0 |
| `doris_be_memtable_flush_total`                   |                                             | Num  | memtable写入磁盘的个数累计值                                                           | 通过斜率可以计算写入文件的频率                                                     | P0 |
| `doris_be_meta_request_duration`                  |                                             | 微秒   | 访问 RocksDB 中的 meta 的耗时累计                                                     | 通过斜率观测 BE 元数据读写延迟                                                   | P0 |
|                                                   | {type="read"}                               | 微秒   | 读取耗时                                                                         |                                                                     |
|                                                   | {type="write"}                              | 微秒   | 写入耗时                                                                         |                                                                     |
| `doris_be_meta_request_total`                     |                                             | Num  | 访问 RocksDB 中的 meta 的次数累计                                                     | 通过斜率观测 BE 元数据访问频率                                                   | P0 |
|                                                   | {type="read"}                               | Num  | 读取次数                                                                         |                                                                     |
|                                                   | {type="write"}                              | Num  | 写入次数                                                                         |                                                                     |
| `doris_be_fragment_instance_count`                |                                             | Num  | 当前已接收的 fragment instance 的数量                                                 | 观测是否出现 instance 堆积                                                  | P0 |
| `doris_be_process_fd_num_limit_hard`              |                                             | Num  | BE 进程的文件句柄数硬限。通过 `/proc/pid/limits` 采集                                       |                                                                     |
| `doris_be_process_fd_num_limit_soft`              |                                             | Num  | BE 进程的文件句柄数软限。通过 `/proc/pid/limits` 采集                                       |                                                                     |
| `doris_be_process_fd_num_used`                    |                                             | Num  | BE 进程已使用的文件句柄数。通过 `/proc/pid/limits` 采集                                      |                                                                     |
| `doris_be_process_thread_num`                     |                                             | Num  | BE 进程线程数。通过 `/proc/pid/task` 采集                                              |                                                                     | P0 |
| `doris_be_query_cache_memory_total_byte`          |                                             | 字节   | Query Cache 占用字节数                                                            |                                                                     |
| `doris_be_query_cache_partition_total_count`      |                                             | Num  | 当前 Partition Cache 缓存个数                                                      |                                                                     |
| `doris_be_query_cache_sql_total_count`            |                                             | Num  | 当前 SQL Cache 缓存个数                                                            |                                                                     |
| `doris_be_query_scan_bytes`                       |                                             | 字节   | 读取数据量的累计值。这里只统计读取 Olap 表的数据量                                                 |                                                                     |
| `doris_be_query_scan_bytes_per_second`            |                                             | 字节/秒 | 根据 `doris_be_query_scan_bytes` 计算得出的读取速率                                     | 观测查询速率                                                              | P0 |
| `doris_be_query_scan_rows`                        |                                             | Num  | 读取行数的累计值。这里只统计读取 Olap 表的数据量。并且是 RawRowsRead（部分数据行可能被索引跳过，并没有真正读取，但仍会记录到这个值中） | 通过斜率观测查询速率                                                          | P0 |
| `doris_be_result_block_queue_count`               |                                             | Num  | 当前查询结果缓存中的 fragment instance 个数                                              | 该队列仅用于被外部系统直接读取时使用。如 Spark on Doris 通过 external scan 查询数据           |
| `doris_be_result_buffer_block_count`              |                                             | Num  | 当前查询结果缓存中的 query 个数                                                          | 该数值反映当前 BE 中有多少查询的结果正在等待 FE 消费                                      | P0 |
| `doris_be_routine_load_task_count`                |                                             | Num  | 当前正在执行的 routine load task 个数                                                 |                                                                     |
| `doris_be_rowset_count_generated_and_in_use`      |                                             | Num  | 自上次启动后，新增的并且正在使用的 rowset id 个数。                                              |                                                                     |
| `doris_be_s3_bytes_read_total`                    |                                             | Num  | `S3FileReader` 的打开累计次数                                                       |                                                                     |
| `doris_be_s3_file_open_reading`                   |                                             | Num  | 当前打开的 `S3FileReader` 个数                                                      |                                                                     |
| `doris_be_s3_bytes_read_total`                    |                                             | 字节   | `S3FileReader` 读取字节数累计值                                                      |                                                                     |
| `doris_be_scanner_thread_pool_queue_size`         |                                             | Num  | 用于 OlapScanner 的线程池的当前排队数量                                                   | 大于零则表示 Scanner 开始堆积                                                 | P0 |
| `doris_be_segment_read`                           | `{type="segment_read_total"}`               | Num  | 读取的segment的个数累计值                                                             |                                                                     |
| `doris_be_segment_read`                           | `{type="segment_row_total"}`                | Num  | 读取的segment的行数累计值                                                             | 该数值也包含了被索引过滤的行数。相当于读取的segment个数 * 每个segment的总行数                     |
| `doris_be_send_batch_thread_pool_queue_size`      |                                             | Num  | 导入时用于发送数据包的线程池的排队个数                                                          | 大于0则表示有堆积                                                           | P0 |
| `doris_be_send_batch_thread_pool_thread_num`      |                                             | Num  | 导入时用于发送数据包的线程池的线程数                                                           |                                                                     |
| `doris_be_small_file_cache_count`                 |                                             | Num  | 当前BE缓存的小文件数量                                                                 |                                                                     |
| `doris_be_streaming_load_current_processing`      |                                             | Num  | 当前正在运行的 stream load 任务数                                                      | 仅包含 curl 命令发送的任务                                                    |
| `doris_be_streaming_load_duration_ms`             |                                             | 毫秒   | 所有stream load 任务执行时间的耗时累计值                                                   |                                                                     |
| `doris_be_streaming_load_requests_total`          |                                             | Num  | stream load 任务数的累计值                                                          | 通过斜率可观测任务提交频率                                                       | P0 |
| `doris_be_stream_load_pipe_count`                 |                                             | Num  | 当前 stream load 数据管道的个数                                                       | 包括 stream load 和 routine load 任务                                    |
| `doris_be_stream_load`                            | {type="load_rows"}                          | Num  | stream load 最终导入的行数累计值                                                       | 包括 stream load 和 routine load 任务                                    | P0 |
| `doris_be_stream_load`                            | {type="receive_bytes"}                      | 字节   | stream load 接收的字节数累计值                                                        | 包括 stream load 从 http 接收的数据，以及 routine load 从kafka 读取的数据            | P0 |
| `doris_be_tablet_base_max_compaction_score`       |                                             | Num  | 当前最大的 Base Compaction Score                                                  | 该数值实时变化，有可能丢失峰值数据。数值越高，表示 compaction 堆积越严重                          | P0 |
| `doris_be_tablet_cumulative_max_compaction_score` |                                             | Num  | 同上。当前最大的 Cumulative Compaction Score                                         |                                                                     |
| `doris_be_tablet_version_num_distribution`        |                                             | Num  | tablet version 数量的直方。                                                        | 用于反映 tablet version 数量的分布                                           | P0 |
| `doris_be_thrift_connections_total`               |                                             | Num  | 创建过的 thrift 连接数的累计值。如 `{name="heartbeat"}` 表示心跳服务的连接数累计                      | 此数值为 BE 作为服务端的 thrift server 的连接                                    |
| `doris_be_thrift_current_connections`             |                                             | Num  | 当前 thrift 连接数。如 `{name="heartbeat"}` 表示心跳服务的当前连接数。                           | 同上                                                                  |
| `doris_be_thrift_opened_clients`                  |                                             | Num  | 当前已打开的 thrift 客户端的数量。如 `{name="frontend"}` 表示访问 FE 服务的客户端数量                  |                                                                     |
| `doris_be_thrift_used_clients`                    |                                             | Num  | 当前正在使用的 thrift 客户端的数量。如 `{name="frontend"}` 表示正在用于访问 FE 服务的客户端数量             |                                                                     |
| `doris_be_timeout_canceled_fragment_count`        |                                             | Num  | 因超时而被取消的 fragment instance 数量累计值                                             | 这个值可能会被重复记录。比如部分 fragment instance 被多次取消                            | P0 |
| `doris_be_stream_load_txn_request`                | {type="begin"}                              | Num  | stream load 开始事务数的累计值                                                        | 包括 stream load 和 routine load 任务                                    |
| `doris_be_stream_load_txn_request `               | {type="commit"}                             | Num  | stream load 执行成功的事务数的累计值                                                     | 同上                                                                  |
| `doris_be_stream_load_txn_request `               | {type="rollback"}                           |      | stream load 执行失败的事务数的累计值                                                     | 同上                                                                  |
| `doris_be_unused_rowsets_count`                   |                                             | Num  | 当前已废弃的rowset的个数                                                              | 这些rowset正常情况下会被定期删除                                                 |
| `doris_be_upload_fail_count`                      |                                             | Num  | 冷热分层功能，上传到远端存储失败的rowset的次数累计值                                                |                                                                     |
| `doris_be_upload_rowset_count`                    |                                             | Num  | 冷热分层功能，上传到远端存储成功的rowset的次数累计值                                                |                                                                     |
| `doris_be_upload_total_byte`                      |                                             |      | 字节                                                                           | 冷热分层功能，上传到远端存储成功的rowset数据量累计值                                       |    |
| `doris_be_load_bytes`                             |                                             | 字节   | 通过 tablet sink 发送的数量累计                                                       | 可观测导入数据量                                                            | P0 |
| `doris_be_load_rows`                              |                                             | Num  | 通过 tablet sink 发送的行数累计                                                       | 可观测导入数据量                                                            | P0 |
| `fragment_thread_pool_queue_size`                 |                                             | Num  | 当前查询执行线程池等待队列的长度                                                             | 如果大于零，则说明查询线程已耗尽，查询会出现堆积                                            | P0 |
| `doris_be_all_rowsets_num`                        |                                             | Num  | 当前所有 rowset 的个数                                                              |                                                                     | P0 |
| `doris_be_all_segments_num`                       |                                             | Num  | 当前所有 segment 的个数                                                             |                                                                     | P0 |
| `doris_be_heavy_work_max_threads`                 |                                             | Num  | brpc heavy线程池线程个数                                                            |                                                                     | p0 |
| `doris_be_light_work_max_threads`                 |                                             | Num  | brpc light线程池线程个数                                                            |                                                                     | p0 | 
| `doris_be_heavy_work_pool_queue_size`             |                                             | Num  | brpc heavy线程池队列最大长度,超过则阻塞提交work                                              |                                                                     | p0 |
| `doris_be_light_work_pool_queue_size`             |                                             | Num  | brpc light线程池队列最大长度,超过则阻塞提交work                                              |                                                                     | p0 |
| `doris_be_heavy_work_active_threads`              |                                             | Num  | brpc heavy线程池活跃线程数                                                           |                                                                     | p0 |
| `doris_be_light_work_active_threads`              |                                             | Num  | brpc light线程池活跃线程数                                                           |                                                                     | p0 |

### 机器监控

| 名称                                        | 标签                       | 单位   | 含义                                                                                            | 说明                                       | 等级 |
|-------------------------------------------|--------------------------|------|-----------------------------------------------------------------------------------------------|------------------------------------------|----|
| `doris_be_cpu`                            |                          | Num  | CPU 相关监控指标，从 `/proc/stat` 采集。会分别采集每个逻辑核的各项数值。如 `{device="cpu0",mode="nice"}` 表示 cpu0 的 nice 值 | 可计算得出 CPU 使用率                            | P0 |
| `doris_be_disk_bytes_read`                |                          | 字节   | 磁盘读取量累计值。从 `/proc/diskstats` 采集。会分别采集每块磁盘的数值。如 `{device="vdd"}` 表示 vvd 盘的数值                   |                                          |    |
| `doris_be_disk_bytes_written`             |                          | 字节   | 磁盘写入量累计值。采集方式同上                                                                               |                                          |    |
| `doris_be_disk_io_time_ms`                |                          | 字节   | 采集方式同上                                                                                        | 可计算得出 IO Util                            | P0 |
| `doris_be_disk_io_time_weighted`          |                          | 字节   | 采集方式同上                                                                                        |                                          |    |
| `doris_be_disk_reads_completed`           |                          | 字节   | 采集方式同上                                                                                        |                                          |    |
| `doris_be_disk_read_time_ms`              |                          | 字节   | 采集方式同上                                                                                        |                                          |    |
| `doris_be_disk_writes_completed`          |                          | 字节   | 采集方式同上                                                                                        |                                          |    |
| `doris_be_disk_write_time_ms`             |                          | 字节   | 采集方式同上                                                                                        |                                          |    |
| `doris_be_fd_num_limit`                   |                          | Num  | 系统文件句柄限制上限。从 `/proc/sys/fs/file-nr` 采集                                                        |                                          |    |
| `doris_be_fd_num_used`                    |                          | Num  | 系统已使用文件句柄数。 从 `/proc/sys/fs/file-nr` 采集                                                       |                                          |    |
| `doris_be_file_created_total`             |                          | Num  | 本地文件创建次数累计                                                                                    | 所有调用 `local_file_writer` 并最终 close 的文件计数 |    |
| `doris_be_load_average`                   |                          | Num  | 机器 Load Avg 指标监控。如 {mode="15_minutes"} 为 15 分钟 Load Avg                                       | 观测整机负载                                   | P0 |
| `doris_be_max_disk_io_util_percent`       |                          | 百分比  | 计算得出的所有磁盘中，最大的 IO UTIL 的磁盘的数值                                                                 |                                          | P0 |
| `doris_be_max_network_receive_bytes_rate` |                          | 字节/秒 | 计算得出的所有网卡中，最大的接收速率                                                                            |                                          | P0 |
| `doris_be_max_network_send_bytes_rate`    |                          | 字节/秒 | 计算得出的所有网卡中，最大的发送速率                                                                            |                                          | P0 |
| `doris_be_memory_pgpgin`                  |                          | 字节   | 系统从磁盘写到内存页的数据量                                                                                |                                          |    |
| `doris_be_memory_pgpgout`                 |                          | 字节   | 系统内存页写入磁盘的数据量                                                                                 |                                          |    |
| `doris_be_memory_pswpin`                  |                          | 字节   | 系统从磁盘换入到内存的数量                                                                                 | 通常情况下，swap应该关闭，因此这个数值应该是0                |    |
| `doris_be_memory_pswpout`                 |                          | 字节   | 系统从内存换入到磁盘的数量                                                                                 | 通常情况下，swap应该关闭，因此这个数值应该是0                |    |
| `doris_be_network_receive_bytes`          |                          | 字节   | 各个网卡的接收字节累计。采集自 `/proc/net/dev`                                                               |                                          |    |
| `doris_be_network_receive_packets`        |                          | Num  | 各个网卡的接收包个数累计。采集自 `/proc/net/dev`                                                              |                                          |    |
| `doris_be_network_send_bytes`             |                          | 字节   | 各个网卡的发送字节累计。采集自 `/proc/net/dev`                                                               |                                          |    |
| `doris_be_network_send_packets`           |                          | Num  | 各个网卡的发送包个数累计。采集自 `/proc/net/dev`                                                              |                                          |    |
| `doris_be_proc`                           | `{mode="ctxt_switch"}`   | Num  | CPU 上下文切换的累计值。采集自 `/proc/stat`                                                                | 观测是否有异常的上下文切换                            | P0 |
| `doris_be_proc `                          | `{mode="interrupt"}`     | Num  | CPU 中断次数的累计值。采集自 `/proc/stat`                                                                 |                                          |    |
| `doris_be_proc`                           | `{mode="procs_blocked"}` | Num  | 系统当前被阻塞的进程数（如等待IO）。采集自 `/proc/stat`                                                           |                                          |    |
| `doris_be_proc`                           | `{mode="procs_running"}` | Num  | 系统当前正在执行的进程数。采集自 `/proc/stat`                                                                 |                                          |    |
| `doris_be_snmp_tcp_in_errs`               |                          | Num  | tcp包接收错误的次数。采集自 `/proc/net/snmp`                                                              | 可观测网络错误如重传、丢包等。需和其他 snmp 指标配合使用          | P0 |
| `doris_be_snmp_tcp_in_segs`               |                          | Num  | tcp包发送的个数。 采集自 `/proc/net/snmp`                                                               |                                          |    |
| `doris_be_snmp_tcp_out_segs`              |                          | Num  | tcp包发送的个数。采集自 `/proc/net/snmp`                                                                |                                          |    |
| `doris_be_snmp_tcp_retrans_segs`          |                          | Num  | tcp包重传的个数。采集自 `/proc/net/snmp`                                                                |                                          |    |
