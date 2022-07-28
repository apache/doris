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
2. 节点监控：主要展示 Doris 进程所在节点机器本身的监控，如 CPU、内存、IO等等。

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

## FE 监控指标

### 进程监控

|名称| 标签 |单位 | 含义 | 说明 |
|---|---|---|---|---|
|`doris_fe_cache_added`|{type="partition"}| Num | 新增的 Partition Cache 数量 | |
||{type="sql"}|Number| 新增的 SQL Cache 数量 | |
|`doris_fe_cache_hit`|{type="partition"}| Num | 命中 Partition Cache 的计数 | |
||{type="sql"}| Num | 命中 SQL Cache 的计数 | |
|`doris_fe_connection_total`| | Num| 当前FE的MySQL端口连接数 | |
|`doris_fe_counter_hit_sql_block_rule`|| Num| 被 SQL BLOCK RULE 拦截的查询数量 |  | |
|`doris_fe_edit_log_clean`| {type="failed"} | Num| 清理历史元数据日志失败的次数 | |
|| {type="success"} | Num| 清理历史元数据日志成功的次数 | |
|`doris_fe_edit_log`| {type="bytes"} |字节 | 元数据日志写入量的累计值 | |
|| {type="read"} |Num| 元数据日志读取次数的计数 | |
|| {type="write"} |Num | 元数据日志写入次数的计数 | |
|`doris_fe_editlog_write_latency_ms`| | 毫秒| 元数据日志写入延迟的百分位统计。如 {quantile="0.75"} 表示 75 分位的写入延迟 | |
|`doris_fe_image_clean`|{type="failed"} | Num | 清理历史元数据镜像文件失败的次数 | |
||{type="success"} | Num | 清理历史元数据镜像文件成功的次数 | |
|`doris_fe_image_push`|{type="failed"} | Num | 将元数据镜像文件推送给其他FE节点的失败的次数 | |
||{type="success"} | Num | 将元数据镜像文件推送给其他FE节点的成功的次数 | |
|`doris_fe_image_write`|{type="failed"} | Num | 生成元数据镜像文件失败的次数 | |
||{type="success"} | Num | 生成元数据镜像文件成功的次数 | |
|`doris_fe_job`| | Num | 当前不同作业类型以及不同作业状态的计数。如 {job="load", type="INSERT", state="LOADING"} 表示类型为 INSERT 的导入作业，处于 LOADING 状态的作业个数| |
|`doris_fe_max_journal_id`| | Num | 当前FE节点最大元数据日志ID。如果是Master FE，则是当前写入的最大ID，如果是非Master FE，则代表当前回放的元数据日志最大ID | |
|`doris_fe_max_tablet_compaction_score`| | Num| 所有BE节点中最大的 compaction score 值。  | 该值可以观测当前集群最大的 compaction score，以判断是否过高。 |
|`doris_fe_qps`| | Num/Sec  | 当前FE每秒查询数量（仅统计查询请求）| |
|`doris_fe_query_err`| | Num | 错误查询的累积值 | |
|`doris_fe_query_err_rate`|  | Num/Sec| 每秒错误查询数  | |
|`doris_fe_query_latency_ms`| | 毫秒| 查询请求延迟的百分位统计。如 {quantile="0.75"} 表示 75 分位的查询延迟 | |
|`doris_fe_query_olap_table`| | Num| 查询内部表（OlapTable）的请求个数统计 | |
|`doris_fe_query_total`| | Num | 所有查询请求的累积计数 | |
|`doris_fe_report_queue_size`| | Num | BE的各种定期汇报任务在FE端的队列长度 | 该值反映了汇报任务在 Master FE 节点上的阻塞程度，数值越大，表示FE处理能力不足 |
|`doris_fe_request_total`| | Num | 所有通过 MySQL 端口接收的操作请求（包括查询和其他语句）| |
|`doris_fe_routine_load_error_rows`| | Num | 统计集群内所有 Routine Load 作业的错误行数总和 | |
|`doris_fe_routine_load_receive_bytes`| | 字节 | 统计集群内所有 Routine Load 作业接收的数据量大小 | |
|`doris_fe_routine_load_rows`| | Num | 统计集群内所有 Routine Load 作业接收的数据行数 | |
|`doris_fe_rps`| | Num | 当前FE每秒请求数量（包含查询以及其他各类语句） | |
|`doris_fe_scheduled_tablet_num`| | Num | Master FE节点正在调度的 tablet 数量。包括正在修复的副本和正在均衡的副本 | 该数值可以反映当前集群，正在迁移的 tablet 数量 |
|`doris_fe_tablet_max_compaction_score`| | Num | 各个BE节点汇报的 compaction core。如 {backend="172.21.0.1:9556"} 表示 "172.21.0.1:9556" 这个BE的汇报值| |
|`doris_fe_tablet_num`| | Num | 各个BE节点当前tablet总数。如 {backend="172.21.0.1:9556"} 表示 "172.21.0.1:9556" 这个BE的当前tablet数量 | |
|`doris_fe_tablet_status_count`| |Num| 统计 Master FE 节点 Tablet调度器所调度的 tablet 数量的累计值。| |
| | {type="added"} |Num| 统计 Master FE 节点 Tablet调度器所调度的 tablet 数量的累计值。 "added" 表示被调度过的 tablet 数量 | |
|| {type="in_sched"} |Num| 同上。表示被重复调度的 tablet 数量 |该值如果增长较快，则说明有tablet长时间处于不健康状态，导致被调度器反复调度 |
|| {type="not_ready"} | Num |同上。表示尚未满足调度触发条件的tablet数量。| 该值如果增长较快，说明有大量 tablet 处于不健康状态但又无法被调度 |
|| {type="total"} |Num |同上。表示累积的被检查过（但不一定被调度）的tablet数量。| |
|| {type="unhealthy"}|  Num| 同上。表示累积的被检查过的不健康的 tablet 数量。| |
|`doris_fe_thread_pool`| | Num | 统计各类线程池的工作线程数和排队情况。"active_thread_num" 表示正在执行的任务数。"pool_size" 表示线程池总线程数量。"task_in_queue" 表示正在排队的任务数| |
|| {name="agent-task-pool"} | Num | Master FE 用于发送 Agent Task 到 BE的线程池 | |
|| {name="connect-scheduler-check-timer"} | Num | 用于检查MySQL空闲连接是否超时的线程池 | |
|| {name="connect-scheduler-pool"} | Num | 用于接收MySQL连接请求的线程池 | |
|| {name="mysql-nio-pool"} | Num | NIO MySQL Server 用于处理任务的线程池 | |
|| {name="export-exporting-job-pool"} | Num | exporting状态的export作业的调度线程池 | |
|| {name="export-pending-job-pool"} | Num | pending状态的export作业的调度线程池| |
|| {name="heartbeat-mgr-pool"} | Num | Master FE 用于处理各个节点心跳的线程池| |
|| {name="loading-load-task-scheduler"} | Num | Master FE 用于调度Broker Load作业中，loading task的调度线程池| |
|| {name="pending-load-task-scheduler"} | Num | Master FE 用于调度Broker Load作业中，pending task的调度线程池| |
|| {name="schema-change-pool"} | Num | Master FE 用于调度 schema change 作业的线程池 | |
|| {name="thrift-server-pool"} | Num | | FE 端ThriftServer的工作线程池。对应 fe.conf 中 `rpc_port`。用于和BE进行交互。 |
|`doris_fe_txn_counter`| | Num | 统计各个状态的导入事务的数量的累计值| |
|| {type="begin"}| Num| 提交的事务数量 | |
||{type="failed"} | Num| 失败的事务数量| |
|| {type="reject"} | Num| 被拒绝的事务数量。（如当前运行事务数大于阈值，则新的事务会被拒绝）| |
|| {type="succes"} | Num| 成功的事务数量| |
|`doris_fe_txn_status`|  | Num | 统计当前处于各个状态的导入事务的数量。如 {type="committed"} 表示处于 committed 状态的事务的数量 | |

### JVM 监控

|名称| 标签 |单位 | 含义 | 说明 |
|---|---|---|---|---|
|`jvm_direct_buffer_pool_size_bytes`| | | | |
|`jvm_heap_size_bytes`| | 字节 | JVM 内存监控。标签包含 max, used, committed，分别对应最大值，已使用和已申请的内存 | |
|`jvm_non_heap_size_bytes`| | 字节 | JVM 对外内存统计| |
|`jvm_old_gc`| | | 老年代 GC 监控。| |
| |{type="count"}  | Num | 老年代 GC 次数累计值| |
| |{type="time"}  | 毫秒 | 老年代 GC 耗时累计值| |
|`jvm_old_size_bytes`| | 字节| JVM 老年代内存统计 | |
|`jvm_thread`| | Num| JVM 线程数统计 | |
|`jvm_young_gc`| | |新生代 GC 监控。 | |
| |{type="count"}  | Num | 新生代 GC 次数累计值| |
| |{type="time"}  | 毫秒 | 新生代 GC 耗时累计值| |
|`jvm_young_size_bytes`| | 字节|JVM 新生代内存统计 | |

### 机器监控

|名称| 标签 |单位 | 含义 | 说明 |
|---|---|---|---|---|
|`system_meminfo`| | 字节| FE节点机器的内存监控。采集自 `/proc/meminfo`。包括 `buffers`，`cached`, `memory_available`, `memory_free`, `memory_total` | |
|`system_snmp`| | FE节点机器的网络监控。采集自 `/proc/net/snmp`。 | | |
||`{name="tcp_in_errs"}` | Num| tcp包接收错误的次数| |
||`{name="tcp_in_segs"}` | Num | tcp包发送的个数 | |
|| `{name="tcp_out_segs"}`| Num | tcp包发送的个数 | |
||`{name="tcp_retrans_segs"}` | Num | tcp包重传的个数 | |

## BE 监控指标

TODO