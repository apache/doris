---
{
    "title": "FE 配置项",
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

# Doris FE配置参数

该文档主要介绍 FE 的相关配置项。

FE 的配置文件 `fe.conf` 通常存放在 FE 部署路径的 `conf/` 目录下。 而在 0.14 版本中会引入另一个配置文件 `fe_custom.conf`。该配置文件用于记录用户在运行是动态配置并持久化的配置项。

FE 进程启动后，会先读取 `fe.conf` 中的配置项，之后再读取 `fe_custom.conf` 中的配置项。`fe_custom.conf` 中的配置项会覆盖 `fe.conf` 中相同的配置项。

`fe_custom.conf` 文件的位置可以在 `fe.conf` 通过 `custom_config_dir` 配置项配置。

## 查看配置项

FE 的配置项有两种方式进行查看：

1. FE 前端页面查看

   在浏览器中打开 FE 前端页面 `http://fe_host:fe_http_port/variable`。在 `Configure Info` 中可以看到当前生效的 FE 配置项。

2. 通过命令查看

   FE 启动后，可以在 MySQL 客户端中，通过以下命令查看 FE 的配置项：

   `ADMIN SHOW FRONTEND CONFIG;`

   结果中各列含义如下：

   - Key：配置项名称。
   - Value：当前配置项的值。
   - Type：配置项值类型，如果整型、字符串。
   - IsMutable：是否可以动态配置。如果为 true，表示该配置项可以在运行时进行动态配置。如果false，则表示该配置项只能在 `fe.conf` 中配置并且重启 FE 后生效。
   - MasterOnly：是否为 Master FE 节点独有的配置项。如果为 true，则表示该配置项仅在 Master FE 节点有意义，对其他类型的 FE 节点无意义。如果为 false，则表示该配置项在所有 FE 节点中均有意义。
   - Comment：配置项的描述。

## 设置配置项

FE 的配置项有两种方式进行配置：

1. 静态配置

   在 `conf/fe.conf` 文件中添加和设置配置项。`fe.conf` 中的配置项会在 FE 进程启动时被读取。没有在 `fe.conf` 中的配置项将使用默认值。

2. 通过 MySQL 协议动态配置

   FE 启动后，可以通过以下命令动态设置配置项。该命令需要管理员权限。

   `ADMIN SET FRONTEND CONFIG ("fe_config_name" = "fe_config_value");`

   不是所有配置项都支持动态配置。可以通过 `ADMIN SHOW FRONTEND CONFIG;` 命令结果中的 `IsMutable` 列查看是否支持动态配置。

   如果是修改 `MasterOnly` 的配置项，则该命令会直接转发给 Master FE 并且仅修改 Master FE 中对应的配置项。

   **通过该方式修改的配置项将在 FE 进程重启后失效。**

   更多该命令的帮助，可以通过 `HELP ADMIN SET CONFIG;` 命令查看。

3. 通过 HTTP 协议动态配置

   具体请参阅 [Set Config Action](http://doris.apache.org/master/zh-CN/administrator-guide/http-actions/fe/set-config-action.html)

   该方式也可以持久化修改后的配置项。配置项将持久化在 `fe_custom.conf` 文件中，在 FE 重启后仍会生效。

## 应用举例

1. 修改 `async_pending_load_task_pool_size`

   通过 `ADMIN SHOW FRONTEND CONFIG;` 可以查看到该配置项不能动态配置（`IsMutable` 为 false）。则需要在 `fe.conf` 中添加：

   `async_pending_load_task_pool_size=20`

   之后重启 FE 进程以生效该配置。

2. 修改 `dynamic_partition_enable`

   通过 `ADMIN SHOW FRONTEND CONFIG;` 可以查看到该配置项可以动态配置（`IsMutable` 为 true）。并且是 Master FE 独有配置。则首先我们可以连接到任意 FE，执行如下命令修改配置：

   ```text
   ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");`
   ```

   之后可以通过如下命令查看修改后的值：

   ```text
   set forward_to_master=true;
   ADMIN SHOW FRONTEND CONFIG;
   ```

   通过以上方式修改后，如果 Master FE 重启或进行了 Master 切换，则配置将失效。可以通过在 `fe.conf` 中直接添加配置项，并重启 FE 后，永久生效该配置项。

3. 修改 `max_distribution_pruner_recursion_depth`

   通过 `ADMIN SHOW FRONTEND CONFIG;` 可以查看到该配置项可以动态配置（`IsMutable` 为 true）。并且不是 Master FE 独有配置。

   同样，我们可以通过动态修改配置的命令修改该配置。因为该配置不是 Master FE 独有配置，所以需要单独连接到不同的 FE，进行动态修改配置的操作，这样才能保证所有 FE 都使用了修改后的配置值

## 配置项列表

### `max_dynamic_partition_num`

默认值：500

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

用于限制创建动态分区表时可以创建的最大分区数，避免一次创建过多分区。 数量由动态分区参数中的“开始”和“结束”决定。

### `grpc_max_message_size_bytes`

默认值：1G

用于设置 GRPC 客户端通道的初始流窗口大小，也用于设置最大消息大小。当结果集较大时，可能需要增大该值。

### `min_replication_num_per_tablet`

默认值：1

用于设置单个tablet的最小replication数量。

### `max_replication_num_per_tablet`

默认值：32767

用于设置单个 tablet 的最大 replication 数量。

### `enable_outfile_to_local`

默认值：false

是否允许 outfile 函数将结果导出到本地磁盘

### `enable_access_file_without_broker`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

此配置用于在通过代理访问 bos 或其他云存储时尝试跳过代理

### `enable_bdbje_debug_mode`

默认值：false

如果设置为 true，FE 将在 BDBJE 调试模式下启动，在 Web 页面 `System->bdbje` 可以查看相关信息，否则不可以查看

### `enable_alpha_rowset`

默认值：false

是否支持创建 alpha rowset。默认为 false，只应在紧急情况下使用，此配置应在未来的某个版本中删除

### `enable_http_server_v2`

默认值：从官方 0.14.0 release 版之后默认是 true，之前默认 false

HTTP Server V2 由 SpringBoot 实现, 并采用前后端分离的架构。只有启用 httpv2，用户才能使用新的前端 UI 界面

### `jetty_server_acceptors`

默认值：2

### `jetty_server_selectors`

默认值：4

### `jetty_server_workers`

默认值：0

Jetty 的线程数量由以上三个参数控制。Jetty的线程架构模型非常简单，分为 acceptors、selectors 和 workers 三个线程池。acceptors 负责接受新连接，然后交给 selectors 处理HTTP消息协议的解包，最后由 workers 处理请求。前两个线程池采用非阻塞模型，一个线程可以处理很多 socket 的读写，所以线程池数量较小。

大多数项目，acceptors 线程只需要1～2个，selectors 线程配置2～4个足矣。workers 是阻塞性的业务逻辑，往往有较多的数据库操作，需要的线程数量较多，具体数量随应用程序的 QPS 和 IO 事件占比而定。QPS 越高，需要的线程数量越多，IO 占比越高，等待的线程数越多，需要的总线程数也越多。

workers 线程池默认不做设置，根据自己需要进行设置

### `jetty_server_max_http_post_size`

默认值：100 * 1024 * 1024  （100MB）

这个是 put 或 post 方法上传文件的最大字节数，默认值：100MB

### `default_max_filter_ratio`

默认值：0

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

可过滤数据（由于数据不规则等原因）的最大百分比。默认值为0，表示严格模式，只要数据有一条被过滤掉整个导入失败

### `default_db_data_quota_bytes`

默认值：1PB

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

用于设置默认数据库数据配额大小，设置单个数据库的配额大小可以使用：

```
设置数据库数据量配额，单位为B/K/KB/M/MB/G/GB/T/TB/P/PB
ALTER DATABASE db_name SET DATA QUOTA quota;
查看配置
show data （其他用法：HELP SHOW DATA）
```

### `default_db_replica_quota_size`

默认值：1073741824

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

用于设置默认数据库Replica数量配额大小，设置单个数据库配额大小可以使用：

```
设置数据库Replica数量配额
ALTER DATABASE db_name SET REPLICA QUOTA quota;
查看配置
show data （其他用法：HELP SHOW DATA）
```

### `enable_batch_delete_by_default`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

创建唯一表时是否添加删除标志列，具体原理参照官方文档：操作手册->数据导入->批量删除

### `recover_with_empty_tablet`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

在某些情况下，某些 tablet 可能会损坏或丢失所有副本。 此时数据已经丢失，损坏的 tablet 会导致整个查询失败，无法查询剩余的健康 tablet。 在这种情况下，您可以将此配置设置为 true。 系统会将损坏的 tablet 替换为空 tablet，以确保查询可以执行。 （但此时数据已经丢失，所以查询结果可能不准确）

### `max_allowed_in_element_num_of_delete`

默认值：1024

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

用于限制 delete 语句中 Predicate 的元素个数

### `cache_result_max_row_count`

默认值：3000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：false

设置可以缓存的最大行数，详细的原理可以参考官方文档：操作手册->分区缓存

### `cache_last_version_interval_second`

默认值：900

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：false

缓存结果时上一版本的最小间隔，该参数区分离线更新和实时更新

### `cache_enable_partition_mode`

默认值：true

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：false

如果设置为 true，FE 将从 BE cache 中获取数据，该选项适用于部分分区的实时更新。

### `cache_enable_sql_mode`

默认值：true

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：false

如果设置为 true，FE 会启用 sql 结果缓存，该选项适用于离线数据更新场景

|                        | case1 | case2 | case3 | case4 |
| ---------------------- | ----- | ----- | ----- | ----- |
| enable_sql_cache       | false | true  | true  | false |
| enable_partition_cache | false | false | true  | true  |

### `min_clone_task_timeout_sec`  和 `max_clone_task_timeout_sec`

默认值：最小3分钟，最大两小时

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

`min_clone_task_timeout_sec` 和 `max_clone_task_timeout_sec` 用于限制克隆任务的最小和最大超时间。 一般情况下，克隆任务的超时时间是通过数据量和最小传输速度（5MB/s）来估计的。 但在特殊情况下，您可能需要手动设置这两个配置，以确保克隆任务不会因超时而失败。

### `agent_task_resend_wait_time_ms`

默认值：5000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

当代理任务的创建时间被设置的时候，此配置将决定是否重新发送代理任务， 当且仅当当前时间减去创建时间大于 `agent_task_task_resend_wait_time_ms` 时，ReportHandler可以重新发送代理任务。

该配置目前主要用来解决 `PUBLISH_VERSION` 代理任务的重复发送问题, 目前该配置的默认值是5000，是个实验值，由于把代理任务提交到代理任务队列和提交到 BE 存在一定的时间延迟，所以调大该配置的值可以有效解决代理任务的重复发送问题，

但同时会导致提交失败或者执行失败的代理任务再次被执行的时间延长。

### `enable_odbc_table`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

是否启用 ODBC 表，默认不启用，在使用的时候需要手动配置启用，该参数可以通过：

`ADMIN SET FRONTEND CONFIG("key"="value") `方式进行设置

### `enable_spark_load`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

是否临时启用 spark load，默认不启用

### `disable_storage_medium_check`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果 disable_storage_medium_check 为true， ReportHandler 将不会检查 tablet 的存储介质， 并使得存储冷却功能失效，默认值为false。当您不关心 tablet 的存储介质是什么时，可以将值设置为true 。

### `drop_backend_after_decommission`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

该配置用于控制系统在成功下线（Decommission） BE 后，是否 Drop 该 BE。如果为 true，则在 BE 成功下线后，会删除掉该 BE 节点。如果为 false，则在 BE 成功下线后，该 BE 会一直处于 DECOMMISSION 状态，但不会被删除。

该配置在某些场景下可以发挥作用。假设一个 Doris 集群的初始状态为每个 BE 节点有一块磁盘。运行一段时间后，系统进行了纵向扩容，即每个 BE 节点新增2块磁盘。因为 Doris 当前还不支持 BE 内部各磁盘间的数据均衡，所以会导致初始磁盘的数据量可能一直远高于新增磁盘的数据量。此时我们可以通过以下操作进行人工的磁盘间均衡：

1. 将该配置项置为 false。
2. 对某一个 BE 节点，执行 decommission 操作，该操作会将该 BE 上的数据全部迁移到其他节点中。
3. decommission 操作完成后，该 BE 不会被删除。此时，取消掉该 BE 的 decommission 状态。则数据会开始从其他 BE 节点均衡回这个节点。此时，数据将会均匀的分布到该 BE 的所有磁盘上。
4. 对所有 BE 节点依次执行 2，3 两个步骤，最终达到所有节点磁盘均衡的目的。

### `period_of_auto_resume_min`

默认值：5 （s）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

自动恢复 Routine load 的周期

### `max_tolerable_backend_down_num`

默认值：0

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

只要有一个BE宕机，Routine Load 就无法自动恢复

### `enable_materialized_view`

默认值：true

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

该配置用于开启和关闭创建物化视图功能。如果设置为 true，则创建物化视图功能开启。用户可以通过 `CREATE MATERIALIZED VIEW` 命令创建物化视图。如果设置为 false，则无法创建物化视图。

如果在创建物化视图的时候报错 `The materialized view is coming soon` 或 `The materialized view is disabled` 则说明改配置被设置为了 false，创建物化视图功能关闭了。可以通过修改配置为 true 来启动创建物化视图功能。

该变量为动态配置，用户可以在 FE 进程启动后，通过命令修改配置。也可以通过修改 FE 的配置文件，重启 FE 来生效

### `check_java_version`

默认值：true

Doris 将检查已编译和运行的 Java 版本是否兼容，如果不兼容将抛出Java版本不匹配的异常信息，并终止启动

### `max_running_rollup_job_num_per_table`

默认值：1

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

控制 Rollup 作业并发限制

### `dynamic_partition_enable`

默认值：true

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

是否启用动态分区，默认启用

### `dynamic_partition_check_interval_seconds`

默认值：600秒，10分钟

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

检查动态分区的频率

### `disable_cluster_feature`

默认值：true

是否可以动态配置：true

多集群功能将在 0.12 版本中弃用 ，将此配置设置为 true 将禁用与集群功能相关的所有操作，包括：

1. 创建/删除集群
2. 添加、释放BE/将BE添加到集群/停用集群balance
3. 更改集群的后端数量
4. 链接/迁移数据库

### `force_do_metadata_checkpoint`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果设置为 true，则无论 jvm 内存使用百分比如何，检查点线程都会创建检查点

### `metadata_checkpoint_memory_threshold`

默认值：60  （60%）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果 jvm 内存使用百分比（堆或旧内存池）超过此阈值，则检查点线程将无法工作以避免 OOM。

### `max_distribution_pruner_recursion_depth`

默认值：100

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：false

这将限制哈希分布修剪器的最大递归深度。 例如：其中 a  in（5 个元素）和 b in（4 个元素）和 c in（3 个元素）和 d in（2 个元素）。 a/b/c/d 是分布式列，所以递归深度为 5 * 4 * 3 * 2 = 120，大于 100， 因此该分发修剪器将不起作用，只会返回所有 buckets。  增加深度可以支持更多元素的分布修剪，但可能会消耗更多的 CPU

通过 `ADMIN SHOW FRONTEND CONFIG;` 可以查看到该配置项可以动态配置（`IsMutable` 为 true）。并且不是 Master FE 独有配置。

同样，我们可以通过动态修改配置的命令修改该配置。因为该配置不是 Master FE 独有配置，所以需要单独连接到不同的 FE，进行动态修改配置的操作，这样才能保证所有 FE 都使用了修改后的配置值


### `using_old_load_usage_pattern`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果设置为 true，处理错误的 insert stmt  仍将返回一个标签给用户。 用户可以使用此标签来检查加载作业的状态。 默认值为 false，表示插入操作遇到错误，不带加载标签，直接抛出异常给用户客户端。

### `small_file_dir`

默认值：DORIS_HOME_DIR + “/small_files”

保存小文件的目录

### `max_small_file_size_bytes`

默认值：1M

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

SmallFileMgr 中单个文件存储的最大大小

### `max_small_file_number`

默认值：100

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

SmallFileMgr 中存储的最大文件数

### `max_routine_load_task_num_per_be`

默认值：5

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

每个 BE 的最大并发例 Routine Load 任务数。 这是为了限制发送到 BE 的 Routine Load 任务的数量，并且它也应该小于 BE config `routine_load_thread_pool_size`（默认 10），这是 BE 上的 Routine Load 任务线程池大小。

### `max_routine_load_task_concurrent_num`

默认值：5

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

单个 Routine Load 作业的最大并发任务数

### `max_routine_load_job_num`

默认值：100

最大 Routine Load 作业数，包括 NEED_SCHEDULED, RUNNING, PAUSE

### `max_backup_restore_job_num_per_db`

默认值：10

此配置用于控制每个 DB 能够记录的 backup/restore 任务的数量

### `max_running_txn_num_per_db`

默认值：100

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

这个配置主要是用来控制同一个 DB 的并发导入个数的。

当集群中有过多的导入任务正在运行时，新提交的导入任务可能会报错：

```text
current running txns on db xxx is xx, larger than limit xx
```

该遇到该错误时，说明当前集群内正在运行的导入任务超过了该配置值。此时建议在业务侧进行等待并重试导入任务。

一般来说不推荐增大这个配置值。过高的并发数可能导致系统负载过大

### `enable_metric_calculator`

默认值：true

如果设置为 true，指标收集器将作为守护程序计时器运行，以固定间隔收集指标

### `report_queue_size`

默认值： 100

是否可以动态配置：true

是否为  Master FE  节点独有的配置项：true

这个阈值是为了避免在 FE 中堆积过多的报告任务，可能会导致 OOM 异常等问题。 并且每个 BE 每 1 分钟会报告一次 tablet 信息，因此无限制接收报告是不可接受的。以后我们会优化 tablet 报告的处理速度

**不建议修改这个值**

### `partition_rebalance_max_moves_num_per_selection`

默认值：10

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

仅在使用  PartitionRebalancer  时有效 ，

### `partition_rebalance_move_expire_after_access`

默认值：600   (s)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

仅在使用  PartitionRebalancer  时有效。 如果更改，缓存的移动将被清除

### tablet_rebalancer_type

默认值：BeLoad

是否为 Master FE 节点独有的配置项：true

rebalancer 类型（忽略大小写）：BeLoad、Partition。 如果类型解析失败，默认使用 BeLoad

### `max_balancing_tablets`

默认值：100

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果 TabletScheduler 中的 balance tablet 数量超过 `max_balancing_tablets`，则不再进行 balance 检查

### `max_scheduling_tablets`

默认值：2000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果 TabletScheduler 中调度的 tablet 数量超过 `max_scheduling_tablets`， 则跳过检查。

### `disable_balance`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果设置为 true，TabletScheduler 将不会做 balance

### `balance_load_score_threshold`

默认值：0.1 (10%)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

集群 balance 百分比的阈值，如果一个BE的负载分数比平均分数低10%，这个后端将被标记为低负载，如果负载分数比平均分数高10%，将被标记为高负载。

### `schedule_slot_num_per_path`

默认值：2

tablet 调度程序中每个路径的默认 slot 数量

### `tablet_repair_delay_factor_second`

默认值：60 （s）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

决定修复 tablet 前的延迟时间因素。

1. 如果优先级为 VERY_HIGH，请立即修复。
2. HIGH，延迟 tablet_repair_delay_factor_second  * 1；
3. 正常：延迟 tablet_repair_delay_factor_second * 2；
4. 低：延迟 tablet_repair_delay_factor_second * 3；

### `es_state_sync_interval_second`

默认值：10

FE 会在每隔 es_state_sync_interval_secs 调用 es api 获取 es 索引分片信息

### `disable_hadoop_load`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

默认不禁用，将来不推荐使用 hadoop 集群 load。 设置为 true 以禁用这种 load 方式。

### `db_used_data_quota_update_interval_secs`

默认值：300 (s)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

一个主守护线程将每 `db_used_data_quota_update_interval_secs` 更新数据库 txn 管理器的数据库使用数据配额

为了更好的数据导入性能，在数据导入之前的数据库已使用的数据量是否超出配额的检查中，我们并不实时计算数据库已经使用的数据量，而是获取后台线程周期性更新的值。

该配置用于设置更新数据库使用的数据量的值的时间间隔

### `disable_load_job`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

不禁用，如果这设置为 true

- 调用开始 txn api 时，所有挂起的加载作业都将失败
- 调用 commit txn api 时，所有准备加载作业都将失败
- 所有提交的加载作业将等待发布

### `catalog_try_lock_timeout_ms`

默认值：5000  （ms）

是否可以动态配置：true

元数据锁的 tryLock 超时配置。 通常它不需要改变，除非你需要测试一些东西。

### `max_query_retry_time`

默认值：1

是否可以动态配置：true

查询重试次数。 如果我们遇到 RPC 异常并且没有将结果发送给用户，则可能会重试查询。 您可以减少此数字以避免雪崩灾难。

### `remote_fragment_exec_timeout_ms`

默认值：5000  （ms）

是否可以动态配置：true

异步执行远程 fragment 的超时时间。 在正常情况下，异步远程 fragment 将在短时间内执行。 如果系统处于高负载状态，请尝试将此超时设置更长的时间。

### `enable_local_replica_selection`

默认值：false

是否可以动态配置：true

如果设置为 true，Planner 将尝试在与此前端相同的主机上选择 tablet 的副本。
在以下情况下，这可能会减少网络传输：

1. N 个主机，部署了 N 个 BE 和 N 个 FE。

2. 数据有N个副本。

3. 高并发查询均匀发送到所有 Frontends

在这种情况下，所有 Frontends 只能使用本地副本进行查询。如果想当本地副本不可用时，使用非本地副本服务查询，请将 enable_local_replica_selection_fallback 设置为 true

### `enable_local_replica_selection_fallback`

默认值：false

是否可以动态配置：true

与 enable_local_replica_selection 配合使用，当本地副本不可用时，使用非本地副本服务查询。

### `max_unfinished_load_job`

默认值：1000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

最大加载任务数，包括 PENDING、ETL、LOADING、QUORUM_FINISHED。 如果超过此数量，则不允许提交加载作业。

### `max_bytes_per_broker_scanner`

默认值：3 * 1024 * 1024 * 1024L  （3G）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

broker scanner 程序可以在一个 broker 加载作业中处理的最大字节数。 通常，每个 BE 都有一个 broker scanner 程序。

### `enable_auth_check`

默认值：true

如果设置为 false，则身份验证检查将被禁用，以防新权限系统出现问题。

### `tablet_stat_update_interval_second`

默认值：300，（5分钟）

tablet 状态更新间隔
所有 FE 将在每个时间间隔从所有 BE 获取 tablet 统计信息

### `storage_flood_stage_usage_percent `

默认值：95 （95%）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

### ` storage_flood_stage_left_capacity_bytes`

默认值：

  storage_flood_stage_usage_percent  : 95  (95%)
  
  storage_flood_stage_left_capacity_bytes :  1 * 1024 * 1024 * 1024 (1GB)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果磁盘容量达到 `storage_flood_stage_usage_percent` 和 `storage_flood_stage_left_capacity_bytes`      以下操作将被拒绝：

1. load 作业      
2. restore 工作 

### `storage_high_watermark_usage_percent`

默认值：85  (85%)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

### `storage_min_left_capacity_bytes`

默认值： 2 * 1024 * 1024 * 1024  (2GB)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

`storage_high_watermark_usage_percent` 限制 BE 端存储路径使用最大容量百的分比。  `storage_min_left_capacity_bytes`限制 BE 端存储路径的最小剩余容量。  如果达到这两个限制，则不能选择此存储路径作为 tablet 存储目的地。 但是对于 tablet 恢复，我们可能会超过这些限制以尽可能保持数据完整性。

### `backup_job_default_timeout_ms`

默认值：86400 * 1000  (1天)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

备份作业的默认超时时间

### `with_k8s_certs`

默认值：false

如果在本地使用 k8s 部署管理器，请将其设置为 true 并准备证书文件

### `dpp_hadoop_client_path`

默认值：/lib/hadoop-client/hadoop/bin/hadoop

### `dpp_bytes_per_reduce`

默认值：100 * 1024 * 1024L (100M)

### `dpp_default_cluster`

默认值：palo-dpp

### `dpp_default_config_str`

默认值：{
            hadoop_configs : 'mapred.job.priority=NORMAL;mapred.job.map.capacity=50;mapred.job.reduce.capacity=50;mapred.hce.replace.streaming=false;abaci.long.stored.job=true;dce.shuffle.enable=false;dfs.client.authserver.force_stop=true;dfs.client.auth.method=0'
        } 

### dpp_config_str

默认值：{
            palo-dpp : {
                    hadoop_palo_path : '/dir',
                    hadoop_configs : 'fs.default.name=hdfs://host:port;mapred.job.tracker=host:port;hadoop.job.ugi=user,password'
                }
        }

### `enable_deploy_manager`

默认值：disable

如果使用第三方部署管理器部署 Doris，则设置为 true

有效的选项是：

- disable：没有部署管理器
- k8s：Kubernetes
- ambari：Ambari
- local：本地文件（用于测试或 Boxer2 BCC 版本）

### `enable_token_check`

默认值：true

为了向前兼容，稍后将被删除。 下载image文件时检查令牌。

### `expr_depth_limit`

默认值：3000

是否可以动态配置：true

限制 expr 树的深度。 超过此限制可能会导致在持有 db read lock 时分析时间过长。

### `expr_children_limit`

默认值：10000

是否可以动态配置：true

限制 expr 树的 expr 子节点的数量。 超过此限制可能会导致在持有数据库读锁时分析时间过长。

### `proxy_auth_magic_prefix`

默认值：x@8

### `proxy_auth_enable`

默认值：false

### `meta_publish_timeout_ms`

默认值：1000ms

默认元数据发布超时时间

### `disable_colocate_balance`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

此配置可以设置为 true 以禁用自动 colocate 表的重新定位和平衡。 如果 `disable_colocate_balance'`设置为 true，则 ColocateTableBalancer 将不会重新定位和平衡并置表。

**注意：**

1. 一般情况下，根本不需要关闭平衡。
2. 因为一旦关闭平衡，不稳定的 colocate 表可能无法恢复
3. 最终查询时无法使用 colocate 计划。

### `query_colocate_join_memory_limit_penalty_factor`

默认值：1

是否可以动态配置：true

colocote join PlanFragment instance 的 memory_limit = exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)

### `max_connection_scheduler_threads_num`

默认值：4096

查询请求调度器中的最大线程数。

前的策略是，有请求过来，就为其单独申请一个线程进行服务

### `qe_max_connection`

默认值：1024

每个 FE 的最大连接数

### `check_consistency_default_timeout_second`

默认值：600 （10分钟）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

单个一致性检查任务的默认超时。 设置足够长以适合您的tablet大小。

### `consistency_check_start_time`

默认值：23

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

一致性检查开始时间

一致性检查器将从 `consistency_check_start_time` 运行到 `consistency_check_end_time`。 默认为 23:00 至 04:00

### `consistency_check_end_time`

默认值：04

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

一致性检查结束时间

一致性检查器将从 `consistency_check_start_time` 运行到 `consistency_check_end_time`。 默认为 23:00 至 04:00

### `export_tablet_num_per_task`

默认值：5

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

每个导出查询计划的 tablet 数量

### `export_task_default_timeout_second`

默认值：2 * 3600   （2小时）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

导出作业的默认超时时间

### `export_running_job_num_limit`

默认值：5

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

运行导出作业的并发限制，默认值为 5，0 表示无限制

### `export_checker_interval_second`

默认值：5

导出检查器的运行间隔

### `default_load_parallelism`

默认值：1

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

单个节点broker load导入的默认并发度。
如果用户在提交broker load任务时，在properties中自行指定了并发度，则采用用户自定义的并发度。
此参数将与`max_broker_concurrency`、`min_bytes_per_broker_scanner`等多个配置共同决定导入任务的并发度。

### `max_broker_concurrency`

默认值：10

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

broker scanner 的最大并发数。

### `min_bytes_per_broker_scanner`

默认值：67108864L (64M)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

单个 broker scanner 将读取的最小字节数。

### `catalog_trash_expire_second`

默认值：86400L (1天)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

删除数据库（表/分区）后，您可以使用 RECOVER stmt 恢复它。 这指定了最大数据保留时间。 一段时间后，数据将被永久删除。

### `storage_cooldown_second`

默认值：30 * 24 * 3600L  （30天）

创建表（或分区）时，可以指定其存储介质（HDD 或 SSD）。 如果设置为 SSD，这将指定tablet在 SSD 上停留的默认时间。 之后，tablet将自动移动到 HDD。 您可以在 `CREATE TABLE stmt` 中设置存储冷却时间。

### `default_storage_medium`

默认值：HDD

创建表（或分区）时，可以指定其存储介质（HDD 或 SSD）。 如果未设置，则指定创建时的默认介质。

### `max_backend_down_time_second`

默认值：3600  （1小时）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果 BE 关闭了 `max_backend_down_time_second`，将触发 BACKEND_DOWN 事件。

### `alter_table_timeout_second`

默认值：86400   （1天）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

ALTER TABLE 请求的最大超时时间。 设置足够长以适合您的表格数据大小

### `capacity_used_percent_high_water`

默认值：0.75  （75%）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

磁盘容量的高水位使用百分比。 这用于计算后端的负载分数

### `clone_distribution_balance_threshold`

默认值：0.2

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

BE副本数的平衡阈值。

### `clone_capacity_balance_threshold`

默认值：0.2

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

* BE 中数据大小的平衡阈值。

  平衡算法为：

    1. 计算整个集群的平均使用容量（AUC）（总数据大小/BE数）

    2. 高水位为(AUC * (1 + clone_capacity_balance_threshold))

    3. 低水位为(AUC * (1 - clone_capacity_balance_threshold))

克隆检查器将尝试将副本从高水位 BE 移动到低水位 BE。

### `replica_delay_recovery_second`

默认值：0

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

副本之间的最小延迟秒数失败，并且尝试使用克隆来恢复它。

### `clone_high_priority_delay_second`

默认值：0

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

高优先级克隆作业的延迟触发时间

### `clone_normal_priority_delay_second`

默认值：300 （5分钟）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

正常优先级克隆作业的延迟触发时间

### `clone_low_priority_delay_second`

默认值：600 （10分钟）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

低优先级克隆作业的延迟触发时间。 克隆作业包含需要克隆（恢复或迁移）的tablet。 如果优先级为 LOW，则会延迟  `clone_low_priority_delay_second `，在作业创建之后然后被执行。 这是为了避免仅因为主机短时间停机而同时运行大量克隆作业。

注意这个配置（还有 `clone_normal_priority_delay_second`） 如果它小于 `clone_checker_interval_second` 将不起作用

### `clone_max_job_num`

默认值：100

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

低优先级克隆作业的并发数。 高优先级克隆作业的并发性目前是无限的。

### `clone_job_timeout_second`

默认值：7200  (2小时)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

单个克隆作业的默认超时。 设置足够长以适合您的副本大小。 副本数据越大，完成克隆所需的时间就越多

### `clone_checker_interval_second`

默认值：300 （5分钟）

克隆检查器的运行间隔

### `tablet_delete_timeout_second`

默认值：2

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

与 `tablet_create_timeout_second` 含义相同，但在删除 tablet 时使用

### `async_loading_load_task_pool_size`

默认值：10

是否可以动态配置：false

是否为 Master FE 节点独有的配置项：true

`loading_load`任务执行程序池大小。 该池大小限制了正在运行的最大 `loading_load`任务数。

当前，它仅限制 `broker load`的 `loading_load`任务的数量。

### `async_pending_load_task_pool_size`

默认值：10

是否可以动态配置：false

是否为 Master FE 节点独有的配置项：true

`pending_load`任务执行程序池大小。 该池大小限制了正在运行的最大 `pending_load`任务数。

当前，它仅限制 `broker load`和 `spark load`的 `pending_load`任务的数量。

它应该小于 `max_running_txn_num_per_db`的值

### `async_load_task_pool_size`

默认值：10

是否可以动态配置：false

是否为 Master FE 节点独有的配置项：true

此配置只是为了兼容旧版本，此配置已被 `async_loading_load_task_pool_size`取代，以后会被移除。

### `disable_show_stream_load`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

是否禁用显示 stream load 并清除内存中的 stream load 记录。

### `max_stream_load_record_size`

默认值：5000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

可以存储在内存中的最近 stream load 记录的默认最大数量

### `fetch_stream_load_record_interval_second`

默认值：120

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

获取 stream load 记录间隔

### `desired_max_waiting_jobs`

默认值：100

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

routine load V2 版本加载的默认等待作业数 ，这是一个理想的数字。 在某些情况下，例如切换 master，当前数量可能超过` desired_max_waiting_jobs`

### `yarn_config_dir`

默认值：PaloFe.DORIS_HOME_DIR + "/lib/yarn-config"


默认的 Yarn 配置文件目录每次运行 Yarn 命令之前，我们需要检查一下这个路径下是否存在 config 文件，如果不存在，则创建它们。


### `yarn_client_path`

默认值：PaloFe.DORIS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"

默认 Yarn 客户端路径

### `spark_launcher_log_dir`

默认值： sys_log_dir + "/spark_launcher_log"

指定的 Spark 启动器日志目录

### `spark_resource_path`

默认值：空

默认值的 Spark 依赖路径

### `spark_home_default_dir`

默认值：PaloFe.DORIS_HOME_DIR + "/lib/spark2x"

默认的 Spark home 路径

### `spark_load_default_timeout_second`

默认值：86400  (1天)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

默认 Spark 加载超时时间

### `spark_dpp_version`

默认值：1.0.0

Spark 默认版本号

### `hadoop_load_default_timeout_second`

默认值：86400 * 3   (3天)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

Hadoop 加载超时时间

### `min_load_timeout_second`

默认值：1 （1秒）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

mini load 超时时间，适用于所有类型的加载

### `max_stream_load_timeout_second`

默认值：259200 （3天）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

stream load 和 mini load 最大超时时间

### `max_load_timeout_second`

默认值：259200 （3天）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

load 最大超时时间，适用于除 stream load 之外的所有类型的加载

### `stream_load_default_timeout_second`

默认值：600 （s）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

默认 stream load 和 mini load 超时时间

### `insert_load_default_timeout_second`

默认值：3600    （1小时）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

默认 insert load 超时时间

### `mini_load_default_timeout_second`

默认值：3600    （1小时）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

默认非 stream load 类型的 mini load 的超时时间

### `broker_load_default_timeout_second`

默认值：14400   （4小时）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

Broker load 的默认超时时间

### `load_running_job_num_limit`

默认值：0

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

Load 任务数量限制，默认0，无限制

### `load_input_size_limit_gb`

默认值：0

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

Load 作业输入的数据大小，默认是0，无限制

### `delete_thread_num`

默认值：10

删除作业的并发线程数

### `load_etl_thread_num_normal_priority`

默认值：10

NORMAL 优先级 etl 加载作业的并发数。

### `load_etl_thread_num_high_priority`

默认值：3

高优先级 etl 加载作业的并发数。

### `load_pending_thread_num_normal_priority`

默认值：10

NORMAL 优先级挂起加载作业的并发数。

### `load_pending_thread_num_high_priority`

默认值：3

高优先级挂起加载作业的并发数。 加载作业优先级定义为 HIGH 或 NORMAL。 所有小批量加载作业都是 HIGH 优先级，其他类型的加载作业是 NORMAL 优先级。 设置优先级是为了避免慢加载作业长时间占用线程。 这只是内部优化的调度策略。 目前，您无法手动指定作业优先级。

### `load_checker_interval_second`

默认值：5 （s）

负载调度器运行间隔。 加载作业将其状态从 PENDING 转移到 LOADING 到 FINISHED。 加载调度程序将加载作业从 PENDING 转移到 LOADING  而 txn 回调会将加载作业从 LOADING 转移到 FINISHED。 因此，当并发未达到上限时，加载作业最多需要一个时间间隔才能完成。

### `max_layout_length_per_row`

默认值：100000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

一行的最大内存布局长度。 默认为 100 KB。
在 BE 中，RowBlock 的最大大小为 100MB（在 be.conf 中配置为  `max_unpacked_row_block_size `）。
每个 RowBlock 包含 1024 行。 因此，一行的最大大小约为 100 KB。

例如。
schema：k1(int), v1(decimal), v2(varchar(2000))
那么一行的内存布局长度为：4(int) + 16(decimal) + 2000(varchar) = 2020 (Bytes)

查看所有类型的内存布局长度，在 mysql-client 中运行 `help create table`。

如果要增加此数字以支持一行中的更多列，则还需要增加
be.conf 中的  `max_unpacked_row_block_size `，但性能影响未知。

### `load_straggler_wait_second`

默认值：300

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

负载中落后节点的最大等待秒数
例如：有 3 个副本 A, B, C load 已经在 t1 时仲裁完成 (A,B) 并且 C 没有完成，如果 (current_time-t1)> 300s，那么 doris会将 C 视为故障节点，将调用事务管理器提交事务并告诉事务管理器 C 失败。

这也用于等待发布任务时

**注意：**这个参数是所有作业的默认值，DBA 可以为单独的作业指定它

### `thrift_server_type`

该配置表示FE的Thrift服务使用的服务模型, 类型为string, 大小写不敏感。

若该参数为 `SIMPLE`, 则使用 `TSimpleServer` 模型, 该模型一般不适用于生产环境，仅限于测试使用。

若该参数为 `THREADED`, 则使用 `TThreadedSelectorServer` 模型，该模型为非阻塞式I/O模型，即主从 Reactor 模型，该模型能及时响应大量的并发连接请求，在多数场景下有较好的表现。

若该参数为 `THREAD_POOL`, 则使用 `TThreadPoolServer` 模型，该模型为阻塞式I/O模型，使用线程池处理用户连接，并发连接数受限于线程池的数量，如果能提前预估并发请求的数量，并且能容忍足够多的线程资源开销，该模型会有较好的性能表现，默认使用该服务模型

### `thrift_server_max_worker_threads`

默认值：4096

Thrift Server最大工作线程数

### `publish_version_interval_ms`

默认值：10 （ms）

两个发布版本操作之间的最小间隔

### `publish_version_timeout_second`

默认值：30 （s）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

一个事务的所有发布版本任务完成的最大等待时间

### `max_create_table_timeout_second`

默认值：60 （s）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

为了在创建表（索引）不等待太久，设置一个最大超时时间

### `tablet_create_timeout_second`

默认值：1（s）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

创建单个副本的最长等待时间。
例如。
如果您为每个表创建一个包含 m 个 tablet 和 n 个副本的表，
创建表请求将在超时前最多运行 (m * n * tablet_create_timeout_second)。

### `max_mysql_service_task_threads_num`

默认值：4096

mysql 中处理任务的最大线程数。

### `cluster_id`

默认值：-1

如果节点（FE 或 BE）具有相同的集群 id，则将认为它们属于同一个Doris 集群。 Cluster id 通常是主 FE 首次启动时生成的随机整数。 您也可以指定一个。

### `auth_token`

默认值：空

用于内部身份验证的集群令牌。

### `cluster_name`

默认值： Apache doris

集群名称，将显示为网页标题

### `mysql_service_io_threads_num`

默认值：4

mysql 中处理 io 事件的线程数。

### `mysql_service_nio_enabled`

默认值：true

mysql 服务 nio 选项是否启用，默认启用

### `query_port`

默认值：9030

Doris FE 通过 mysql 协议查询连接端口

### `rewrite_count_distinct_to_bitmap_hll`

默认值：true

该变量为 session variable，session 级别生效。

- 类型：boolean
- 描述：**仅对于 AGG 模型的表来说**，当变量为 true 时，用户查询时包含 count(distinct c1) 这类聚合函数时，如果 c1 列本身类型为 bitmap，则 count distnct 会改写为 bitmap_union_count(c1)。 当 c1 列本身类型为 hll，则 count distinct 会改写为 hll_union_agg(c1) 如果变量为 false，则不发生任何改写。

### `rpc_port`

默认值：9020

FE Thrift Server的端口

### `thrift_backlog_num`

默认值：1024

thrift 服务器的 backlog_num 当你扩大这个 backlog_num 时，你应该确保它的值大于 linux `/proc/sys/net/core/somaxconn` 配置

### `thrift_client_timeout_ms`

默认值：0

thrift 服务器的连接超时和套接字超时配置 thrift_client_timeout_ms 的默认值设置为零以防止读取超时

### `mysql_nio_backlog_num`

默认值：1024

mysql nio server 的 backlog_num 当你放大这个 backlog_num 时，你应该同时放大 linux `/proc/sys/net/core/somaxconn`文件中的值

### `http_backlog_num`

默认值：1024

netty http server 的 backlog_num 当你放大这个 backlog_num 时，你应该同时放大 linux `/proc/sys/net/core/somaxconn`文件中的值

### `http_max_line_length`

默认值：4096

HTTP 服务允许接收请求的 URL 的最大长度，单位为比特

### `http_max_header_size`

默认值：8192

HTTP 服务允许接收请求的 Header 的最大长度，单位为比特

### `http_max_chunk_size`

默认值：8192

http 上下文 chunk 块的最大尺寸

### `http_port`

默认值：8030

FE http 端口，当前所有 FE http 端口都必须相同

### `max_bdbje_clock_delta_ms`

默认值：5000 （5秒）

设置非主 FE 到主 FE 主机之间的最大可接受时钟偏差。 每当非主 FE 通过 BDBJE 建立到主 FE 的连接时，都会检查该值。 如果时钟偏差大于此值，则放弃连接。

### `ignore_meta_check`

默认值：false

是否可以动态配置：true

如果为 true，非主 FE 将忽略主 FE 与其自身之间的元数据延迟间隙，即使元数据延迟间隙超过 `meta_delay_toleration_second`。 非主 FE 仍将提供读取服务。 当您出于某种原因尝试停止 Master FE 较长时间，但仍希望非 Master FE 可以提供读取服务时，这会很有帮助。

### `metadata_failure_recovery`

默认值：false

如果为 true，FE 将重置 bdbje 复制组（即删除所有可选节点信息）并应该作为 Master 启动。 如果所有可选节点都无法启动，我们可以将元数据复制到另一个节点并将此配置设置为 true 以尝试重新启动 FE。

### `priority_networks`

默认值：空

为那些有很多 ip 的服务器声明一个选择策略。 请注意，最多应该有一个 ip 与此列表匹配。 这是一个以分号分隔格式的列表，用 CIDR 表示法，例如 10.10.10.0/24。 如果没有匹配这条规则的ip，会随机选择一个。

### `txn_rollback_limit`

默认值：100

尝试重新加入组时 bdbje 可以回滚的最大 txn 数

### `max_agent_task_threads_num`

默认值：4096

是否为 Master FE 节点独有的配置项：true

代理任务线程池中处理代理任务的最大线程数。

### `heartbeat_mgr_blocking_queue_size`

默认值：1024

是否为 Master FE 节点独有的配置项：true

在 heartbeat_mgr 中存储心跳任务的阻塞队列大小。

### `heartbeat_mgr_threads_num`

默认值：8

是否为 Master FE 节点独有的配置项：true

heartbeat _mgr 中处理心跳事件的线程数。

### `bdbje_replica_ack_timeout_second`

默认值：10

元数据会同步写入到多个 Follower FE，这个参数用于控制 Master FE 等待 Follower FE 发送 ack 的超时时间。当写入的数据较大时，可能 ack 时间较长，如果超时，会导致写元数据失败，FE 进程退出。此时可以适当调大这个参数。

### `bdbje_lock_timeout_second`

默认值：1

bdbje 操作的 lock timeout  如果 FE WARN 日志中有很多 LockTimeoutException，可以尝试增加这个值

### `bdbje_heartbeat_timeout_second`

默认值：30

master 和 follower 之间 bdbje 的心跳超时。 默认为 30 秒，与 bdbje 中的默认值相同。 如果网络遇到暂时性问题，一些意外的长 Java GC 使您烦恼，您可以尝试增加此值以减少错误超时的机会

### `replica_ack_policy`

默认值：SIMPLE_MAJORITY

选项：ALL, NONE, SIMPLE_MAJORITY

bdbje 的副本 ack 策略。 更多信息，请参见：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html

### `replica_sync_policy`

默认值：SYNC

选项：SYNC, NO_SYNC, WRITE_NO_SYNC

bdbje 的Follower FE 同步策略。

### `master_sync_policy`

默认值：SYNC

选项：SYNC, NO_SYNC, WRITE_NO_SYNC

Master FE 的 bdbje 同步策略。 如果您只部署一个 Follower FE，请将其设置为“SYNC”。 如果你部署了超过 3 个 Follower FE，你可以将这个和下面的 `replica_sync_policy ` 设置为 WRITE_NO_SYNC。 更多信息，参见：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html

### `meta_delay_toleration_second`

默认值：300 （5分钟）

如果元数据延迟间隔超过  `meta_delay_toleration_second `，非主 FE 将停止提供服务

### `edit_log_roll_num`

默认值：50000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

Master FE will save image every  `edit_log_roll_num ` meta journals.。

### `edit_log_port`

默认值：9010

bdbje端口

### `edit_log_type`

默认值：BDB

编辑日志类型。
BDB：将日志写入 bdbje
LOCAL：已弃用。

### `tmp_dir`

默认值：PaloFe.DORIS_HOME_DIR + "/temp_dir"

temp dir 用于保存某些过程的中间结果，例如备份和恢复过程。 这些过程完成后，将清除此目录中的文件。

### `meta_dir`

默认值：PaloFe.DORIS_HOME_DIR + "/doris-meta"

Doris 元数据将保存在这里。 强烈建议将此目录的存储为：

1. 高写入性能（SSD）

2. 安全（RAID）

### `custom_config_dir`

默认值：PaloFe.DORIS_HOME_DIR + "/conf"

自定义配置文件目录

配置 `fe_custom.conf` 文件的位置。默认为 `conf/` 目录下。

在某些部署环境下，`conf/` 目录可能因为系统的版本升级被覆盖掉。这会导致用户在运行是持久化修改的配置项也被覆盖。这时，我们可以将 `fe_custom.conf` 存储在另一个指定的目录中，以防止配置文件被覆盖。

### `log_roll_size_mb`

默认值：1024  （1G）

一个系统日志和审计日志的最大大小

### `sys_log_dir`

默认值：PaloFe.DORIS_HOME_DIR + "/log"

sys_log_dir:

这指定了 FE 日志目录。 FE 将产生 2 个日志文件：

1. fe.log：FE进程的所有日志。
2. fe.warn.log FE 进程的所有警告和错误日志。

### `sys_log_level`

默认值：INFO

日志级别，可选项：INFO, WARNING, ERROR, FATAL

### `sys_log_roll_num`

默认值：10

要保存在  `sys_log_roll_interval ` 内的最大 FE 日志文件。 默认为 10，表示一天最多有 10 个日志文件

### `sys_log_verbose_modules`

默认值：{}

详细模块。 VERBOSE 级别由 log4j DEBUG 级别实现。

例如：
    sys_log_verbose_modules = org.apache.doris.catalog
    这只会打印包 org.apache.doris.catalog 及其所有子包中文件的调试日志。

### `sys_log_roll_interval`

默认值：DAY

可选项:

- DAY:  log 前缀是 yyyyMMdd
- HOUR: log 前缀是 yyyyMMddHH

### `sys_log_delete_age`

默认值：7d

默认为 7 天，如果日志的最后修改时间为 7 天前，则将其删除。

支持格式：

- 7d: 7 天
- 10h: 10 小时
- 60m: 60 分钟
- 120s: 120 秒

### `audit_log_dir`

默认值：PaloFe.DORIS_HOME_DIR + "/log"

审计日志目录：
这指定了 FE 审计日志目录。
审计日志 fe.audit.log 包含所有请求以及相关信息，如  `user, host, cost, status ` 等。

### `audit_log_roll_num`

默认值：90

保留在  `audit_log_roll_interval ` 内的最大 FE 审计日志文件。

### `audit_log_modules`

默认值：{"slow_query", "query", "load", "stream_load"}

慢查询包含所有开销超过 *qe_slow_log_ms* 的查询

### `qe_slow_log_ms`

默认值：5000 （5秒）

如果查询的响应时间超过此阈值，则会在审计日志中记录为 slow_query。

### `audit_log_roll_interval`

默认值：DAY

DAY:  log前缀是：yyyyMMdd
HOUR: log前缀是：yyyyMMddHH

### `audit_log_delete_age`

默认值：30d

默认为 30 天，如果日志的最后修改时间为 30 天前，则将其删除。
支持格式：
7d 7 天
10 小时 10 小时
60m 60 分钟
120s    120 秒

### `plugin_dir`

默认值：DORIS_HOME + "/plugins

插件安装目录

### `plugin_enable`

默认值:true

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

插件是否启用，默认启用

### `label_keep_max_second`

默认值：3 * 24 * 3600  (3天)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

`label_keep_max_second  `后将删除已完成或取消的加载作业的标签，

1. 去除的标签可以重复使用。
2. 设置较短的时间会降低 FE 内存使用量 （因为所有加载作业的信息在被删除之前都保存在内存中）

在高并发写的情况下，如果出现大量作业积压，出现 `call frontend service failed`的情况，查看日志如果是元数据写占用锁的时间太长，可以将这个值调成12小时，或者更小6小时

### `streaming_label_keep_max_second`

默认值：43200 （12小时）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

对于一些高频负载工作，例如：INSERT、STREAMING LOAD、ROUTINE_LOAD_TASK 。 如果过期，则删除已完成的作业或任务。

### `history_job_keep_max_second`

默认值：7 * 24 * 3600   （7天）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

某些作业的最大保留时间。 像 schema 更改和 Rollup 作业。

### `label_clean_interval_second`

默认值：4 * 3600  （4小时）

load 标签清理器将每隔 `label_clean_interval_second` 运行一次以清理过时的作业。

### `delete_info_keep_max_second`

默认值：3 * 24 * 3600  (3天)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：false

删除元数据中创建时间大于`delete_info_keep_max_second`的delete信息。

设置较短的时间将减少 FE 内存使用量和镜像文件大小。（因为所有的deleteInfo在被删除之前都存储在内存和镜像文件中）

### `transaction_clean_interval_second`

默认值：30

如果事务 visible 或者 aborted 状态，事务将在 `transaction_clean_interval_second` 秒后被清除 ，我们应该让这个间隔尽可能短，每个清洁周期都尽快


### `default_max_query_instances`

默认值：-1

用户属性max_query_instances小于等于0时，使用该配置，用来限制单个用户同一时刻可使用的查询instance个数。该参数小于等于0表示无限制。

### `use_compact_thrift_rpc`

默认值：true

是否使用压缩格式发送查询计划结构体。开启后，可以降低约50%的查询计划结构体大小，从而避免一些 "send fragment timeout" 错误。
但是在某些高并发小查询场景下，可能会降低约10%的并发度。

### `disable_tablet_scheduler`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果设置为true，将关闭副本修复和均衡逻辑。



### `enable_force_drop_redundant_replica`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果设置为 true，系统会在副本调度逻辑中，立即删除冗余副本。这可能导致部分正在对对应副本写入的导入作业失败，但是会加速副本的均衡和修复速度。
当集群中有大量等待被均衡或修复的副本时，可以尝试设置此参数，以牺牲部分导入成功率为代价，加速副本的均衡和修复。

### `repair_slow_replica`

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

如果设置为 true，会自动检测compaction比较慢的副本，并将迁移到其他机器，检测条件是 最慢副本的版本计数超过 `min_version_count_indicate_replica_compaction_too_slow` 的值， 且与最快副本的版本计数差异所占比例超过 `valid_version_count_delta_ratio_between_replicas` 的值

### `colocate_group_relocate_delay_second`

默认值：1800

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

重分布一个 Colocation Group 可能涉及大量的tablet迁移。因此，我们需要一个更保守的策略来避免不必要的 Colocation 重分布。
重分布通常发生在 Doris 检测到有 BE 节点宕机后。这个参数用于推迟对BE宕机的判断。如默认参数下，如果 BE 节点能够在 1800 秒内恢复，则不会触发 Colocation 重分布。

### `allow_replica_on_same_host`

默认值：false

是否可以动态配置：false

是否为 Master FE 节点独有的配置项：false

是否允许同一个 tablet 的多个副本分布在同一个 host 上。这个参数主要用于本地测试是，方便搭建多个 BE 已测试某些多副本情况。不要用于非测试环境。

### `min_version_count_indicate_replica_compaction_too_slow`

默认值：300

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

版本计数阈值，用来判断副本做 compaction 的速度是否太慢

### `valid_version_count_delta_ratio_between_replicas`

默认值：0.5

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

最慢副本的版本计数与最快副本的差异有效比率阈值，如果设置 `repair_slow_replica` 为 true，则用于判断是否修复最慢的副本

### `min_bytes_indicate_replica_too_large`

默认值：2 * 1024 * 1024 * 1024 (2G)

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

数据大小阈值，用来判断副本的数据量是否太大

### skip_compaction_slower_replica

默认值：true

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：false

如果设置为true，则在选择可查询副本时，将跳过 compaction 较慢的副本

### enable_create_sync_job

开启 MySQL 数据同步作业功能。默认是 false，关闭此功能

默认值：false

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

### sync_commit_interval_second

提交事务的最大时间间隔。若超过了这个时间 channel 中还有数据没有提交，consumer 会通知 channel 提交事务。

默认值：10（秒）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

### min_sync_commit_size

提交事务需满足的最小 event 数量。若 Fe 接收到的 event 数量小于它，会继续等待下一批数据直到时间超过了 `sync_commit_interval_second ` 为止。默认值是 10000 个 events，如果你想修改此配置，请确保此值小于 canal 端的 `canal.instance.memory.buffer.size` 配置（默认16384），否则在 ack 前Fe会尝试获取比 store 队列长度更多的 event，导致 store 队列阻塞至超时为止。

默认值：10000

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

### min_bytes_sync_commit

提交事务需满足的最小数据大小。若 Fe 接收到的数据大小小于它，会继续等待下一批数据直到时间超过了 `sync_commit_interval_second` 为止。默认值是 15 MB，如果你想修改此配置，请确保此值小于 canal 端的 `canal.instance.memory.buffer.size` 和 `canal.instance.memory.buffer.memunit` 的乘积（默认 16 MB），否则在 ack 前 Fe 会尝试获取比 store 空间更大的数据，导致 store 队列阻塞至超时为止。

默认值：15 * 1024 * 1024（15M）

是否可以动态配置：true

是否为 Master FE 节点独有的配置项：true

### max_bytes_sync_commit

 数据同步作业线程池中的最大线程数量。此线程池整个FE中只有一个，用于处理FE中所有数据同步作业向BE发送数据的任务 task，线程池的实现在 `SyncTaskPool` 类。

默认值：10

是否可以动态配置：false

是否为 Master FE 节点独有的配置项：false
