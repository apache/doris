---
{
    "title": "FE 配置项",
    "language": "zh-CN",
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

# Doris FE配置参数

该文档主要介绍 FE 的相关配置项。

FE 的配置文件 `fe.conf` 通常存放在 FE 部署路径的 `conf/` 目录下。 而在 0.14 版本中会引入另一个配置文件 `fe_custom.conf`。该配置文件用于记录用户在运行时动态配置并持久化的配置项。

FE 进程启动后，会先读取 `fe.conf` 中的配置项，之后再读取 `fe_custom.conf` 中的配置项。`fe_custom.conf` 中的配置项会覆盖 `fe.conf` 中相同的配置项。

`fe_custom.conf` 文件的位置可以在 `fe.conf` 通过 `custom_config_dir` 配置项配置。

## 注意事项

**1.** 出于简化架构的目的，目前通过```mysql协议修改Config```的方式修改配置只会修改本地FE内存中的数据，而不会把变更同步到所有FE。
对于只会在Master FE生效的Config项，修改请求会自动转发到Master节点

**2.** 需要注意```forward_to_master```选项会影响```admin show frontend config```的展示结果，如果```forward_to_master=true```，那么只会展示Master的配置（即使您此时连接的是Follower FE节点），这可能导致您无法看到对本地FE配置的修改；如果期望show config返回本地FE的配置项，那么执行命令```set forward_to_master=false```

## 查看配置项

FE 的配置项有两种方式进行查看：

1. FE 前端页面查看

   在浏览器中打开 FE 前端页面 `http://fe_host:fe_http_port/Configure`。在 `Configure Info` 中可以看到当前生效的 FE 配置项。

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

   具体请参阅 [Set Config Action](../http-actions/fe/set-config-action.md)

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

> 注：
> 
> 以下内容由 `docs/generate-config-and-variable-doc.sh` 自动生成。
> 
> 如需修改，请修改 `fe/fe-common/src/main/java/org/apache/doris/common/Config.java` 中的描述信息。

### `access_control_allowed_origin_domain`

设置允许跨域访问的特定域名,默认允许任何域名跨域访问

类型：`String`

默认值：`*`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `agent_task_resend_wait_time_ms`

待补充

类型：`long`

默认值：`5000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `allow_replica_on_same_host`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `alter_table_timeout_second`

ALTER TABLE 请求的最大超时时间。设置的足够长以适应表的数据量。

类型：`int`

默认值：`2592000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `analyze_record_limit`

控制统计信息的自动触发作业执行记录的持久化行数

类型：`long`

默认值：`20000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `analyze_task_timeout_in_hours`

待补充

类型：`int`

默认值：`12`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `arrow_flight_sql_port`

FE Arrow-Flight-SQL server 的端口号

类型：`int`

默认值：`-1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `arrow_flight_token_alive_time`

Arrow Flight Server中用户token的存活时间，自上次写入后过期时间，单位分钟，默认值为4320，即3天

类型：`int`

默认值：`4320`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `arrow_flight_token_cache_size`

Arrow Flight Server中所有用户token的缓存上限，超过后LRU淘汰，默认值为2000

类型：`int`

默认值：`2000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `async_loading_load_task_pool_size`

loading load task 执行线程数。这个配置可以限制当前正在导入的作业数。

类型：`int`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `async_pending_load_task_pool_size`

pending load task 执行线程数。这个配置可以限制当前等待的导入作业数。并且应小于 `max_running_txn_num_per_db`。

类型：`int`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `async_task_consumer_thread_num`

待补充

类型：`int`

默认值：`5`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `async_task_queen_size`

待补充

类型：`int`

默认值：`1024`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `audit_log_delete_age`

FE 审计日志文件的最大存活时间。超过这个时间后，日志文件会被删除。支持的格式包括：7d, 10h, 60m, 120s

类型：`String`

默认值：`30d`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `audit_log_dir`

FE 审计日志文件的存放路径，用于存放 fe.audit.log。

类型：`String`

默认值：`null/log`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `audit_log_enable_compress`

是否压缩 FE 的 Audit 日志

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `audit_log_modules`

FE 审计日志文件的种类

类型：`String[]`

默认值：`[slow_query, query, load, stream_load]`

可选值：`slow_query`, `query`, `load`, `stream_load`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `audit_log_roll_interval`

FE 审计日志文件的切分周期

类型：`String`

默认值：`DAY`

可选值：`DAY`, `HOUR`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `audit_log_roll_num`

FE 审计日志文件的最大数量。超过这个数量后，最老的日志文件会被删除

类型：`int`

默认值：`90`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `auth_token`

集群 token，用于内部认证。

类型：`String`

默认值：``

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `auto_check_statistics_in_minutes`

该参数控制自动收集作业检查库表统计信息健康度并触发自动收集的时间间隔

类型：`int`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `autobucket_min_buckets`

Auto Buckets中最小的buckets数目

类型：`int`

默认值：`1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `backend_load_capacity_coeficient`

设置固定的 BE 负载分数中磁盘使用率系数。BE 负载分数会综合磁盘使用率和副本数而得。有效值范围为[0, 1]，当超出此范围时，则使用其他方法自动计算此系数。

类型：`double`

默认值：`-1.0`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `backend_proxy_num`

BackendServiceProxy数量, 用于池化GRPC channel

类型：`int`

默认值：`48`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `backend_rpc_timeout_ms`

待补充

类型：`int`

默认值：`60000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `backup_job_default_timeout_ms`

待补充

类型：`int`

默认值：`86400000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `backup_plugin_path`

待补充

类型：`String`

默认值：`/tools/trans_file_tool/trans_files.sh`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `balance_load_score_threshold`

待补充

类型：`double`

默认值：`0.1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `balance_slot_num_per_path`

待补充

类型：`int`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `bdbje_file_logging_level`

待补充

类型：`String`

默认值：`ALL`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `bdbje_heartbeat_timeout_second`

BDBJE 主从节点间心跳超时时间，单位为秒。默认值为 30 秒，与 BDBJE 的默认值相同。如果网络不稳定，或者 Java GC 经常导致长时间的暂停，可以适当增大这个值，减少误报超时的概率

类型：`int`

默认值：`30`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `bdbje_lock_timeout_second`

BDBJE 操作的锁超时时间，单位为秒。如果 FE 的 WARN 日志中出现大量的 LockTimeoutException，可以适当增大这个值

类型：`int`

默认值：`5`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `bdbje_replica_ack_timeout_second`

BDBJE 主从节点间同步的超时时间，单位为秒。如果出现大量的 ReplicaWriteException，可以适当增大这个值

类型：`int`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `bdbje_reserved_disk_bytes`

BDBJE 所需的空闲磁盘空间大小。如果空闲磁盘空间小于这个值，则BDBJE将无法写入。

类型：`int`

默认值：`1073741824`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `be_exec_version`

待补充

类型：`int`

默认值：`3`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `be_rebalancer_fuzzy_test`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `broker_load_default_timeout_second`

Broker load 的默认超时时间，单位是秒。

类型：`int`

默认值：`14400`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `broker_timeout_ms`

和 Broker 进程交互的 RPC 的超时时间，单位是毫秒。

类型：`int`

默认值：`10000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `cache_enable_partition_mode`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `cache_enable_sql_mode`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `cache_last_version_interval_second`

待补充

类型：`int`

默认值：`900`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `cache_result_max_data_size`

SQL/Partition Cache可以缓存的最大数据大小。

类型：`int`

默认值：`31457280`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `cache_result_max_row_count`

SQL/Partition Cache可以缓存的最大行数。

类型：`int`

默认值：`3000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `capacity_used_percent_high_water`

磁盘使用率的高水位线。用于计算 BE 的负载分数。

类型：`double`

默认值：`0.75`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `catalog_trash_expire_second`

删除数据库(表/分区)后，可以使用 RECOVER 语句恢复。此配置指定了数据的最大保留时间。超过此时间，数据将被永久删除。

类型：`long`

默认值：`86400`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `catalog_try_lock_timeout_ms`

待补充

类型：`long`

默认值：`5000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `cbo_concurrency_statistics_task_num`

待补充

类型：`int`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `cbo_default_sample_percentage`

待补充

类型：`int`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `cbo_max_statistics_job_num`

待补充

类型：`int`

默认值：`20`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `check_consistency_default_timeout_second`

单个一致性检查任务的默认超时时间。设置的足够长以适应表的数据量。

类型：`long`

默认值：`600`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `check_java_version`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `cluster_id`

集群 ID，用于内部认证。通常在集群第一次启动时，会随机生成一个 cluster id. 用户也可以手动指定。

类型：`int`

默认值：`-1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `colocate_group_relocate_delay_second`

待补充

类型：`long`

默认值：`1800`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `commit_timeout_second`

提交事务的最大超时时间，单位是秒。该参数仅用于事务型 insert 操作中。

类型：`int`

默认值：`30`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `consistency_check_end_time`

一致性检查的结束时间。与 `consistency_check_start_time` 配合使用，决定一致性检查的起止时间。如果将两个参数设置为相同的值，则一致性检查将不会被调度。

类型：`String`

默认值：`23`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `consistency_check_start_time`

一致性检查的开始时间。与 `consistency_check_end_time` 配合使用，决定一致性检查的起止时间。如果将两个参数设置为相同的值，则一致性检查将不会被调度。

类型：`String`

默认值：`23`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `cpu_resource_limit_per_analyze_task`

待补充

类型：`int`

默认值：`1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `custom_config_dir`

用户自定义配置文件的路径，用于存放 fe_custom.conf。该文件中的配置会覆盖 fe.conf 中的配置

类型：`String`

默认值：`${Env.DORIS_HOME}/conf`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `db_used_data_quota_update_interval_secs`

待补充

类型：`int`

默认值：`300`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `decommission_tablet_check_threshold`

待补充

类型：`int`

默认值：`5000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `default_db_data_quota_bytes`

待补充

类型：`long`

默认值：`1125899906842624`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `default_db_max_running_txn_num`

待补充

类型：`long`

默认值：`-1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `default_db_replica_quota_size`

待补充

类型：`long`

默认值：`1073741824`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `default_load_parallelism`

broker load 时，单个节点上 load 执行计划的默认并行度

类型：`int`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `default_max_filter_ratio`

待补充

类型：`double`

默认值：`0.0`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `default_max_query_instances`

待补充

类型：`int`

默认值：`-1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `default_schema_change_scheduler_interval_millisecond`

待补充

类型：`int`

默认值：`500`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `default_storage_medium`

创建表或分区时，可以指定存储介质(HDD 或 SSD)。如果未指定，则使用此配置指定的默认介质。

类型：`String`

默认值：`HDD`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `delete_job_max_timeout_second`

Delete 操作的最大超时时间，单位是秒。

类型：`int`

默认值：`300`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `desired_max_waiting_jobs`

Broker Load 的最大等待 job 数量。这个值是一个期望值。在某些情况下，比如切换 master，当前等待的 job 数量可能会超过这个值。

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_backend_black_list`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `disable_balance`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_colocate_balance`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_colocate_balance_between_groups`

是否启用group间的均衡

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_datev1`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `disable_decimalv2`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `disable_disk_balance`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_hadoop_load`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_iceberg_hudi_table`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `disable_load_job`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_local_deploy_manager_drop_node`

是否禁止LocalDeployManager删除节点

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_mini_load`

是否禁用 mini load，默认禁用

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `disable_nested_complex_type`

当前默认设置为 true，不支持建表时创建复杂类型(array/struct/map)嵌套复杂类型, 仅支持array类型自身嵌套。

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_shared_scan`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `disable_show_stream_load`

是否禁用 show stream load 和 clear stream load 命令，以及是否清理内存中的 stream load 记录。

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_storage_medium_check`

是否禁用存储介质检查。如果禁用，ReportHandler 将不会检查 tablet 的存储介质，并且禁用存储介质冷却功能。默认值为 false。

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disable_tablet_scheduler`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `disallow_create_catalog_with_resource`

是否禁止使用 WITH REOSOURCE 语句创建 Catalog。

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `div_precision_increment`

待补充

类型：`int`

默认值：`4`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `dpp_bytes_per_reduce`

待补充

类型：`long`

默认值：`104857600`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `dpp_config_str`

待补充

类型：`String`

默认值：`{palo-dpp : {hadoop_palo_path : '/dir',hadoop_configs : 'fs.default.name=hdfs://host:port;mapred.job.tracker=host:port;hadoop.job.ugi=user,password'}}`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `dpp_default_cluster`

待补充

类型：`String`

默认值：`palo-dpp`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `dpp_default_config_str`

待补充

类型：`String`

默认值：`{hadoop_configs : 'mapred.job.priority=NORMAL;mapred.job.map.capacity=50;mapred.job.reduce.capacity=50;mapred.hce.replace.streaming=false;abaci.long.stored.job=true;dce.shuffle.enable=false;dfs.client.authserver.force_stop=true;dfs.client.auth.method=0'}`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `dpp_hadoop_client_path`

待补充

类型：`String`

默认值：`/lib/hadoop-client/hadoop/bin/hadoop`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `drop_backend_after_decommission`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `dynamic_partition_check_interval_seconds`

待补充

类型：`long`

默认值：`600`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `dynamic_partition_enable`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `edit_log_port`

BDBJE 的端口号

类型：`int`

默认值：`9010`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `edit_log_roll_num`

BDBJE 的日志滚动大小。当日志条目数超过这个值后，会触发日志滚动

类型：`int`

默认值：`50000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `edit_log_type`

元数据日志的存储类型。BDB: 日志存储在 BDBJE 中。LOCAL：日志存储在本地文件中（仅用于测试）

类型：`String`

默认值：`bdb`

可选值：`BDB`, `LOCAL`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_access_file_without_broker`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `experimental_enable_all_http_auth`

是否启用所有 http 接口的认证

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_array_type`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `enable_auto_sample`

是否开启大表自动sample，开启后对于大小超过huge_table_lower_bound_size_in_bytes会自动通过采样收集统计信息

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_batch_delete_by_default`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `enable_bdbje_debug_mode`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_col_auth`

是否开启列权限

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_concurrent_update`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `enable_convert_light_weight_schema_change`

暂时性配置项，开启后会自动将所有的olap表修改为可light schema change

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `experimental_enable_cpu_hard_limit`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_date_conversion`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_debug_points`

是否开启debug point模式，测试使用

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `enable_decimal_conversion`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_delete_existing_files`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_deploy_manager`

待补充

类型：`String`

默认值：`disable`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `experimental_enable_feature_binlog`

是否启用binlog特性

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_force_drop_redundant_replica`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `experimental_enable_fqdn_mode`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_func_pushdown`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_hidden_version_column_by_default`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `enable_hms_events_incremental_sync`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `enable_http_server_v2`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `experimental_enable_https`

是否启用 https，如果启用，http 端口将不可用

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_local_replica_selection`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_local_replica_selection_fallback`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_metric_calculator`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `experimental_enable_mtmv`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `enable_multi_tags`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `enable_odbc_table`

是否开启 ODBC 外表功能，默认关闭，ODBC 外表是淘汰的功能，请使用 JDBC Catalog

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `enable_outfile_to_local`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_pipeline_load`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_quantile_state_type`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `enable_query_hit_stats`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `experimental_enable_query_hive_views`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_query_queue`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `enable_round_robin_create_tablet`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `experimental_enable_single_replica_load`

是否启用 stream load 和 broker load 的单副本写入。

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `experimental_enable_ssl`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_storage_policy`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `enable_token_check`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `enable_tracing`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `experimental_enable_workload_group`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `es_state_sync_interval_second`

待补充

类型：`long`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `expr_children_limit`

待补充

类型：`int`

默认值：`10000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `expr_depth_limit`

待补充

类型：`int`

默认值：`3000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `external_cache_expire_time_minutes_after_access`

待补充

类型：`long`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `fetch_stream_load_record_interval_second`

FE 从 BE 获取 Stream Load 作业信息的间隔。

类型：`int`

默认值：`120`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `finish_job_max_saved_second`

待补充

类型：`int`

默认值：`259200`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `forbid_running_alter_job`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `force_do_metadata_checkpoint`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `force_olap_table_replication_num`

用于强制设定内表的副本数，如果该参数大于零，则用户在建表时指定的副本数将被忽略，而使用本参数设置的值。同时，建表语句中指定的副本标签等参数会被忽略。该参数不影响包括创建分区、修改表属性的操作。该参数建议仅用于测试环境

类型：`int`

默认值：`0`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `full_auto_analyze_simultaneously_running_task_num`

待补充

类型：`int`

默认值：`1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `fuzzy_test_type`

待补充

类型：`String`

默认值：``

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `grpc_max_message_size_bytes`

待补充

类型：`int`

默认值：`2147483647`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `grpc_threadmgr_threads_nums`

待补充

类型：`int`

默认值：`4096`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `hadoop_load_default_timeout_second`

Hadoop load 的默认超时时间，单位是秒。

类型：`int`

默认值：`259200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `heartbeat_mgr_blocking_queue_size`

心跳线程池的队列大小

类型：`int`

默认值：`1024`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `heartbeat_mgr_threads_num`

心跳线程池的线程数

类型：`int`

默认值：`8`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `history_job_keep_max_second`

针对 ALTER, EXPORT 作业，如果作业已经完成，且超过这个时间后，会被删除。

类型：`int`

默认值：`604800`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `hive_metastore_client_timeout_second`

待补充

类型：`long`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `hive_stats_partition_sample_size`

Hive行数估算分区采样数

类型：`int`

默认值：`3000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `hms_events_batch_size_per_rpc`

待补充

类型：`int`

默认值：`500`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `hms_events_polling_interval_ms`

待补充

类型：`int`

默认值：`10000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `http_api_extra_base_path`

待补充

类型：`String`

默认值：``

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `http_port`

FE http 端口，目前所有 FE 的 http 端口必须相同

类型：`int`

默认值：`8030`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `https_port`

FE https 端口，目前所有 FE 的 https 端口必须相同

类型：`int`

默认值：`8050`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `huge_table_auto_analyze_interval_in_millis`

控制对大表的自动ANALYZE的最小时间间隔，在该时间间隔内大小超过huge_table_lower_bound_size_in_bytes的表仅ANALYZE一次

类型：`long`

默认值：`43200000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `huge_table_default_sample_rows`

定义开启开启大表自动sample后，对大表的采样比例

类型：`int`

默认值：`4194304`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `huge_table_lower_bound_size_in_bytes`

定义大表的大小下界，在开启enable_auto_sample的情况下，大小超过该值的表将会自动通过采样收集统计信息

类型：`long`

默认值：`5368709120`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `iceberg_table_creation_interval_second`

待补充

类型：`long`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `iceberg_table_creation_strict_mode`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `ignore_meta_check`

是否忽略元数据延迟，如果 FE 的元数据延迟超过这个阈值，则非 Master FE 仍然提供读服务。这个配置可以用于当 Master FE 因为某些原因停止了较长时间，但是仍然希望非 Master FE 可以提供读服务。

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `ignore_unknown_metadata_module`

是否忽略 Image 文件中未知的模块。如果为 true，不在 PersistMetaModules.MODULE_NAMES 中的元数据模块将被忽略并跳过。默认为 false，如果 Image 文件中包含未知的模块，Doris 将会抛出异常。该参数主要用于降级操作中，老版本可以兼容新版本的 Image 文件。

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `infodb_support_ext_catalog`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `insert_load_default_timeout_second`

Insert load 的默认超时时间，单位是秒。

类型：`int`

默认值：`14400`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `jdbc_drivers_dir`

JDBC 驱动的存放路径。在创建 JDBC Catalog 时，如果指定的驱动文件路径不是绝对路径，则会在这个目录下寻找

类型：`String`

默认值：`null/jdbc_drivers`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `jetty_server_acceptors`

Jetty 的 acceptor 线程数。Jetty的线程架构模型很简单，分为三个线程池：acceptor、selector 和 worker。acceptor 负责接受新的连接，然后交给 selector 处理HTTP报文协议的解包，最后由 worker 处理请求。前两个线程池采用非阻塞模型，并且一个线程可以处理很多socket的读写，所以线程池的数量少。对于大多数项目，只需要 1-2 个 acceptor 线程，2 到 4 个就足够了。Worker 的数量取决于应用的QPS和IO事件的比例。越高QPS，或者IO占比越高，等待的线程越多，需要的线程总数越多。

类型：`int`

默认值：`2`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `jetty_server_max_http_header_size`

Jetty 的最大 HTTP header 大小，单位是字节，默认值是 1MB。

类型：`int`

默认值：`1048576`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `jetty_server_max_http_post_size`

Jetty 的最大 HTTP POST 大小，单位是字节，默认值是 100MB。

类型：`int`

默认值：`104857600`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `jetty_server_selectors`

Jetty 的 selector 线程数。

类型：`int`

默认值：`4`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `jetty_server_workers`

Jetty 的 worker 线程数。0 表示使用默认线程池。

类型：`int`

默认值：`0`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `jetty_threadPool_maxThreads`

Jetty 的线程池的默认最大线程数。

类型：`int`

默认值：`400`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `jetty_threadPool_minThreads`

Jetty 的线程池的默认最小线程数。

类型：`int`

默认值：`20`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `keep_scheduler_mtmv_task_when_job_deleted`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `key_store_alias`

FE https 服务的 key store 别名

类型：`String`

默认值：`doris_ssl_certificate`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `key_store_password`

FE https 服务的 key store 密码

类型：`String`

默认值：``

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `key_store_path`

FE https 服务的 key store 路径

类型：`String`

默认值：`null/conf/ssl/doris_ssl_certificate.keystore`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `key_store_type`

FE https 服务的 key store 类型

类型：`String`

默认值：`JKS`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `label_clean_interval_second`

导入作业的清理周期，单位为秒。每个周期内，将会清理已经结束的并且过期的导入作业

类型：`int`

默认值：`3600`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `label_keep_max_second`

已完成或取消的导入作业信息的 label 会在这个时间后被删除。被删除的 label 可以被重用。

类型：`int`

默认值：`259200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `load_checker_interval_second`

load job 调度器的执行间隔，单位是秒。

类型：`int`

默认值：`5`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `locale`

待补充

类型：`String`

默认值：`zh_CN.UTF-8`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `lock_reporting_threshold_ms`

待补充

类型：`long`

默认值：`500`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `log_roll_size_mb`

fe.log 和 fe.audit.log 的最大文件大小。超过这个大小后，日志文件会被切分

类型：`int`

默认值：`1024`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `lower_case_table_names`

待补充

类型：`int`

默认值：`0`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `master_sync_policy`

元数据日志的写同步策略。如果仅部署一个 Follower FE，则推荐设置为 `SYNC`，如果有多个 Follower FE，则可以设置为 `WRITE_NO_SYNC`。可参阅：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html

类型：`String`

默认值：`SYNC`

可选值：`SYNC`, `NO_SYNC`, `WRITE_NO_SYNC`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_agent_task_threads_num`

Agent任务线程池的线程数

类型：`int`

默认值：`4096`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `max_allowed_in_element_num_of_delete`

待补充

类型：`int`

默认值：`1024`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_backend_heartbeat_failure_tolerance_count`

待补充

类型：`long`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_backup_restore_job_num_per_db`

待补充

类型：`int`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_balancing_tablets`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_bdbje_clock_delta_ms`

非 Master FE 与 Master FE 的最大时钟偏差，单位为毫秒。这个配置用于在非 Master FE 与 Master FE 之间建立 BDBJE 连接时检查时钟偏差，如果时钟偏差超过这个阈值，则 BDBJE 连接会被放弃。

类型：`long`

默认值：`5000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_be_exec_version`

待补充

类型：`int`

默认值：`3`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_broker_concurrency`

单个 broker scanner 的最大并发数。

类型：`int`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_bytes_per_broker_scanner`

待补充

类型：`long`

默认值：`536870912000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_bytes_sync_commit`

Sync job 的最大提交字节数。如果收到的字节数大于该值，Sync Job 会立即提交所有数据。这个值应大于 canal 的缓冲区大小和 `min_bytes_sync_commit`。

类型：`long`

默认值：`67108864`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_cbo_statistics_task_timeout_sec`

待补充

类型：`int`

默认值：`300`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_clone_task_timeout_sec`

待补充

类型：`long`

默认值：`7200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_create_table_timeout_second`

创建表的最大超时时间，单位是秒。

类型：`int`

默认值：`3600`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_distribution_pruner_recursion_depth`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `max_dynamic_partition_num`

待补充

类型：`int`

默认值：`500`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_error_tablet_of_broker_load`

待补充

类型：`int`

默认值：`3`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_external_cache_loader_thread_pool_size`

待补充

类型：`int`

默认值：`64`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_external_file_cache_num`

待补充

类型：`long`

默认值：`100000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_external_schema_cache_num`

待补充

类型：`long`

默认值：`10000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_hive_list_partition_num`

获取Hive分区值时候的最大返回数量，-1代表没有限制。

类型：`short`

默认值：`-1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_hive_partition_cache_num`

待补充

类型：`long`

默认值：`100000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_hive_table_cache_num`

Hive表到分区名列表缓存的最大数量。

类型：`long`

默认值：`1000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_load_timeout_second`

Load 的最大超时时间，单位是秒。

类型：`int`

默认值：`259200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_multi_partition_num`

待补充

类型：`int`

默认值：`4096`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_mysql_service_task_threads_num`

MySQL 服务的最大任务线程数

类型：`int`

默认值：`4096`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_pending_mtmv_scheduler_task_num`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_point_query_retry_time`

待补充

类型：`int`

默认值：`2`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `max_query_profile_num`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `max_query_retry_time`

待补充

类型：`int`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `max_remote_file_system_cache_num`

远程文件系统缓存的最大数量

类型：`long`

默认值：`100`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_replica_count_when_schema_change`

待补充

类型：`long`

默认值：`100000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_replication_num_per_tablet`

待补充

类型：`short`

默认值：`32767`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_routine_load_job_num`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_routine_load_task_concurrent_num`

待补充

类型：`int`

默认值：`5`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_routine_load_task_num_per_be`

待补充

类型：`int`

默认值：`5`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_running_mtmv_scheduler_task_num`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_running_rollup_job_num_per_table`

待补充

类型：`int`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_running_txn_num_per_db`

单个数据库最大并发运行的事务数，包括 prepare 和 commit 事务。

类型：`int`

默认值：`1000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_same_name_catalog_trash_num`

待补充

类型：`int`

默认值：`3`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_scheduling_tablets`

待补充

类型：`int`

默认值：`2000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_small_file_number`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_small_file_size_bytes`

待补充

类型：`int`

默认值：`1048576`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_stream_load_record_size`

Stream load 的默认最大记录数。

类型：`int`

默认值：`5000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_stream_load_timeout_second`

Stream load 的最大超时时间，单位是秒。

类型：`int`

默认值：`259200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_sync_task_threads_num`

Sync job 的最大并发数。

类型：`int`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `max_tolerable_backend_down_num`

待补充

类型：`int`

默认值：`0`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `max_unfinished_load_job`

待补充

类型：`long`

默认值：`1000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `maximum_number_of_export_partitions`

Export任务允许的最大分区数量

类型：`int`

默认值：`2000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `maximum_parallelism_of_export_job`

Export任务允许的最大并行数

类型：`int`

默认值：`50`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `maximum_tablets_of_outfile_in_export`

ExportExecutorTask任务中一个OutFile语句允许的最大tablets数量

类型：`int`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `meta_delay_toleration_second`

元数据同步的容忍延迟时间，单位为秒。如果元数据的延迟超过这个值，非主 FE 会停止提供服务

类型：`int`

默认值：`300`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `meta_dir`

元数据的存储目录

类型：`String`

默认值：`null/doris-meta`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `meta_publish_timeout_ms`

待补充

类型：`int`

默认值：`1000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `metadata_checkpoint_memory_threshold`

待补充

类型：`long`

默认值：`70`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_backend_num_for_external_table`

待补充

类型：`int`

默认值：`3`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `min_be_exec_version`

待补充

类型：`int`

默认值：`0`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `min_bytes_indicate_replica_too_large`

待补充

类型：`long`

默认值：`2147483648`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_bytes_per_broker_scanner`

单个 broker scanner 读取的最小字节数。Broker Load 切分文件时，如果切分后的文件大小小于此值，将不会切分。

类型：`long`

默认值：`67108864`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_bytes_sync_commit`

Sync job 的最小提交字节数。如果收到的字节数小于该值，Sync Job 会继续等待下一批数据，直到时间超过 `sync_commit_interval_second`。这个值应小于 canal 的缓冲区大小。

类型：`long`

默认值：`15728640`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_clone_task_timeout_sec`

待补充

类型：`long`

默认值：`180`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_create_table_timeout_second`

创建表的最小超时时间，单位是秒。

类型：`int`

默认值：`30`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_load_replica_num`

Load 成功所需的最小写入副本数。

类型：`short`

默认值：`-1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_load_timeout_second`

Load 的最小超时时间，单位是秒。

类型：`int`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_replication_num_per_tablet`

待补充

类型：`short`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_sync_commit_size`

Sync job 的最小提交事件数。如果收到的事件数小于该值，Sync Job 会继续等待下一批数据，直到时间超过 `sync_commit_interval_second`。这个值应小于 canal 的缓冲区大小。

类型：`long`

默认值：`10000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `min_version_count_indicate_replica_compaction_too_slow`

待补充

类型：`int`

默认值：`200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `multi_partition_name_prefix`

待补充

类型：`String`

默认值：`p_`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `mysql_load_in_memory_record`

待补充

类型：`int`

默认值：`20`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_load_server_secure_path`

待补充

类型：`String`

默认值：``

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_load_thread_pool`

待补充

类型：`int`

默认值：`4`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_nio_backlog_num`

mysql nio server 的 backlog 数量。如果调大这个值，则需同时调整 /proc/sys/net/core/somaxconn 的值

类型：`int`

默认值：`1024`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_service_io_threads_num`

MySQL 服务的 IO 线程数

类型：`int`

默认值：`4`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_ssl_default_ca_certificate`

待补充

类型：`String`

默认值：`null/mysql_ssl_default_certificate/ca_certificate.p12`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_ssl_default_ca_certificate_password`

待补充

类型：`String`

默认值：`doris`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_ssl_default_server_certificate`

待补充

类型：`String`

默认值：`null/mysql_ssl_default_certificate/server_certificate.p12`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysql_ssl_default_server_certificate_password`

待补充

类型：`String`

默认值：`doris`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `mysqldb_replace_name`

Doris 为了兼用 mysql 周边工具生态，会内置一个名为 mysql 的数据库，如果该数据库与用户自建数据库冲突，请修改这个字段，为 doris 内置的 mysql database 更换一个名字

类型：`String`

默认值：`mysql`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `partition_in_memory_update_interval_secs`

待补充

类型：`int`

默认值：`300`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `partition_rebalance_max_moves_num_per_selection`

待补充

类型：`int`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `partition_rebalance_move_expire_after_access`

待补充

类型：`long`

默认值：`600`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `period_analyze_simultaneously_running_task_num`

待补充

类型：`int`

默认值：`1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `period_of_auto_resume_min`

待补充

类型：`int`

默认值：`5`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `plugin_dir`

插件的安装目录

类型：`String`

默认值：`null/plugins`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `plugin_enable`

是否启用插件

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `point_query_timeout_ms`

主键高并发点查短路径超时时间。

类型：`int`

默认值：`10000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `prefer_compute_node_for_external_table`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `priority_networks`

优先使用的网络地址，如果 FE 有多个网络地址，可以通过这个配置来指定优先使用的网络地址。这是一个分号分隔的列表，每个元素是一个 CIDR 表示的网络地址

类型：`String`

默认值：``

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `proxy_auth_enable`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `proxy_auth_magic_prefix`

待补充

类型：`String`

默认值：`x@8`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `publish_topic_info_interval_ms`

待补充

类型：`int`

默认值：`30000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `publish_version_interval_ms`

Publish 任务触发线程的执行间隔，单位是毫秒。

类型：`int`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `publish_version_timeout_second`

导入 Publish 阶段的最大超时时间，单位是秒。

类型：`int`

默认值：`30`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `publish_wait_time_second`

导入 Publish 阶段的等待时间，单位是秒。超过此时间，则只需每个tablet包含一个成功副本，则导入成功。值为 -1 时，表示无限等待。

类型：`int`

默认值：`300`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `pull_request_id`

待补充

类型：`int`

默认值：`0`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `qe_max_connection`

单个 FE 的 MySQL Server 的最大连接数。

类型：`int`

默认值：`1024`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `qe_slow_log_ms`

慢查询的阈值，单位为毫秒。如果一个查询的响应时间超过这个阈值，则会被记录在 audit log 中。

类型：`long`

默认值：`5000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `query_colocate_join_memory_limit_penalty_factor`

Colocate join 每个 instance 的内存 penalty 系数。计算方式：`exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)`

类型：`int`

默认值：`1`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `query_metadata_name_ids_timeout`

查询information_schema.metadata_name_ids表时,获取一个数据库中所有表用的时间

类型：`long`

默认值：`3`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `query_port`

FE MySQL server 的端口号

类型：`int`

默认值：`9030`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `recover_with_empty_tablet`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `recover_with_skip_missing_version`

待补充

类型：`String`

默认值：`disable`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `remote_fragment_exec_timeout_ms`

待补充

类型：`long`

默认值：`30000`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `repair_slow_replica`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `replica_ack_policy`

BDBJE 节点间同步策略，可参阅：http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html

类型：`String`

默认值：`SIMPLE_MAJORITY`

可选值：`ALL`, `NONE`, `SIMPLE_MAJORITY`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `replica_sync_policy`

同 `master_sync_policy`

类型：`String`

默认值：`SYNC`

可选值：`SYNC`, `NO_SYNC`, `WRITE_NO_SYNC`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `report_queue_size`

待补充

类型：`int`

默认值：`100`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `rpc_port`

FE thrift server 的端口号

类型：`int`

默认值：`9020`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `s3_compatible_object_storages`

待补充

类型：`String`

默认值：`s3,oss,cos,bos`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `schedule_batch_size`

待补充

类型：`int`

默认值：`50`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `schedule_slot_num_per_hdd_path`

待补充

类型：`int`

默认值：`4`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `schedule_slot_num_per_ssd_path`

待补充

类型：`int`

默认值：`8`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `scheduler_job_task_max_saved_count`

待补充

类型：`int`

默认值：`20`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `scheduler_mtmv_job_expired`

待补充

类型：`long`

默认值：`86400`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `scheduler_mtmv_task_expired`

待补充

类型：`long`

默认值：`86400`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `show_details_for_unaccessible_tablet`

设置为 true，如果查询无法选择到健康副本时，会打印出该tablet所有副本的详细信息，以及不可查询的具体原因。

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `skip_compaction_slower_replica`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `skip_localhost_auth_check`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `small_file_dir`

待补充

类型：`String`

默认值：`null/small_files`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `spark_dpp_version`

Spark DPP 程序的版本

类型：`String`

默认值：`1.2-SNAPSHOT`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `spark_home_default_dir`

Spark Load 所使用的 Spark 程序目录

类型：`String`

默认值：`null/lib/spark2x`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `spark_launcher_log_dir`

Spark launcher 日志路径

类型：`String`

默认值：`null/log/spark_launcher_log`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `spark_load_checker_interval_second`

spark load job 调度器的执行间隔，单位是秒。

类型：`int`

默认值：`60`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `spark_load_default_timeout_second`

Spark load 的默认超时时间，单位是秒。

类型：`int`

默认值：`86400`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `spark_resource_path`

Spark load 所使用的依赖项目录

类型：`String`

默认值：``

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `ssl_force_client_auth`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `statistics_simultaneously_running_task_num`

待补充

类型：`int`

默认值：`5`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `statistics_sql_mem_limit_in_bytes`

待补充

类型：`long`

默认值：`2147483648`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `statistics_sql_parallel_exec_instance_num`

待补充

类型：`int`

默认值：`1`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `stats_cache_size`

待补充

类型：`long`

默认值：`100000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `storage_flood_stage_left_capacity_bytes`

待补充

类型：`long`

默认值：`1073741824`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `storage_flood_stage_usage_percent`

待补充

类型：`int`

默认值：`95`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `storage_high_watermark_usage_percent`

待补充

类型：`int`

默认值：`85`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `storage_min_left_capacity_bytes`

待补充

类型：`long`

默认值：`2147483648`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `stream_load_default_precommit_timeout_second`

Stream load 的默认预提交超时时间，单位是秒。

类型：`int`

默认值：`3600`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `stream_load_default_timeout_second`

Stream load 的默认超时时间，单位是秒。

类型：`int`

默认值：`259200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `streaming_label_keep_max_second`

针对一些高频的导入作业，比如 INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, DELETE如果导入作业或者任务已经完成，且超过这个时间后，会被删除。被删除的作业或者任务可以被重用。

类型：`int`

默认值：`43200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `sync_checker_interval_second`

Sync job 调度器的执行间隔，单位是秒。

类型：`int`

默认值：`5`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sync_commit_interval_second`

Sync job 的最大提交间隔，单位是秒。

类型：`long`

默认值：`10`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `sync_image_timeout_second`

从主节点同步image文件的超时时间，用户可根据${meta_dir}/image文件夹下面的image文件大小和节点间的网络环境调整，单位为秒，默认值300

类型：`int`

默认值：`300`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `sys_log_delete_age`

FE 日志文件的最大存活时间。超过这个时间后，日志文件会被删除。支持的格式包括：7d, 10h, 60m, 120s

类型：`String`

默认值：`7d`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sys_log_dir`

FE 日志文件的存放路径，用于存放 fe.log。

类型：`String`

默认值：`null/log`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sys_log_enable_compress`

是否压缩 FE 的历史日志

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sys_log_level`

FE 日志的级别

类型：`String`

默认值：`INFO`

可选值：`INFO`, `WARN`, `ERROR`, `FATAL`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sys_log_mode`

FE 日志的输出模式，其中 NORMAL 为默认的输出模式，日志同步输出且包含位置信息，BRIEF 模式是日志同步输出但不包含位置信息，ASYNC 模式是日志异步输出且不包含位置信息，三种日志输出模式的性能依次递增

类型：`String`

默认值：`NORMAL`

可选值：`NORMAL`, `BRIEF`, `ASYNC`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sys_log_roll_interval`

FE 日志文件的切分周期

类型：`String`

默认值：`DAY`

可选值：`DAY`, `HOUR`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sys_log_roll_num`

FE 日志文件的最大数量。超过这个数量后，最老的日志文件会被删除

类型：`int`

默认值：`10`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `sys_log_verbose_modules`

Verbose 模块。VERBOSE 级别的日志是通过 log4j 的 DEBUG 级别实现的。如设置为 `org.apache.doris.catalog`，则会打印这个 package 下的类的 DEBUG 日志。

类型：`String[]`

默认值：`[]`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `table_name_length_limit`

待补充

类型：`int`

默认值：`64`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `table_stats_health_threshold`

待补充

类型：`int`

默认值：`80`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `tablet_checker_interval_ms`

待补充

类型：`long`

默认值：`20000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_create_timeout_second`

创建单个 Replica 的最大超时时间，单位是秒。如果你要创建 m 个 tablet，每个 tablet 有 n 个 replica。则总的超时时间为 `m * n * tablet_create_timeout_second`

类型：`int`

默认值：`2`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_delete_timeout_second`

和 `tablet_create_timeout_second` 含义相同，但是是用于 Delete 操作的。

类型：`int`

默认值：`2`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_further_repair_max_times`

待补充

类型：`int`

默认值：`5`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_further_repair_timeout_second`

待补充

类型：`long`

默认值：`1200`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_rebalancer_type`

待补充

类型：`String`

默认值：`BeLoad`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_repair_delay_factor_second`

待补充

类型：`long`

默认值：`60`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_schedule_interval_ms`

待补充

类型：`long`

默认值：`1000`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `tablet_stat_update_interval_second`

待补充

类型：`int`

默认值：`60`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `thrift_backlog_num`

thrift server 的 backlog 数量。如果调大这个值，则需同时调整 /proc/sys/net/core/somaxconn 的值

类型：`int`

默认值：`1024`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `thrift_client_timeout_ms`

thrift client 的连接超时时间，单位是毫秒。0 表示不设置超时时间。

类型：`int`

默认值：`0`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `thrift_server_max_worker_threads`

thrift server 的最大 worker 线程数

类型：`int`

默认值：`4096`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `thrift_server_type`

待补充

类型：`String`

默认值：`THREAD_POOL`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `tmp_dir`

临时文件的存储目录

类型：`String`

默认值：`null/temp_dir`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `token_generate_period_hour`

待补充

类型：`int`

默认值：`12`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `token_queue_size`

待补充

类型：`int`

默认值：`6`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`true`

### `trace_export_url`

待补充

类型：`String`

默认值：`http://127.0.0.1:9411/api/v2/spans`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `trace_exporter`

待补充

类型：`String`

默认值：`zipkin`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `transaction_clean_interval_second`

事务的清理周期，单位为秒。每个周期内，将会清理已经结束的并且过期的历史事务信息

类型：`int`

默认值：`30`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `txn_rollback_limit`

BDBJE 重加入集群时，最多回滚的事务数。如果回滚的事务数超过这个值，则 BDBJE 将无法重加入集群，需要手动清理 BDBJE 的数据。

类型：`int`

默认值：`100`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `use_compact_thrift_rpc`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `use_fuzzy_session_variable`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `use_mysql_bigint_for_largeint`

是否用 mysql 的 bigint 类型来返回 Doris 的 largeint 类型

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `use_new_tablet_scheduler`

待补充

类型：`boolean`

默认值：`true`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `used_capacity_percent_max_diff`

负载均衡时，磁盘使用率最大差值。

类型：`double`

默认值：`0.3`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `using_old_load_usage_pattern`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `valid_version_count_delta_ratio_between_replicas`

待补充

类型：`double`

默认值：`0.5`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`true`

### `virtual_node_number`

开启 file cache 后，一致性哈希算法中，每个节点的虚拟节点数。该值越大，哈希算法的分布越均匀，但是会增加内存开销。

类型：`int`

默认值：`2048`

是否可动态修改：`true`

是否为 Master FE 节点独有的配置项：`false`

### `with_k8s_certs`

待补充

类型：`boolean`

默认值：`false`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `yarn_client_path`

Yarn client 的路径

类型：`String`

默认值：`null/lib/yarn-client/hadoop/bin/yarn`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`

### `yarn_config_dir`

Yarn 配置文件的路径

类型：`String`

默认值：`null/lib/yarn-config`

是否可动态修改：`false`

是否为 Master FE 节点独有的配置项：`false`


