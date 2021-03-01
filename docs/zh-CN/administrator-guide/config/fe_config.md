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

<!-- Please sort the configuration alphabetically -->

# FE 配置项

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
    
    * Key：配置项名称。
    * Value：当前配置项的值。
    * Type：配置项值类型，如果整型、字符串。
    * IsMutable：是否可以动态配置。如果为 true，表示该配置项可以在运行时进行动态配置。如果false，则表示该配置项只能在 `fe.conf` 中配置并且重启 FE 后生效。
    * MasterOnly：是否为 Master FE 节点独有的配置项。如果为 true，则表示该配置项仅在 Master FE 节点有意义，对其他类型的 FE 节点无意义。如果为 false，则表示该配置项在所有 FE 节点中均有意义。
    * Comment：配置项的描述。

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
    
    ```
    ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");`
    ```

    之后可以通过如下命令查看修改后的值：
    
    ```
    set forward_to_master=true;
    ADMIN SHOW FRONTEND CONFIG;
    ```
    
    通过以上方式修改后，如果 Master FE 重启或进行了 Master 切换，则配置将失效。可以通过在 `fe.conf` 中直接添加配置项，并重启 FE 后，永久生效该配置项。

3. 修改 `max_distribution_pruner_recursion_depth`

    通过 `ADMIN SHOW FRONTEND CONFIG;` 可以查看到该配置项可以动态配置（`IsMutable` 为 true）。并且不是 Master FE 独有配置。
    
    同样，我们可以通过动态修改配置的命令修改该配置。因为该配置不是 Master FE 独有配置，所以需要单独连接到不同的 FE，进行动态修改配置的操作，这样才能保证所有 FE 都使用了修改后的配置值。

## 配置项列表

### `agent_task_resend_wait_time_ms`

当代理任务的创建时间被设置的时候，此配置将决定是否重新发送代理任务， 当且仅当当前时间减去创建时间大于 `agent_task_task_resend_wait_time_ms` 时，ReportHandler可以重新发送代理任务。 
  
该配置目前主要用来解决`PUBLISH_VERSION`代理任务的重复发送问题, 目前该配置的默认值是5000，是个实验值，由于把代理任务提交到代理任务队列和提交到be存在一定的时间延迟，所以调大该配置的值可以有效解决代理任务的重复发送问题，

但同时会导致提交失败或者执行失败的代理任务再次被执行的时间延长。  
    
### `alter_table_timeout_second`

### `async_load_task_pool_size`

此配置仅用于与旧版本兼容，该配置已经被`async_loading_load_task_pool_size`所取代，将来会被移除。

### `async_loading_load_task_pool_size`

`loading_load`任务执行程序池大小。 该池大小限制了正在运行的最大`loading_load`任务数。

当前，它仅限制`broker load`的`loading_load`任务的数量。

### `async_pending_load_task_pool_size`

`pending_load`任务执行程序池大小。 该池大小限制了正在运行的最大`pending_load`任务数。

当前，它仅限制`broker load`和`spark load`的`pending_load`任务的数量。

它应该小于`max_running_txn_num_per_db`的值。

### `audit_log_delete_age`

### `audit_log_dir`

### `audit_log_modules`

### `audit_log_roll_interval`

### `audit_log_roll_mode`

### `audit_log_roll_num`

### `auth_token`

### `autocommit`

### `auto_increment_increment`

### `backup_job_default_timeout_ms`

### `backup_plugin_path`

### `balance_load_score_threshold`

### `batch_size`

### `bdbje_heartbeat_timeout_second`

### `bdbje_lock_timeout_second`

### `broker_load_default_timeout_second`

### `brpc_idle_wait_max_time`

### `brpc_number_of_concurrent_requests_processed`

### `capacity_used_percent_high_water`

### `catalog_trash_expire_second`

### `catalog_try_lock_timeout_ms`

### `character_set_client`

### `character_set_connection`

### `character_set_results`

### `character_set_server`

### `check_consistency_default_timeout_second`

### `check_java_version`

### `clone_capacity_balance_threshold`

### `clone_checker_interval_second`

### `clone_distribution_balance_threshold`

### `clone_high_priority_delay_second`

### `clone_job_timeout_second`

### `clone_low_priority_delay_second`

### `clone_max_job_num`

### `clone_normal_priority_delay_second`

### `cluster_id`

### `cluster_name`

### `codegen_level`

### `collation_connection`

### `collation_database`

### `collation_server`

### `consistency_check_end_time`

### `consistency_check_start_time`

### `custom_config_dir`

配置 `fe_custom.conf` 文件的位置。默认为 `conf/` 目录下。

在某些部署环境下，`conf/` 目录可能因为系统的版本升级被覆盖掉。这会导致用户在运行是持久化修改的配置项也被覆盖。这时，我们可以将 `fe_custom.conf` 存储在另一个指定的目录中，以防止配置文件被覆盖。

### `db_used_data_quota_update_interval_secs`

为了更好的数据导入性能，在数据导入之前的数据库已使用的数据量是否超出配额的检查中，我们并不实时计算数据库已经使用的数据量，而是获取后台线程周期性更新的值。

该配置用于设置更新数据库使用的数据量的值的时间间隔。

### `default_rowset_type`

### `default_storage_medium`

### `delete_thread_num`

### `desired_max_waiting_jobs`

### `disable_balance`

### `disable_cluster_feature`

### `disable_colocate_balance`

### `disable_colocate_join`

### `disable_colocate_join`

### `disable_colocate_relocate`

### `disable_hadoop_load`

### `disable_load_job`

### `disable_streaming_preaggregations`

### `div_precision_increment`

### `dpp_bytes_per_reduce`

### `dpp_config_str`

### `dpp_default_cluster`

### `dpp_default_config_str`

### `dpp_hadoop_client_path`

### `drop_backend_after_decommission`

该配置用于控制系统在成功下线（Decommission） BE 后，是否 Drop 该 BE。如果为 true，则在 BE 成功下线后，会删除掉该BE节点。如果为 false，则在 BE 成功下线后，该 BE 会一直处于 DECOMMISSION 状态，但不会被删除。

该配置在某些场景下可以发挥作用。假设一个 Doris 集群的初始状态为每个 BE 节点有一块磁盘。运行一段时间后，系统进行了纵向扩容，即每个 BE 节点新增2块磁盘。因为 Doris 当前还不支持 BE 内部各磁盘间的数据均衡，所以会导致初始磁盘的数据量可能一直远高于新增磁盘的数据量。此时我们可以通过以下操作进行人工的磁盘间均衡：

1. 将该配置项置为 false。
2. 对某一个 BE 节点，执行 decommission 操作，该操作会将该 BE 上的数据全部迁移到其他节点中。
3. decommission 操作完成后，该 BE 不会被删除。此时，取消掉该 BE 的 decommission 状态。则数据会开始从其他 BE 节点均衡回这个节点。此时，数据将会均匀的分布到该 BE 的所有磁盘上。
4. 对所有 BE 节点依次执行 2，3 两个步骤，最终达到所有节点磁盘均衡的目的。

### `dynamic_partition_check_interval_seconds`

### `dynamic_partition_enable`

### `edit_log_port`

### `edit_log_roll_num`

### `edit_log_type`

### `enable_auth_check`

### `enable_batch_delete_by_default`
在创建 unique 表时是否自动启用批量删除功能

### `enable_deploy_manager`

### `enable_insert_strict`

### `enable_local_replica_selection`

### `enable_materialized_view`

该配置用于开启和关闭创建物化视图功能。如果设置为 true，则创建物化视图功能开启。用户可以通过 `CREATE MATERIALIZED VIEW` 命令创建物化视图。如果设置为 false，则无法创建物化视图。

如果在创建物化视图的时候报错 `The materialized view is coming soon` 或 `The materialized view is disabled` 则说明改配置被设置为了 false，创建物化视图功能关闭了。可以通过修改配置为 true 来启动创建物化视图功能。

该变量为动态配置，用户可以在 FE 进程启动后，通过命令修改配置。也可以通过修改 FE 的配置文件，重启 FE 来生效。

### `enable_metric_calculator`

### `enable_spilling`

### `enable_token_check`

### `es_state_sync_interval_second`

### `event_scheduler`

### `exec_mem_limit`

### `export_checker_interval_second`

### `export_running_job_num_limit`

### `export_tablet_num_per_task`

### `export_task_default_timeout_second`

### `expr_children_limit`

### `expr_depth_limit`

### `force_do_metadata_checkpoint`

### `forward_to_master`

### `frontend_address`

### `hadoop_load_default_timeout_second`

### `heartbeat_mgr_blocking_queue_size`

### `heartbeat_mgr_threads_num`

### `history_job_keep_max_second`

### `http_backlog_num`
Doris netty http server 的backlog_num 参数，当你增大该配置时，也需要同时
增大 Linux /proc/sys/net/core/somaxconn 文件的值 

### `mysql_nio_backlog_num`
Doris mysql nio server 的backlog_num 参数，当你增大该配置时，也需要同时
增大 Linux /proc/sys/net/core/somaxconn 文件的值 

### `http_port`

HTTP服务监听的端口号，默认为8030

### `http_max_line_length`

HTTP服务允许接收请求的URL的最大长度，单位为比特，默认是4096

### `http_max_header_size`

HTTP服务允许接收请求的Header的最大长度，单位为比特，默认是8192

### `ignore_meta_check`

### `init_connect`

### `insert_load_default_timeout_second`

### `interactive_timeout`

### `is_report_success`

### `label_clean_interval_second`

### `label_keep_max_second`

### `language`

### `license`

### `load_checker_interval_second`

### `load_etl_thread_num_high_priority`

### `load_etl_thread_num_normal_priority`

### `load_input_size_limit_gb`

### `load_mem_limit`

### `load_pending_thread_num_high_priority`

### `load_pending_thread_num_normal_priority`

### `load_running_job_num_limit`

### `load_straggler_wait_second`

### `locale`

### `log_roll_size_mb`

### `lower_case_table_names`

### `master_sync_policy`

### `max_agent_task_threads_num`

### `max_allowed_in_element_num_of_delete`
    
该配置被用于限制delete语句中谓词in的元素数量。默认值为1024。 

### `max_allowed_packet`

### `max_backend_down_time_second`

### `max_balancing_tablets`

### `max_bdbje_clock_delta_ms`

### `max_broker_concurrency`

### `max_bytes_per_broker_scanner`

### `max_clone_task_timeout_sec`

类型：long
说明：用于控制一个 clone 任务的最大超时时间。单位秒。
默认值：7200
动态修改：是

可以配合 `mix_clone_task_timeout_sec` 来控制一个 clone 任务最大和最小的超时间。正常情况下，一个 clone 任务的超时间是通过数据量和最小传输速率（5MB/s）估算的。而在某些特殊情况下，可以通过这两个配置来认为设定 clone 任务超时时间的上下界，以保证 clone 任务可以顺利完成。

### `max_connection_scheduler_threads_num`

### `max_create_table_timeout_second`

### `max_distribution_pruner_recursion_depth`

### `max_layout_length_per_row`

### `max_load_timeout_second`

### `max_mysql_service_task_threads_num`

### `max_query_retry_time`

### `max_routine_load_job_num`

### `max_routine_load_task_concurrent_num`

### `max_routine_load_task_num_per_be`

### `max_running_rollup_job_num_per_table`

### `max_running_txn_num_per_db`

这个配置主要是用来控制同一个 db 的并发导入个数的。

当集群中有过多的导入任务正在运行时，新提交的导入任务可能会报错：

```
current running txns on db xxx is xx, larger than limit xx
```

该遇到该错误时，说明当前集群内正在运行的导入任务超过了该配置值。此时建议在业务侧进行等待并重试导入任务。

一般来说不推荐增大这个配置值。过高的并发数可能导致系统负载过大。

### `max_scheduling_tablets`

### `max_small_file_number`

### `max_small_file_size_bytes`

### `max_stream_load_timeout_second`

该配置是专门用来限制stream load的超时时间配置，防止失败的stream load事务因为用户的超长时间设置无法在短时间内被取消掉。

### `max_tolerable_backend_down_num`

### `max_unfinished_load_job`

### `metadata_checkopoint_memory_threshold`

### `metadata_failure_recovery`

### `meta_delay_toleration_second`

### `meta_dir`

### `meta_publish_timeout_ms`

### `min_bytes_per_broker_scanner`

### `min_clone_task_timeout_sec`

类型：long
说明：用于控制一个 clone 任务的最小超时时间。单位秒。
默认值：180
动态修改：是

见 `max_clone_task_timeout_sec` 说明。

### `mini_load_default_timeout_second`

### `min_load_timeout_second`

### `mysql_service_io_threads_num`

### `mysql_service_nio_enabled`

### `net_buffer_length`

### `net_read_timeout`

### `net_write_timeout`

### `parallel_exchange_instance_num`

### `parallel_fragment_exec_instance_num`

### `period_of_auto_resume_min`

### `plugin_dir`

### `plugin_enable`

### `priority_networks`

### `proxy_auth_enable`

### `proxy_auth_magic_prefix`

### `publish_version_interval_ms`

### `publish_version_timeout_second`

### `qe_max_connection`

### `qe_slow_log_ms`

### `query_cache_size`

### `query_cache_type`

### `query_colocate_join_memory_limit_penalty_factor`

### `query_port`

### `query_timeout`

### `remote_fragment_exec_timeout_ms`

### `replica_ack_policy`

### `replica_delay_recovery_second`

### `replica_sync_policy`

### `report_queue_size`

### `resource_group`

### `rewrite_count_distinct_to_bitmap_hll`

该变量为 session variable，session 级别生效。

+ 类型：boolean
+ 描述：**仅对于 AGG 模型的表来说**，当变量为 true 时，用户查询时包含 count(distinct c1) 这类聚合函数时，如果 c1 列本身类型为 bitmap，则 count distnct 会改写为 bitmap_union_count(c1)。
        当 c1 列本身类型为 hll，则 count distinct 会改写为 hll_union_agg(c1)
        如果变量为 false，则不发生任何改写。
+ 默认值：true。

### `rpc_port`

### `schedule_slot_num_per_path`

### `small_file_dir`

### `SQL_AUTO_IS_NULL`

### `sql_mode`

### `sql_safe_updates`

### `sql_select_limit`

### `storage_cooldown_second`

### `storage_engine`

### `storage_flood_stage_left_capacity_bytes`

### `storage_flood_stage_usage_percent`

### `storage_high_watermark_usage_percent`

### `storage_min_left_capacity_bytes`

### `stream_load_default_timeout_second`

### `sys_log_delete_age`

### `sys_log_dir`

### `sys_log_level`

### `sys_log_roll_interval`

### `sys_log_roll_mode`

### `sys_log_roll_num`

### `sys_log_verbose_modules`

### `system_time_zone`

### `tablet_create_timeout_second`

### `tablet_delete_timeout_second`

### `tablet_repair_delay_factor_second`

### `tablet_stat_update_interval_second`

### `test_materialized_view`

### `thrift_backlog_num`

### `thrift_client_timeout_ms`

这是 thrift 服务端的关于连接超时和socket读取数据超时的配置。
   
thrift_client_timeout_ms 的值被设置为大于0来避免线程卡在java.net.SocketInputStream.socketRead0的问题.

### `thrift_server_max_worker_threads`

### `time_zone`

### `tmp_dir`

### `transaction_clean_interval_second`

### `tx_isolation`

### `txn_rollback_limit`

### `use_new_tablet_scheduler`

### `use_v2_rollup`

### `using_old_load_usage_pattern`

### `Variable Info`

### `version`

### `version_comment`

### `wait_timeout`

### `with_k8s_certs`

### `enable_strict_storage_medium`

该配置表示在建表时，检查集群中是否存在相应的存储介质。例如当用户指定建表时存储介质为`SSD`，但此时集群中只存在`HDD`的磁盘时:

若该参数为`True`，则建表时会报错 `Failed to find enough host in all backends with storage medium with storage medium is SSD, need 3`.

若该参数为`False`，则建表时不会报错，而是将表建立在存储介质为`HDD`的磁盘上。

### `thrift_server_type`

该配置表示FE的Thrift服务使用的服务模型, 类型为string, 大小写不敏感。

若该参数为`SIMPLE`, 则使用`TSimpleServer`模型, 该模型一般不适用于生产环境，仅限于测试使用。

若该参数为`THREADED`, 则使用`TThreadedSelectorServer`模型，该模型为非阻塞式I/O模型，即主从Reactor模型，该模型能及时响应大量的并发连接请求，在多数场景下有较好的表现。

若该参数为`THREAD_POOL`, 则使用`TThreadPoolServer`模型，该模型为阻塞式I/O模型，使用线程池处理用户连接，并发连接数受限于线程池的数量，如果能提前预估并发请求的数量，并且能容忍足够多的线程资源开销，该模型会有较好的性能表现，默认使用该服务模型。

### `cache_enable_sql_mode`

该开关打开会缓存SQL查询结果集，如果查询的所有表的所有分区中的最后更新时间离查询时的间隔大于cache_last_version_interval_second，且结果集小于cache_result_max_row_count则缓存结果集，下个相同SQL会命中缓存。

### `cache_enable_partition_mode`

该开关打开会按照分区缓存查询结果集，如果查询的表的分区时间离查询时的间隔小于cache_last_version_interval_second，则会按照分区缓存结果集。

查询时会从缓存中获取部分数据，从磁盘中获取部分数据，并把数据合并返回给客户端。

### `cache_last_version_interval_second`

表最新分区的版本的时间间隔，指数据更新离当前的时间间隔，一般设置为900秒，区分离线和实时导入。

### `cache_result_max_row_count`

为了避免过多占用内存，能够被缓存最大的行数，默认2000，超过这个阈值将不能缓存置。

### `recover_with_empty_tablet`

在某些极特殊情况下，如代码BUG、或人为误操作等，可能导致部分分片的全部副本都丢失。这种情况下，数据已经实质性的丢失。但是在某些场景下，业务依然希望能够在即使有数据丢失的情况下，保证查询正常不报错，降低用户层的感知程度。此时，我们可以通过使用空白Tablet填充丢失副本的功能，来保证查询能够正常执行。

将此参数设置为 true，则 Doris 会自动使用空白副本填充所有副本都以损坏或丢失的 Tablet。

默认为 false。

### `default_db_data_quota_bytes`

用于设置database data的默认quota值，单位为 bytes，默认1T.

### `default_max_filter_ratio`

默认的最大容忍可过滤（数据不规范等原因）的数据比例。它将被Load Job 中设置的"max_filter_ratio"覆盖，默认0，取值范围0-1.

### `enable_http_server_v2`

是否启用的 V2 版本的 HTTP Server 实现。新的 HTTP Server 采用 SpringBoot 实现。并且实现了前后端分离。
只有当开启后，才能使用 `ui/` 目录下的新版 UI 界面。

默认为 false。

### `http_api_extra_base_path`

一些部署环境下，需要指定额外的 base path 作为 HTTP API 的统一前缀。这个参数用于用户指定额外的前缀。
设置后，可以通过 `GET /api/basepath` 接口获取这个参数值。
新版本的UI也会先尝试获取这个base path来拼接URL。
仅在 `enable_http_server_v2` 为 true 的情况下才有效。

默认为空，即不设置。
