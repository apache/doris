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
    
2. 动态配置

    FE 启动后，可以通过以下命令动态设置配置项。该命令需要管理员权限。
    
    `ADMIN SET FRONTEND CONFIG ("fe_config_name" = "fe_config_value");`
    
    不是所有配置项都支持动态配置。可以通过 `ADMIN SHOW FRONTEND CONFIG;` 命令结果中的 `IsMutable` 列查看是否支持动态配置。
    
    如果是修改 `MasterOnly` 的配置项，则该命令会直接转发给 Master FE 并且仅修改 Master FE 中对应的配置项。
    
    **通过该方式修改的配置项将在 FE 进程重启后失效。**
    
    更多该命令的帮助，可以通过 `HELP ADMIN SET CONFIG;` 命令查看。
    
## 应用举例

1. 修改 `async_load_task_pool_size`

    通过 `ADMIN SHOW FRONTEND CONFIG;` 可以查看到该配置项不能动态配置（`IsMutable` 为 false）。则需要在 `fe.conf` 中添加：
    
    `async_load_task_pool_size=20`
    
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

### `alter_table_timeout_second`

### `async_load_task_pool_size`

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

### `enable_deploy_manager`

### `enable_insert_strict`

### `enable_local_replica_selection`

### `enable_materialized_view`

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

### `http_port`

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

### `max_allowed_packet`

### `max_backend_down_time_second`

### `max_balancing_tablets`

### `max_bdbje_clock_delta_ms`

### `max_broker_concurrency`

### `max_bytes_per_broker_scanner`

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

### `max_tolerable_backend_down_num`

### `max_unfinished_load_job`

### `metadata_checkopoint_memory_threshold`

### `metadata_failure_recovery`

### `meta_delay_toleration_second`

### `meta_dir`

### `meta_publish_timeout_ms`

### `min_bytes_per_broker_scanner`

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

