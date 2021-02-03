---
{
    "title": "FE Configuration",
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

# FE Configuration

This document mainly introduces the relevant configuration items of FE.

The FE configuration file `fe.conf` is usually stored in the `conf/` directory of the FE deployment path. In version 0.14, another configuration file `fe_custom.conf` will be introduced. The configuration file is used to record the configuration items that are dynamically configured and persisted by the user during operation.

After the FE process is started, it will read the configuration items in `fe.conf` first, and then read the configuration items in `fe_custom.conf`. The configuration items in `fe_custom.conf` will overwrite the same configuration items in `fe.conf`.

The location of the `fe_custom.conf` file can be configured in `fe.conf` through the `custom_config_dir` configuration item.

## View configuration items

There are two ways to view the configuration items of FE:

1. FE web page

    Open the FE web page `http://fe_host:fe_http_port/variable` in the browser. You can see the currently effective FE configuration items in `Configure Info`.
    
2. View by command

    After the FE is started, you can view the configuration items of the FE in the MySQL client with the following command:

    `ADMIN SHOW FRONTEND CONFIG;`

    The meanings of the columns in the results are as follows:

    * Key: the name of the configuration item.
    * Value: The value of the current configuration item.
    * Type: The configuration item value type, such as integer or string.
    * IsMutable: whether it can be dynamically configured. If true, the configuration item can be dynamically configured at runtime. If false, it means that the configuration item can only be configured in `fe.conf` and takes effect after restarting FE.
    * MasterOnly: Whether it is a unique configuration item of Master FE node. If it is true, it means that the configuration item is meaningful only at the Master FE node, and is meaningless to other types of FE nodes. If false, it means that the configuration item is meaningful in all types of FE nodes.
    * Comment: The description of the configuration item.

## Set configuration items

There are two ways to configure FE configuration items:

1. Static configuration

    Add and set configuration items in the `conf/fe.conf` file. The configuration items in `fe.conf` will be read when the FE process starts. Configuration items not in `fe.conf` will use default values.
    
2. Dynamic configuration via MySQL protocol

    After the FE starts, you can set the configuration items dynamically through the following commands. This command requires administrator privilege.

    `ADMIN SET FRONTEND CONFIG (" fe_config_name "=" fe_config_value ");`

    Not all configuration items support dynamic configuration. You can check whether the dynamic configuration is supported by the `IsMutable` column in the` ADMIN SHOW FRONTEND CONFIG; `command result.

    If the configuration item of `MasterOnly` is modified, the command will be directly forwarded to the Master FE and only the corresponding configuration item in the Master FE will be modified.

    **Configuration items modified in this way will become invalid after the FE process restarts.**

    For more help on this command, you can view it through the `HELP ADMIN SET CONFIG;` command.
    
3. Dynamic configuration via HTTP protocol

    For details, please refer to [Set Config Action](../http-actions/fe/set-config-action.md)

    This method can also persist the modified configuration items. The configuration items will be persisted in the `fe_custom.conf` file and will still take effect after FE is restarted.

## Examples

1. Modify `async_pending_load_task_pool_size`

    Through `ADMIN SHOW FRONTEND CONFIG;` you can see that this configuration item cannot be dynamically configured (`IsMutable` is false). You need to add in `fe.conf`:

    `async_pending_load_task_pool_size = 20`

    Then restart the FE process to take effect the configuration.
    
2. Modify `dynamic_partition_enable`

    Through `ADMIN SHOW FRONTEND CONFIG;` you can see that the configuration item can be dynamically configured (`IsMutable` is true). And it is the unique configuration of Master FE. Then first we can connect to any FE and execute the following command to modify the configuration:

    ```
    ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true"); `
    ```

    Afterwards, you can view the modified value with the following command:

    ```
    set forward_to_master = true;
    ADMIN SHOW FRONTEND CONFIG;
    ```

    After modification in the above manner, if the Master FE restarts or a Master election is performed, the configuration will be invalid. You can add the configuration item directly in `fe.conf` and restart the FE to make the configuration item permanent.

3. Modify `max_distribution_pruner_recursion_depth`

    Through `ADMIN SHOW FRONTEND CONFIG;` you can see that the configuration item can be dynamically configured (`IsMutable` is true). It is not unique to Master FE.

    Similarly, we can modify the configuration by dynamically modifying the configuration command. Because this configuration is not unique to the Master FE, user need to connect to different FEs separately to modify the configuration dynamically, so that all FEs use the modified configuration values.

## Configurations

### `agent_task_resend_wait_time_ms`

This configuration will decide whether to resend agent task when create_time for agent_task is set, only when current_time - create_time > agent_task_resend_wait_time_ms can ReportHandler do resend agent task.     

This configuration is currently mainly used to solve the problem of repeated sending of `PUBLISH_VERSION` agent tasks. The current default value of this configuration is 5000, which is an experimental value.
 
Because there is a certain time delay between submitting agent tasks to AgentTaskQueue and submitting to be, Increasing the value of this configuration can effectively solve the problem of repeated sending of agent tasks,

But at the same time, it will cause the submission of failed or failed execution of the agent task to be executed again for an extended period of time.

### `alter_table_timeout_second`

### `async_load_task_pool_size`

This configuration is just for compatible with old version, this config has been replaced by async_loading_load_task_pool_size, it will be removed in the future.

### `async_loading_load_task_pool_size`

The loading_load task executor pool size. This pool size limits the max running loading_load tasks.

Currently, it only limits the loading_load task of broker load.

### `async_pending_load_task_pool_size`

The pending_load task executor pool size. This pool size limits the max running pending_load tasks.

Currently, it only limits the pending_load task of broker load and spark load.

It should be less than 'max_running_txn_num_per_db'

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

Configure the location of the `fe_custom.conf` file. The default is in the `conf/` directory.

In some deployment environments, the `conf/` directory may be overwritten due to system upgrades. This will cause the user modified configuration items to be overwritten. At this time, we can store `fe_custom.conf` in another specified directory to prevent the configuration file from being overwritten.

### `db_used_data_quota_update_interval_secs`

For better data load performance, in the check of whether the amount of data used by the database before data load exceeds the quota, we do not calculate the amount of data already used by the database in real time, but obtain the periodically updated value of the daemon thread.

This configuration is used to set the time interval for updating the value of the amount of data used by the database.

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

This configuration is used to control whether the system drops the BE after successfully decommissioning the BE. If true, the BE node will be deleted after the BE is successfully offline. If false, after the BE successfully goes offline, the BE will remain in the DECOMMISSION state, but will not be dropped.

This configuration can play a role in certain scenarios. Assume that the initial state of a Doris cluster is one disk per BE node. After running for a period of time, the system has been vertically expanded, that is, each BE node adds 2 new disks. Because Doris currently does not support data balancing among the disks within the BE, the data volume of the initial disk may always be much higher than the data volume of the newly added disk. At this time, we can perform manual inter-disk balancing by the following operations:

1. Set the configuration item to false.
2. Perform a decommission operation on a certain BE node. This operation will migrate all data on the BE to other nodes.
3. After the decommission operation is completed, the BE will not be dropped. At this time, cancel the decommission status of the BE. Then the data will start to balance from other BE nodes back to this node. At this time, the data will be evenly distributed to all disks of the BE.
4. Perform steps 2 and 3 for all BE nodes in sequence, and finally achieve the purpose of disk balancing for all nodes.

### `dynamic_partition_check_interval_seconds`

### `dynamic_partition_enable`

### `edit_log_port`

### `edit_log_roll_num`

### `edit_log_type`

### `enable_auth_check`

### `enable_batch_delete_by_default`
Whether to add a delete sign column when create unique table

### `enable_deploy_manager`

### `enable_insert_strict`

### `enable_local_replica_selection`

### `enable_materialized_view`

This configuration is used to turn on and off the creation of materialized views. If set to true, the function to create a materialized view is enabled. The user can create a materialized view through the `CREATE MATERIALIZED VIEW` command. If set to false, materialized views cannot be created.

If you get an error `The materialized view is coming soon` or `The materialized view is disabled` when creating the materialized view, it means that the configuration is set to false and the function of creating the materialized view is turned off. You can start to create a materialized view by modifying the configuration to true.

This variable is a dynamic configuration, and users can modify the configuration through commands after the FE process starts. You can also modify the FE configuration file and restart the FE to take effect.

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

The backlog_num for netty http server, When you enlarge this backlog_num,
you should enlarge the value in the linux /proc/sys/net/core/somaxconn file at the same time

### `mysql_nio_backlog_num`

The backlog_num for mysql nio server, When you enlarge this backlog_num,
you should enlarge the value in the linux /proc/sys/net/core/somaxconn file at the same time

### `http_port`

HTTP bind port. Defaults to 8030.

### `http_max_line_length`

The max length of an HTTP URL. The unit of this configuration is BYTE. Defaults to 4096.

### `http_max_header_size`

The max size of allowed HTTP headers. The unit of this configuration is BYTE. Defaults to 8192.

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
    
This configuration is used to limit element num of InPredicate in delete statement. The default value is 1024.

### `max_allowed_packet`

### `max_backend_down_time_second`

### `max_balancing_tablets`

### `max_bdbje_clock_delta_ms`

### `max_broker_concurrency`

### `max_bytes_per_broker_scanner`

### `max_clone_task_timeout_sec`

Type: long
Description: Used to control the maximum timeout of a clone task. The unit is second.
Default value: 7200
Dynamic modification: yes

Can cooperate with `mix_clone_task_timeout_sec` to control the maximum and minimum timeout of a clone task. Under normal circumstances, the timeout of a clone task is estimated by the amount of data and the minimum transfer rate (5MB/s). In some special cases, these two configurations can be used to set the upper and lower bounds of the clone task timeout to ensure that the clone task can be completed successfully.

### `max_connection_scheduler_threads_num`

### `max_conn_per_user`

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

This configuration is mainly used to control the number of concurrent load jobs of the same database.

When there are too many load jobs running in the cluster, the newly submitted load jobs may report errors:

```
current running txns on db xxx is xx, larger than limit xx
```

When this error is encountered, it means that the load jobs currently running in the cluster exceeds the configuration value. At this time, it is recommended to wait on the business side and retry the load jobs.

Generally it is not recommended to increase this configuration value. An excessively high number of concurrency may cause excessive system load.

### `max_scheduling_tablets`

### `max_small_file_number`

### `max_small_file_size_bytes`

### `max_stream_load_timeout_second`

This configuration is specifically used to limit timeout setting for stream load. It is to prevent that failed stream load transactions cannot be canceled within a short time because of the user's large timeout setting. 

### `max_tolerable_backend_down_num`

### `max_unfinished_load_job`

### `metadata_checkopoint_memory_threshold`

### `metadata_failure_recovery`

### `meta_delay_toleration_second`

### `meta_dir`

### `meta_publish_timeout_ms`

### `min_bytes_per_broker_scanner`

### `min_clone_task_timeout_sec`

Type: long
Description: Used to control the minimum timeout of a clone task. The unit is second.
Default value: 180
Dynamic modification: yes

See the description of `max_clone_task_timeout_sec`.

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

This variable is a session variable, and the session level takes effect.

+ Type: boolean
+ Description: **Only for the table of the AGG model**, when the variable is true, when the user query contains aggregate functions such as count(distinct c1), if the type of the c1 column itself is bitmap, count distnct will be rewritten It is bitmap_union_count(c1).
         When the type of the c1 column itself is hll, count distinct will be rewritten as hll_union_agg(c1)
         If the variable is false, no overwriting occurs.
+ Default value: true.

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

The connection timeout and socket timeout config for thrift server.
   
The value for thrift_client_timeout_ms is set to be larger than zero to prevent some hang up problems in java.net.SocketInputStream.socketRead0.

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

### `enable_strict_storage_medium_check`

This configuration indicates that when the table is being built, it checks for the presence of the appropriate storage medium in the cluster. For example, when the user specifies that the storage medium is' SSD 'when the table is built, but only' HDD 'disks exist in the cluster,

If this parameter is' True ', the error 'Failed to find enough host in all Backends with storage medium with storage medium is SSD, need 3'.

If this parameter is' False ', no error is reported when the table is built. Instead, the table is built on a disk with 'HDD' as the storage medium.

### `thrift_server_type`

This configuration represents the service model used by The Thrift Service of FE, is of type String and is case-insensitive.

If this parameter is 'SIMPLE', then the 'TSimpleServer' model is used, which is generally not suitable for production and is limited to test use.

If the parameter is 'THREADED', then the 'TThreadedSelectorServer' model is used, which is a non-blocking I/O model, namely the master-slave Reactor model, which can timely respond to a large number of concurrent connection requests and performs well in most scenarios.

If this parameter is `THREAD_POOL`, then the `TThreadPoolServer` model is used, the model for blocking I/O model, use the thread pool to handle user connections, the number of simultaneous connections are limited by the number of thread pool, if we can estimate the number of concurrent requests in advance, and tolerant enough thread resources cost, this model will have a better performance, the service model is used by default.

### `cache_enable_sql_mode`

If this switch is turned on, the SQL query result set will be cached. If the interval between the last visit version time in all partitions of all tables in the query is greater than cache_last_version_interval_second, and the result set is less than cache_result_max_row_count, the result set will be cached, and the next same SQL will hit the cache.

### `cache_enable_partition_mode`

When this switch is turned on, the query result set will be cached according to the partition. If the interval between the query table partition time and the query time is less than cache_last_version_interval_second, the result set will be cached according to the partition.

Part of the data will be obtained from the cache and some data from the disk when querying, and the data will be merged and returned to the client.

### `cache_last_version_interval_second`

The time interval of the latest partitioned version of the table refers to the time interval between the data update and the current version. It is generally set to 900 seconds, which distinguishes offline and real-time import.

### `cache_result_max_row_count`

In order to avoid occupying too much memory, the maximum number of rows that can be cached is 2000 by default. If this threshold is exceeded, the cache cannot be set.

### `recover_with_empty_tablet`

In some very special circumstances, such as code bugs, or human misoperation, etc., all replicas of some tablets may be lost. In this case, the data has been substantially lost. However, in some scenarios, the business still hopes to ensure that the query will not report errors even if there is data loss, and reduce the perception of the user layer. At this point, we can use the blank Tablet to fill the missing replica to ensure that the query can be executed normally.

Set to true so that Doris will automatically use blank replicas to fill tablets which all replicas have been damaged or missing.

Default is false.


### `default_db_data_quota_bytes`

Used to set default database data quota size, default is 1T.


### `default_max_filter_ratio`

Used to set default max filter ratio of load Job. It will be overridden by `max_filter_ratio` of the load job properties，default value is 0, value range 0-1.

### `enable_http_server_v2`

Whether to enable the V2 version of the HTTP Server implementation. The new HTTP Server is implemented using SpringBoot. And realize the separation of front and back ends.
Only when it is turned on, can you use the new UI interface under the `ui/` directory.

Default is false.

### `http_api_extra_base_path`

In some deployment environments, user need to specify an additional base path as the unified prefix of the HTTP API. This parameter is used by the user to specify additional prefixes.
After setting, user can get the parameter value through the `GET /api/basepath` interface.
And the new UI will also try to get this base path first to assemble the URL.
Only valid when `enable_http_server_v2` is true.

The default is empty, that is, not set.
