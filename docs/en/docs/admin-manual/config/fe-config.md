---
{
    "title": "FE Configuration",
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

# FE Configuration

This document mainly introduces the relevant configuration items of FE.

The FE configuration file `fe.conf` is usually stored in the `conf/` directory of the FE deployment path. In version 0.14, another configuration file `fe_custom.conf` will be introduced. The configuration file is used to record the configuration items that are dynamically configured and persisted by the user during operation.

After the FE process is started, it will read the configuration items in `fe.conf` first, and then read the configuration items in `fe_custom.conf`. The configuration items in `fe_custom.conf` will overwrite the same configuration items in `fe.conf`.

The location of the `fe_custom.conf` file can be configured in `fe.conf` through the `custom_config_dir` configuration item.

## Precautions

**1.** For the purpose of simplifying the architecture, modifying the configuration through the mysql protocol will only modify the data in the local FE memory, and will not synchronize the changes to all FEs.
For Config items that only take effect on the Master FE, the modification request will be automatically forwarded to the Master FE.

**2.** Note that the option ```forward_to_master``` will affect the display results of ```admin show frontend config```, if ```forward_to_master=true```, ```admin show frontend config``` shows the Config of Master FE (Even if you are connecting to a Follower FE currently), this may cause you to be unable to see the modification of the local FE configuration; if you expect show config of the FE you're connecting, then execute the command ```set forward_to_master=false```.


## View configuration items

There are two ways to view the configuration items of FE:

1. FE web page

    Open the FE web page `http://fe_host:fe_http_port/Configure` in the browser. You can see the currently effective FE configuration items in `Configure Info`.

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

> Note:
>
> The following content is automatically generated by `docs/generate-config-and-variable-doc.sh`.
>
> If you need to modify, please modify the description information in `fe/fe-common/src/main/java/org/apache/doris/common/Config.java`.

### `access_control_allowed_origin_domain`

Set the specific domain name that allows cross-domain access. By default, any domain name is allowed cross-domain access

Type: `String`

Default: `*`

Mutable: `false`

Master only: `false`

### `agent_task_resend_wait_time_ms`

TODO

Type: `long`

Default: `5000`

Mutable: `true`

Master only: `true`

### `allow_replica_on_same_host`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `alter_table_timeout_second`

Maximal timeout of ALTER TABLE request. Set long enough to fit your table data size.

Type: `int`

Default: `2592000`

Mutable: `true`

Master only: `true`

### `analyze_record_limit`

Determine the persist number of automatic triggered analyze job execution status

Type: `long`

Default: `20000`

Mutable: `false`

Master only: `false`

### `analyze_task_timeout_in_hours`

TODO

Type: `int`

Default: `12`

Mutable: `false`

Master only: `false`

### `arrow_flight_sql_port`

The port of FE Arrow-Flight-SQL server

Type: `int`

Default: `-1`

Mutable: `false`

Master only: `false`

### `arrow_flight_token_alive_time`

The alive time of the user token in Arrow Flight Server, expire after write, unit minutes,the default value is 4320, which is 3 days

Type: `int`

Default: `4320`

Mutable: `false`

Master only: `false`

### `arrow_flight_token_cache_size`

The cache limit of all user tokens in Arrow Flight Server. which will be eliminated byLRU rules after exceeding the limit, the default value is 2000.

Type: `int`

Default: `2000`

Mutable: `false`

Master only: `false`

### `async_loading_load_task_pool_size`

The loading load task executor pool size. This pool size limits the max running loading load tasks.

Type: `int`

Default: `10`

Mutable: `false`

Master only: `true`

### `async_pending_load_task_pool_size`

The pending load task executor pool size. This pool size limits the max running pending load tasks.

Type: `int`

Default: `10`

Mutable: `false`

Master only: `true`

### `async_task_consumer_thread_num`

TODO

Type: `int`

Default: `5`

Mutable: `false`

Master only: `false`

### `async_task_queen_size`

TODO

Type: `int`

Default: `1024`

Mutable: `false`

Master only: `false`

### `audit_log_delete_age`

The maximum survival time of the FE audit log file. After exceeding this time, the log file will be deleted. Supported formats include: 7d, 10h, 60m, 120s

Type: `String`

Default: `30d`

Mutable: `false`

Master only: `false`

### `audit_log_dir`

The path of the FE audit log file, used to store fe.audit.log

Type: `String`

Default: `null/log`

Mutable: `false`

Master only: `false`

### `audit_log_enable_compress`

enable compression for FE audit log file

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `audit_log_modules`

The type of FE audit log file

Type: `String[]`

Default: `[slow_query, query, load, stream_load]`

Options: `slow_query`, `query`, `load`, `stream_load`

Mutable: `false`

Master only: `false`

### `audit_log_roll_interval`

The split cycle of the FE audit log file

Type: `String`

Default: `DAY`

Options: `DAY`, `HOUR`

Mutable: `false`

Master only: `false`

### `audit_log_roll_num`

The maximum number of FE audit log files. After exceeding this number, the oldest log file will be deleted

Type: `int`

Default: `90`

Mutable: `false`

Master only: `false`

### `auth_token`

Cluster token used for internal authentication.

Type: `String`

Default: ``

Mutable: `false`

Master only: `false`

### `auto_check_statistics_in_minutes`

This parameter controls the time interval for automatic collection jobs to check the health of tablestatistics and trigger automatic collection

Type: `int`

Default: `10`

Mutable: `true`

Master only: `true`

### `autobucket_min_buckets`

min buckets of auto bucket

Type: `int`

Default: `1`

Mutable: `false`

Master only: `false`

### `backend_load_capacity_coeficient`

Sets a fixed disk usage factor in the BE load fraction. The BE load score is a combination of disk usage and replica count. The valid value range is [0, 1]. When it is out of this range, other methods are used to automatically calculate this coefficient.

Type: `double`

Default: `-1.0`

Mutable: `true`

Master only: `true`

### `backend_proxy_num`

BackendServiceProxy pool size for pooling GRPC channels.

Type: `int`

Default: `48`

Mutable: `false`

Master only: `false`

### `backend_rpc_timeout_ms`

TODO

Type: `int`

Default: `60000`

Mutable: `false`

Master only: `true`

### `backup_job_default_timeout_ms`

TODO

Type: `int`

Default: `86400000`

Mutable: `true`

Master only: `true`

### `backup_plugin_path`

TODO

Type: `String`

Default: `/tools/trans_file_tool/trans_files.sh`

Mutable: `false`

Master only: `false`

### `balance_load_score_threshold`

TODO

Type: `double`

Default: `0.1`

Mutable: `true`

Master only: `true`

### `balance_slot_num_per_path`

TODO

Type: `int`

Default: `1`

Mutable: `true`

Master only: `true`

### `bdbje_file_logging_level`

TODO

Type: `String`

Default: `ALL`

Mutable: `false`

Master only: `false`

### `bdbje_heartbeat_timeout_second`

The heartbeat timeout of bdbje between master and follower, in seconds. The default is 30 seconds, which is same as default value in bdbje. If the network is experiencing transient problems, of some unexpected long java GC annoying you, you can try to increase this value to decrease the chances of false timeouts

Type: `int`

Default: `30`

Mutable: `false`

Master only: `false`

### `bdbje_lock_timeout_second`

The lock timeout of bdbje operation, in seconds. If there are many LockTimeoutException in FE WARN log, you can try to increase this value

Type: `int`

Default: `5`

Mutable: `false`

Master only: `false`

### `bdbje_replica_ack_timeout_second`

The replica ack timeout of bdbje between master and follower, in seconds. If there are many ReplicaWriteException in FE WARN log, you can try to increase this value

Type: `int`

Default: `10`

Mutable: `false`

Master only: `false`

### `bdbje_reserved_disk_bytes`

Amount of free disk space required by BDBJE. If the free disk space is less than this value, BDBJE will not be able to write.

Type: `int`

Default: `1073741824`

Mutable: `false`

Master only: `false`

### `be_exec_version`

TODO

Type: `int`

Default: `3`

Mutable: `true`

Master only: `true`

### `be_rebalancer_fuzzy_test`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `broker_load_default_timeout_second`

Default timeout for broker load job, in seconds.

Type: `int`

Default: `14400`

Mutable: `true`

Master only: `true`

### `broker_timeout_ms`

The timeout of RPC between FE and Broker, in milliseconds

Type: `int`

Default: `10000`

Mutable: `false`

Master only: `false`

### `cache_enable_partition_mode`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `cache_enable_sql_mode`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `cache_last_version_interval_second`

TODO

Type: `int`

Default: `900`

Mutable: `true`

Master only: `false`

### `cache_result_max_data_size`

Maximum data size of rows that can be cached in SQL/Partition Cache, is 3000 by default.

Type: `int`

Default: `31457280`

Mutable: `true`

Master only: `false`

### `cache_result_max_row_count`

Maximum number of rows that can be cached in SQL/Partition Cache, is 3000 by default.

Type: `int`

Default: `3000`

Mutable: `true`

Master only: `false`

### `capacity_used_percent_high_water`

The high water of disk capacity used percent. This is used for calculating load score of a backend.

Type: `double`

Default: `0.75`

Mutable: `true`

Master only: `true`

### `catalog_trash_expire_second`

After dropping database(table/partition), you can recover it by using RECOVER stmt.

Type: `long`

Default: `86400`

Mutable: `true`

Master only: `true`

### `catalog_try_lock_timeout_ms`

TODO

Type: `long`

Default: `5000`

Mutable: `true`

Master only: `false`

### `cbo_concurrency_statistics_task_num`

TODO

Type: `int`

Default: `10`

Mutable: `false`

Master only: `true`

### `cbo_default_sample_percentage`

TODO

Type: `int`

Default: `10`

Mutable: `true`

Master only: `true`

### `cbo_max_statistics_job_num`

TODO

Type: `int`

Default: `20`

Mutable: `true`

Master only: `true`

### `check_consistency_default_timeout_second`

Default timeout of a single consistency check task. Set long enough to fit your tablet size.

Type: `long`

Default: `600`

Mutable: `true`

Master only: `true`

### `check_java_version`

TODO

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `cluster_id`

Cluster id used for internal authentication. Usually a random integer generated when master FE start at first time. You can also specify one.

Type: `int`

Default: `-1`

Mutable: `false`

Master only: `false`

### `colocate_group_relocate_delay_second`

TODO

Type: `long`

Default: `1800`

Mutable: `true`

Master only: `true`

### `commit_timeout_second`

Maximal waiting time for all data inserted before one transaction to be committed, in seconds. This parameter is only used for transactional insert operation

Type: `int`

Default: `30`

Mutable: `true`

Master only: `true`

### `consistency_check_end_time`

End time of consistency check. Used with `consistency_check_start_time` to decide the start and end time of consistency check. If set to the same value, consistency check will not be scheduled.

Type: `String`

Default: `23`

Mutable: `true`

Master only: `true`

### `consistency_check_start_time`

Start time of consistency check. Used with `consistency_check_end_time` to decide the start and end time of consistency check. If set to the same value, consistency check will not be scheduled.

Type: `String`

Default: `23`

Mutable: `true`

Master only: `true`

### `cpu_resource_limit_per_analyze_task`

TODO

Type: `int`

Default: `1`

Mutable: `false`

Master only: `false`

### `custom_config_dir`

The path of the user-defined configuration file, used to store fe_custom.conf. The configuration in this file will override the configuration in fe.conf

Type: `String`

Default: `${Env.DORIS_HOME}/conf`

Mutable: `false`

Master only: `false`

### `db_used_data_quota_update_interval_secs`

TODO

Type: `int`

Default: `300`

Mutable: `false`

Master only: `true`

### `decommission_tablet_check_threshold`

TODO

Type: `int`

Default: `5000`

Mutable: `true`

Master only: `true`

### `default_db_data_quota_bytes`

TODO

Type: `long`

Default: `1125899906842624`

Mutable: `true`

Master only: `true`

### `default_db_max_running_txn_num`

TODO

Type: `long`

Default: `-1`

Mutable: `true`

Master only: `true`

### `default_db_replica_quota_size`

TODO

Type: `long`

Default: `1073741824`

Mutable: `true`

Master only: `true`

### `default_load_parallelism`

The default parallelism of the load execution plan on a single node when the broker load is submitted

Type: `int`

Default: `1`

Mutable: `true`

Master only: `true`

### `default_max_filter_ratio`

TODO

Type: `double`

Default: `0.0`

Mutable: `true`

Master only: `true`

### `default_max_query_instances`

TODO

Type: `int`

Default: `-1`

Mutable: `true`

Master only: `false`

### `default_schema_change_scheduler_interval_millisecond`

TODO

Type: `int`

Default: `500`

Mutable: `false`

Master only: `true`

### `default_storage_medium`

When create a table(or partition), you can specify its storage medium(HDD or SSD).

Type: `String`

Default: `HDD`

Mutable: `false`

Master only: `false`

### `delete_job_max_timeout_second`

Maximal timeout for delete job, in seconds.

Type: `int`

Default: `300`

Mutable: `true`

Master only: `true`

### `desired_max_waiting_jobs`

Maximal number of waiting jobs for Broker Load. This is a desired number. In some situation, such as switch the master, the current number is maybe more than this value.

Type: `int`

Default: `100`

Mutable: `true`

Master only: `true`

### `disable_backend_black_list`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `disable_balance`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_colocate_balance`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_colocate_balance_between_groups`

is allow colocate balance between all groups

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_datev1`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `disable_decimalv2`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `disable_disk_balance`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_hadoop_load`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_iceberg_hudi_table`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `disable_load_job`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_local_deploy_manager_drop_node`

Whether to disable LocalDeployManager drop node

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `disable_mini_load`

Whether to disable mini load, disabled by default

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `disable_nested_complex_type`

Now default set to true, not support create complex type(array/struct/map) nested complex type when we create table, only support array type nested array

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `disable_shared_scan`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `disable_show_stream_load`

Whether to disable show stream load and clear stream load records in memory.

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_storage_medium_check`

When disable_storage_medium_check is true, ReportHandler would not check tablet's storage medium and disable storage cool down function.

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disable_tablet_scheduler`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `disallow_create_catalog_with_resource`

Whether to disable creating catalog with WITH RESOURCE statement.

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `div_precision_increment`

TODO

Type: `int`

Default: `4`

Mutable: `true`

Master only: `false`

### `dpp_bytes_per_reduce`

TODO

Type: `long`

Default: `104857600`

Mutable: `false`

Master only: `false`

### `dpp_config_str`

TODO

Type: `String`

Default: `{palo-dpp : {hadoop_palo_path : '/dir',hadoop_configs : 'fs.default.name=hdfs://host:port;mapred.job.tracker=host:port;hadoop.job.ugi=user,password'}}`

Mutable: `false`

Master only: `false`

### `dpp_default_cluster`

TODO

Type: `String`

Default: `palo-dpp`

Mutable: `false`

Master only: `false`

### `dpp_default_config_str`

TODO

Type: `String`

Default: `{hadoop_configs : 'mapred.job.priority=NORMAL;mapred.job.map.capacity=50;mapred.job.reduce.capacity=50;mapred.hce.replace.streaming=false;abaci.long.stored.job=true;dce.shuffle.enable=false;dfs.client.authserver.force_stop=true;dfs.client.auth.method=0'}`

Mutable: `false`

Master only: `false`

### `dpp_hadoop_client_path`

TODO

Type: `String`

Default: `/lib/hadoop-client/hadoop/bin/hadoop`

Mutable: `false`

Master only: `false`

### `drop_backend_after_decommission`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `dynamic_partition_check_interval_seconds`

TODO

Type: `long`

Default: `600`

Mutable: `true`

Master only: `true`

### `dynamic_partition_enable`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `edit_log_port`

The port of BDBJE

Type: `int`

Default: `9010`

Mutable: `false`

Master only: `false`

### `edit_log_roll_num`

The log roll size of BDBJE. When the number of log entries exceeds this value, the log will be rolled

Type: `int`

Default: `50000`

Mutable: `true`

Master only: `true`

### `edit_log_type`

The storage type of the metadata log. BDB: Logs are stored in BDBJE. LOCAL: logs are stored in a local file (for testing only)

Type: `String`

Default: `bdb`

Options: `BDB`, `LOCAL`

Mutable: `false`

Master only: `false`

### `enable_access_file_without_broker`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `experimental_enable_all_http_auth`

Whether to enable all http interface authentication

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_array_type`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `enable_auto_sample`

Whether to enable automatic sampling for large tables, which, when enabled, automaticallycollects statistics through sampling for tables larger than 'huge_table_lower_bound_size_in_bytes'

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_batch_delete_by_default`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `enable_bdbje_debug_mode`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_col_auth`

Whether to enable col auth

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_concurrent_update`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `true`

### `enable_convert_light_weight_schema_change`

temporary config filed, will make all olap tables enable light schema change

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `experimental_enable_cpu_hard_limit`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `enable_date_conversion`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `enable_debug_points`

is enable debug points, use in test.

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `true`

### `enable_decimal_conversion`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `enable_delete_existing_files`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_deploy_manager`

TODO

Type: `String`

Default: `disable`

Mutable: `false`

Master only: `false`

### `experimental_enable_feature_binlog`

Whether to enable binlog feature

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_force_drop_redundant_replica`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `experimental_enable_fqdn_mode`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_func_pushdown`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `enable_hidden_version_column_by_default`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `enable_hms_events_incremental_sync`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `true`

### `enable_http_server_v2`

TODO

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `experimental_enable_https`

Whether to enable https, if enabled, http port will not be available

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_local_replica_selection`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `enable_local_replica_selection_fallback`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `enable_metric_calculator`

TODO

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `experimental_enable_mtmv`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `enable_multi_tags`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `true`

### `enable_odbc_table`

Whether to enable the ODBC appearance function, it is disabled by default, and the ODBC appearance is an obsolete feature. Please use the JDBC Catalog

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `enable_outfile_to_local`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_pipeline_load`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_quantile_state_type`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `enable_query_hit_stats`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `experimental_enable_query_hive_views`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `enable_query_queue`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `enable_round_robin_create_tablet`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `experimental_enable_single_replica_load`

Whether to enable to write single replica for stream load and broker load.

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `experimental_enable_ssl`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `enable_storage_policy`

TODO

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `true`

### `enable_token_check`

TODO

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `enable_tracing`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `experimental_enable_workload_group`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `es_state_sync_interval_second`

TODO

Type: `long`

Default: `10`

Mutable: `false`

Master only: `false`

### `expr_children_limit`

TODO

Type: `int`

Default: `10000`

Mutable: `true`

Master only: `false`

### `expr_depth_limit`

TODO

Type: `int`

Default: `3000`

Mutable: `true`

Master only: `false`

### `external_cache_expire_time_minutes_after_access`

TODO

Type: `long`

Default: `10`

Mutable: `false`

Master only: `false`

### `fetch_stream_load_record_interval_second`

The interval of FE fetch stream load record from BE.

Type: `int`

Default: `120`

Mutable: `true`

Master only: `true`

### `finish_job_max_saved_second`

TODO

Type: `int`

Default: `259200`

Mutable: `false`

Master only: `false`

### `forbid_running_alter_job`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `force_do_metadata_checkpoint`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `force_olap_table_replication_num`

Used to force the number of replicas of the internal table. If the config is greater than zero, the number of replicas specified by the user when creating the table will be ignored, and the value set by this parameter will be used. At the same time, the replica tags and other parameters specified in the create table statement will be ignored. This config does not effect the operations including creating partitions and modifying table properties. This config is recommended to be used only in the test environment

Type: `int`

Default: `0`

Mutable: `true`

Master only: `true`

### `full_auto_analyze_simultaneously_running_task_num`

TODO

Type: `int`

Default: `1`

Mutable: `false`

Master only: `false`

### `fuzzy_test_type`

TODO

Type: `String`

Default: ``

Mutable: `true`

Master only: `false`

### `grpc_max_message_size_bytes`

TODO

Type: `int`

Default: `2147483647`

Mutable: `false`

Master only: `false`

### `grpc_threadmgr_threads_nums`

TODO

Type: `int`

Default: `4096`

Mutable: `false`

Master only: `false`

### `hadoop_load_default_timeout_second`

Default timeout for hadoop load job, in seconds.

Type: `int`

Default: `259200`

Mutable: `true`

Master only: `true`

### `heartbeat_mgr_blocking_queue_size`

Queue size to store heartbeat task in heartbeat_mgr

Type: `int`

Default: `1024`

Mutable: `false`

Master only: `true`

### `heartbeat_mgr_threads_num`

Num of thread to handle heartbeat events

Type: `int`

Default: `8`

Mutable: `false`

Master only: `true`

### `history_job_keep_max_second`

For ALTER, EXPORT jobs, remove the finished job if expired.

Type: `int`

Default: `604800`

Mutable: `true`

Master only: `true`

### `hive_metastore_client_timeout_second`

TODO

Type: `long`

Default: `10`

Mutable: `true`

Master only: `false`

### `hive_stats_partition_sample_size`

Sample size for hive row count estimation.

Type: `int`

Default: `3000`

Mutable: `true`

Master only: `false`

### `hms_events_batch_size_per_rpc`

TODO

Type: `int`

Default: `500`

Mutable: `true`

Master only: `true`

### `hms_events_polling_interval_ms`

TODO

Type: `int`

Default: `10000`

Mutable: `false`

Master only: `true`

### `http_api_extra_base_path`

TODO

Type: `String`

Default: ``

Mutable: `false`

Master only: `false`

### `http_port`

Fe http port, currently all FE's http port must be same

Type: `int`

Default: `8030`

Mutable: `false`

Master only: `false`

### `https_port`

Fe https port, currently all FE's https port must be same

Type: `int`

Default: `8050`

Mutable: `false`

Master only: `false`

### `huge_table_auto_analyze_interval_in_millis`

This controls the minimum time interval for automatic ANALYZE on large tables. Within this interval,tables larger than huge_table_lower_bound_size_in_bytes are analyzed only once.

Type: `long`

Default: `43200000`

Mutable: `false`

Master only: `false`

### `huge_table_default_sample_rows`

This defines the number of sample percent for large tables when automatic sampling forlarge tables is enabled

Type: `int`

Default: `4194304`

Mutable: `false`

Master only: `false`

### `huge_table_lower_bound_size_in_bytes`

This defines the lower size bound for large tables. When enable_auto_sample is enabled, tables larger than this value will automatically collect statistics through sampling

Type: `long`

Default: `5368709120`

Mutable: `false`

Master only: `false`

### `iceberg_table_creation_interval_second`

TODO

Type: `long`

Default: `10`

Mutable: `true`

Master only: `true`

### `iceberg_table_creation_strict_mode`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `ignore_meta_check`

If true, non-master FE will ignore the meta data delay gap between Master FE and its self, even if the metadata delay gap exceeds this threshold. Non-master FE will still offer read service. This is helpful when you try to stop the Master FE for a relatively long time for some reason, but still wish the non-master FE can offer read service.

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `ignore_unknown_metadata_module`

Whether to ignore unknown modules in Image file. If true, metadata modules not in PersistMetaModules.MODULE_NAMES will be ignored and skipped. Default is false, if Image file contains unknown modules, Doris will throw exception. This parameter is mainly used in downgrade operation, old version can be compatible with new version Image file.

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `infodb_support_ext_catalog`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `insert_load_default_timeout_second`

Default timeout for insert load job, in seconds.

Type: `int`

Default: `14400`

Mutable: `true`

Master only: `true`

### `jdbc_drivers_dir`

The path to save jdbc drivers. When creating JDBC Catalog,if the specified driver file path is not an absolute path, Doris will find jars from this path

Type: `String`

Default: `null/jdbc_drivers`

Mutable: `false`

Master only: `false`

### `jetty_server_acceptors`

The number of acceptor threads for Jetty. Jetty's thread architecture model is very simple, divided into three thread pools: acceptor, selector and worker. The acceptor is responsible for accepting new connections, and then handing it over to the selector to process the unpacking of the HTTP message protocol, and finally the worker processes the request. The first two thread pools adopt a non-blocking model, and one thread can handle many socket reads and writes, so the number of thread pools is small. For most projects, only 1-2 acceptor threads are needed, 2 to 4 should be enough. The number of workers depends on the ratio of QPS and IO events of the application. The higher the QPS, or the higher the IO ratio, the more threads are waiting, and the more threads are required.

Type: `int`

Default: `2`

Mutable: `false`

Master only: `false`

### `jetty_server_max_http_header_size`

The maximum HTTP header size of Jetty, in bytes, the default value is 1MB.

Type: `int`

Default: `1048576`

Mutable: `false`

Master only: `false`

### `jetty_server_max_http_post_size`

The maximum HTTP POST size of Jetty, in bytes, the default value is 100MB.

Type: `int`

Default: `104857600`

Mutable: `false`

Master only: `false`

### `jetty_server_selectors`

The number of selector threads for Jetty.

Type: `int`

Default: `4`

Mutable: `false`

Master only: `false`

### `jetty_server_workers`

The number of worker threads for Jetty. 0 means using the default thread pool.

Type: `int`

Default: `0`

Mutable: `false`

Master only: `false`

### `jetty_threadPool_maxThreads`

The default maximum number of threads for jetty.

Type: `int`

Default: `400`

Mutable: `false`

Master only: `false`

### `jetty_threadPool_minThreads`

The default minimum number of threads for jetty.

Type: `int`

Default: `20`

Mutable: `false`

Master only: `false`

### `keep_scheduler_mtmv_task_when_job_deleted`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `key_store_alias`

The key store alias of FE https service

Type: `String`

Default: `doris_ssl_certificate`

Mutable: `false`

Master only: `false`

### `key_store_password`

The key store password of FE https service

Type: `String`

Default: ``

Mutable: `false`

Master only: `false`

### `key_store_path`

The key store path of FE https service

Type: `String`

Default: `null/conf/ssl/doris_ssl_certificate.keystore`

Mutable: `false`

Master only: `false`

### `key_store_type`

The key store type of FE https service

Type: `String`

Default: `JKS`

Mutable: `false`

Master only: `false`

### `label_clean_interval_second`

The clean interval of load job, in seconds. In each cycle, the expired history load job will be cleaned

Type: `int`

Default: `3600`

Mutable: `false`

Master only: `false`

### `label_keep_max_second`

Labels of finished or cancelled load jobs will be removed after this timeThe removed labels can be reused.

Type: `int`

Default: `259200`

Mutable: `true`

Master only: `true`

### `load_checker_interval_second`

The interval of load job scheduler, in seconds.

Type: `int`

Default: `5`

Mutable: `false`

Master only: `false`

### `locale`

TODO

Type: `String`

Default: `zh_CN.UTF-8`

Mutable: `false`

Master only: `false`

### `lock_reporting_threshold_ms`

TODO

Type: `long`

Default: `500`

Mutable: `false`

Master only: `false`

### `log_roll_size_mb`

The maximum file size of fe.log and fe.audit.log. After exceeding this size, the log file will be split

Type: `int`

Default: `1024`

Mutable: `false`

Master only: `false`

### `lower_case_table_names`

TODO

Type: `int`

Default: `0`

Mutable: `false`

Master only: `true`

### `master_sync_policy`

The sync policy of meta data log. If you only deploy one Follower FE, set this to `SYNC`. If you deploy more than 3 Follower FE, you can set this and the following `replica_sync_policy` to `WRITE_NO_SYNC`. See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html

Type: `String`

Default: `SYNC`

Options: `SYNC`, `NO_SYNC`, `WRITE_NO_SYNC`

Mutable: `false`

Master only: `false`

### `max_agent_task_threads_num`

Num of thread to handle agent task in agent task thread-pool

Type: `int`

Default: `4096`

Mutable: `false`

Master only: `true`

### `max_allowed_in_element_num_of_delete`

TODO

Type: `int`

Default: `1024`

Mutable: `true`

Master only: `true`

### `max_backend_heartbeat_failure_tolerance_count`

TODO

Type: `long`

Default: `1`

Mutable: `true`

Master only: `true`

### `max_backup_restore_job_num_per_db`

TODO

Type: `int`

Default: `10`

Mutable: `true`

Master only: `true`

### `max_balancing_tablets`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `true`

### `max_bdbje_clock_delta_ms`

The maximum clock skew between non-master FE to Master FE host, in milliseconds. This value is checked whenever a non-master FE establishes a connection to master FE via BDBJE. The connection is abandoned if the clock skew is larger than this value.

Type: `long`

Default: `5000`

Mutable: `false`

Master only: `false`

### `max_be_exec_version`

TODO

Type: `int`

Default: `3`

Mutable: `false`

Master only: `false`

### `max_broker_concurrency`

Maximal concurrency of broker scanners.

Type: `int`

Default: `10`

Mutable: `true`

Master only: `true`

### `max_bytes_per_broker_scanner`

TODO

Type: `long`

Default: `536870912000`

Mutable: `true`

Master only: `true`

### `max_bytes_sync_commit`

Max bytes that a sync job will commit. When receiving bytes larger than it, SyncJob will commit all data immediately. You should set it larger than canal memory and `min_bytes_sync_commit`.

Type: `long`

Default: `67108864`

Mutable: `true`

Master only: `true`

### `max_cbo_statistics_task_timeout_sec`

TODO

Type: `int`

Default: `300`

Mutable: `true`

Master only: `true`

### `max_clone_task_timeout_sec`

TODO

Type: `long`

Default: `7200`

Mutable: `true`

Master only: `true`

### `max_create_table_timeout_second`

Maximal waiting time for creating a table, in seconds.

Type: `int`

Default: `3600`

Mutable: `true`

Master only: `true`

### `max_distribution_pruner_recursion_depth`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `false`

### `max_dynamic_partition_num`

TODO

Type: `int`

Default: `500`

Mutable: `true`

Master only: `true`

### `max_error_tablet_of_broker_load`

TODO

Type: `int`

Default: `3`

Mutable: `true`

Master only: `true`

### `max_external_cache_loader_thread_pool_size`

TODO

Type: `int`

Default: `64`

Mutable: `false`

Master only: `false`

### `max_external_file_cache_num`

TODO

Type: `long`

Default: `100000`

Mutable: `false`

Master only: `false`

### `max_external_schema_cache_num`

TODO

Type: `long`

Default: `10000`

Mutable: `false`

Master only: `false`

### `max_hive_list_partition_num`

Max number of hive partition values to return while list partitions, -1 means no limitation.

Type: `short`

Default: `-1`

Mutable: `false`

Master only: `false`

### `max_hive_partition_cache_num`

TODO

Type: `long`

Default: `100000`

Mutable: `false`

Master only: `false`

### `max_hive_table_cache_num`

Max cache number of hive table to partition names list.

Type: `long`

Default: `1000`

Mutable: `false`

Master only: `false`

### `max_load_timeout_second`

Maximal timeout for load job, in seconds.

Type: `int`

Default: `259200`

Mutable: `true`

Master only: `true`

### `max_multi_partition_num`

TODO

Type: `int`

Default: `4096`

Mutable: `true`

Master only: `true`

### `max_mysql_service_task_threads_num`

The max number of task threads in MySQL service

Type: `int`

Default: `4096`

Mutable: `false`

Master only: `false`

### `max_pending_mtmv_scheduler_task_num`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `true`

### `max_point_query_retry_time`

TODO

Type: `int`

Default: `2`

Mutable: `true`

Master only: `false`

### `max_query_profile_num`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `false`

### `max_query_retry_time`

TODO

Type: `int`

Default: `1`

Mutable: `true`

Master only: `false`

### `max_remote_file_system_cache_num`

Max cache number of remote file system.

Type: `long`

Default: `100`

Mutable: `false`

Master only: `false`

### `max_replica_count_when_schema_change`

TODO

Type: `long`

Default: `100000`

Mutable: `true`

Master only: `true`

### `max_replication_num_per_tablet`

TODO

Type: `short`

Default: `32767`

Mutable: `true`

Master only: `true`

### `max_routine_load_job_num`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `true`

### `max_routine_load_task_concurrent_num`

TODO

Type: `int`

Default: `5`

Mutable: `true`

Master only: `true`

### `max_routine_load_task_num_per_be`

TODO

Type: `int`

Default: `5`

Mutable: `true`

Master only: `true`

### `max_running_mtmv_scheduler_task_num`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `true`

### `max_running_rollup_job_num_per_table`

TODO

Type: `int`

Default: `1`

Mutable: `true`

Master only: `true`

### `max_running_txn_num_per_db`

Maximum concurrent running txn num including prepare, commit txns under a single db.

Type: `int`

Default: `1000`

Mutable: `true`

Master only: `true`

### `max_same_name_catalog_trash_num`

TODO

Type: `int`

Default: `3`

Mutable: `true`

Master only: `true`

### `max_scheduling_tablets`

TODO

Type: `int`

Default: `2000`

Mutable: `true`

Master only: `true`

### `max_small_file_number`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `true`

### `max_small_file_size_bytes`

TODO

Type: `int`

Default: `1048576`

Mutable: `true`

Master only: `true`

### `max_stream_load_record_size`

Default max number of recent stream load record that can be stored in memory.

Type: `int`

Default: `5000`

Mutable: `true`

Master only: `true`

### `max_stream_load_timeout_second`

Maximal timeout for stream load job, in seconds.

Type: `int`

Default: `259200`

Mutable: `true`

Master only: `true`

### `max_sync_task_threads_num`

Maximal concurrent num of sync job.

Type: `int`

Default: `10`

Mutable: `false`

Master only: `false`

### `max_tolerable_backend_down_num`

TODO

Type: `int`

Default: `0`

Mutable: `true`

Master only: `true`

### `max_unfinished_load_job`

TODO

Type: `long`

Default: `1000`

Mutable: `true`

Master only: `true`

### `maximum_number_of_export_partitions`

The maximum number of partitions allowed by Export job

Type: `int`

Default: `2000`

Mutable: `true`

Master only: `false`

### `maximum_parallelism_of_export_job`

The maximum parallelism allowed by Export job

Type: `int`

Default: `50`

Mutable: `true`

Master only: `false`

### `maximum_tablets_of_outfile_in_export`

The maximum number of tablets allowed by an OutfileStatement in an ExportExecutorTask

Type: `int`

Default: `10`

Mutable: `true`

Master only: `false`

### `meta_delay_toleration_second`

The toleration delay time of meta data synchronization, in seconds. If the delay of meta data exceeds this value, non-master FE will stop offering service

Type: `int`

Default: `300`

Mutable: `false`

Master only: `false`

### `meta_dir`

The directory to save Doris meta data

Type: `String`

Default: `null/doris-meta`

Mutable: `false`

Master only: `false`

### `meta_publish_timeout_ms`

TODO

Type: `int`

Default: `1000`

Mutable: `false`

Master only: `false`

### `metadata_checkpoint_memory_threshold`

TODO

Type: `long`

Default: `70`

Mutable: `true`

Master only: `true`

### `min_backend_num_for_external_table`

TODO

Type: `int`

Default: `3`

Mutable: `true`

Master only: `false`

### `min_be_exec_version`

TODO

Type: `int`

Default: `0`

Mutable: `false`

Master only: `false`

### `min_bytes_indicate_replica_too_large`

TODO

Type: `long`

Default: `2147483648`

Mutable: `true`

Master only: `true`

### `min_bytes_per_broker_scanner`

Minimal bytes that a single broker scanner will read. When splitting file in broker load, if the size of split file is less than this value, it will not be split.

Type: `long`

Default: `67108864`

Mutable: `true`

Master only: `true`

### `min_bytes_sync_commit`

Min bytes that a sync job will commit. When receiving bytes less than it, SyncJob will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`.

Type: `long`

Default: `15728640`

Mutable: `true`

Master only: `true`

### `min_clone_task_timeout_sec`

TODO

Type: `long`

Default: `180`

Mutable: `true`

Master only: `true`

### `min_create_table_timeout_second`

Minimal waiting time for creating a table, in seconds.

Type: `int`

Default: `30`

Mutable: `true`

Master only: `true`

### `min_load_replica_num`

Minimal number of write successful replicas for load job.

Type: `short`

Default: `-1`

Mutable: `true`

Master only: `true`

### `min_load_timeout_second`

Minimal timeout for load job, in seconds.

Type: `int`

Default: `1`

Mutable: `true`

Master only: `true`

### `min_replication_num_per_tablet`

TODO

Type: `short`

Default: `1`

Mutable: `true`

Master only: `true`

### `min_sync_commit_size`

Min events that a sync job will commit. When receiving events less than it, SyncJob will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`.

Type: `long`

Default: `10000`

Mutable: `true`

Master only: `true`

### `min_version_count_indicate_replica_compaction_too_slow`

TODO

Type: `int`

Default: `200`

Mutable: `true`

Master only: `false`

### `multi_partition_name_prefix`

TODO

Type: `String`

Default: `p_`

Mutable: `true`

Master only: `true`

### `mysql_load_in_memory_record`

TODO

Type: `int`

Default: `20`

Mutable: `false`

Master only: `false`

### `mysql_load_server_secure_path`

TODO

Type: `String`

Default: ``

Mutable: `false`

Master only: `false`

### `mysql_load_thread_pool`

TODO

Type: `int`

Default: `4`

Mutable: `false`

Master only: `false`

### `mysql_nio_backlog_num`

The backlog number of mysql nio server. If you enlarge this value, you should enlarge the value in `/proc/sys/net/core/somaxconn` at the same time

Type: `int`

Default: `1024`

Mutable: `false`

Master only: `false`

### `mysql_service_io_threads_num`

The number of IO threads in MySQL service

Type: `int`

Default: `4`

Mutable: `false`

Master only: `false`

### `mysql_ssl_default_ca_certificate`

TODO

Type: `String`

Default: `null/mysql_ssl_default_certificate/ca_certificate.p12`

Mutable: `false`

Master only: `false`

### `mysql_ssl_default_ca_certificate_password`

TODO

Type: `String`

Default: `doris`

Mutable: `false`

Master only: `false`

### `mysql_ssl_default_server_certificate`

TODO

Type: `String`

Default: `null/mysql_ssl_default_certificate/server_certificate.p12`

Mutable: `false`

Master only: `false`

### `mysql_ssl_default_server_certificate_password`

TODO

Type: `String`

Default: `doris`

Mutable: `false`

Master only: `false`

### `mysqldb_replace_name`

To ensure compatibility with the MySQL ecosystem, Doris includes a built-in database called mysql. If this database conflicts with a user's own database, please modify this field to replace the name of the Doris built-in MySQL database with a different name.

Type: `String`

Default: `mysql`

Mutable: `true`

Master only: `false`

### `partition_in_memory_update_interval_secs`

TODO

Type: `int`

Default: `300`

Mutable: `false`

Master only: `true`

### `partition_rebalance_max_moves_num_per_selection`

TODO

Type: `int`

Default: `10`

Mutable: `true`

Master only: `true`

### `partition_rebalance_move_expire_after_access`

TODO

Type: `long`

Default: `600`

Mutable: `true`

Master only: `true`

### `period_analyze_simultaneously_running_task_num`

TODO

Type: `int`

Default: `1`

Mutable: `false`

Master only: `false`

### `period_of_auto_resume_min`

TODO

Type: `int`

Default: `5`

Mutable: `true`

Master only: `true`

### `plugin_dir`

The installation directory of the plugin

Type: `String`

Default: `null/plugins`

Mutable: `false`

Master only: `false`

### `plugin_enable`

Whether to enable the plugin

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `true`

### `point_query_timeout_ms`

The timeout of RPC for high concurrenty short circuit query

Type: `int`

Default: `10000`

Mutable: `false`

Master only: `false`

### `prefer_compute_node_for_external_table`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `priority_networks`

The preferred network address. If FE has multiple network addresses, this configuration can be used to specify the preferred network address. This is a semicolon-separated list, each element is a CIDR representation of the network address

Type: `String`

Default: ``

Mutable: `false`

Master only: `false`

### `proxy_auth_enable`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `proxy_auth_magic_prefix`

TODO

Type: `String`

Default: `x@8`

Mutable: `false`

Master only: `false`

### `publish_topic_info_interval_ms`

TODO

Type: `int`

Default: `30000`

Mutable: `true`

Master only: `true`

### `publish_version_interval_ms`

The interval of publish task trigger thread, in milliseconds

Type: `int`

Default: `10`

Mutable: `false`

Master only: `true`

### `publish_version_timeout_second`

Maximal waiting time for all publish version tasks of one transaction to be finished, in seconds.

Type: `int`

Default: `30`

Mutable: `true`

Master only: `true`

### `publish_wait_time_second`

Waiting time for one transaction changing to "at least one replica success", in seconds.If time exceeds this, and for each tablet it has at least one replica publish successful, then the load task will be successful.

Type: `int`

Default: `300`

Mutable: `true`

Master only: `true`

### `pull_request_id`

TODO

Type: `int`

Default: `0`

Mutable: `true`

Master only: `false`

### `qe_max_connection`

Maximal number of connections of MySQL server per FE.

Type: `int`

Default: `1024`

Mutable: `false`

Master only: `false`

### `qe_slow_log_ms`

The threshold of slow query, in milliseconds. If the response time of a query exceeds this threshold, it will be recorded in audit log.

Type: `long`

Default: `5000`

Mutable: `true`

Master only: `false`

### `query_colocate_join_memory_limit_penalty_factor`

Colocate join PlanFragment instance memory limit penalty factor.

Type: `int`

Default: `1`

Mutable: `true`

Master only: `false`

### `query_metadata_name_ids_timeout`

When querying the information_schema.metadata_name_ids table, the time used to obtain all tables in one database

Type: `long`

Default: `3`

Mutable: `true`

Master only: `false`

### `query_port`

The port of FE MySQL server

Type: `int`

Default: `9030`

Mutable: `false`

Master only: `false`

### `recover_with_empty_tablet`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `recover_with_skip_missing_version`

TODO

Type: `String`

Default: `disable`

Mutable: `true`

Master only: `true`

### `remote_fragment_exec_timeout_ms`

TODO

Type: `long`

Default: `30000`

Mutable: `true`

Master only: `false`

### `repair_slow_replica`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `replica_ack_policy`

The replica ack policy of bdbje. See: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html

Type: `String`

Default: `SIMPLE_MAJORITY`

Options: `ALL`, `NONE`, `SIMPLE_MAJORITY`

Mutable: `false`

Master only: `false`

### `replica_sync_policy`

Same as `master_sync_policy`

Type: `String`

Default: `SYNC`

Options: `SYNC`, `NO_SYNC`, `WRITE_NO_SYNC`

Mutable: `false`

Master only: `false`

### `report_queue_size`

TODO

Type: `int`

Default: `100`

Mutable: `true`

Master only: `true`

### `rpc_port`

The port of FE thrift server

Type: `int`

Default: `9020`

Mutable: `false`

Master only: `false`

### `s3_compatible_object_storages`

TODO

Type: `String`

Default: `s3,oss,cos,bos`

Mutable: `false`

Master only: `false`

### `schedule_batch_size`

TODO

Type: `int`

Default: `50`

Mutable: `true`

Master only: `true`

### `schedule_slot_num_per_hdd_path`

TODO

Type: `int`

Default: `4`

Mutable: `true`

Master only: `true`

### `schedule_slot_num_per_ssd_path`

TODO

Type: `int`

Default: `8`

Mutable: `true`

Master only: `true`

### `scheduler_job_task_max_saved_count`

TODO

Type: `int`

Default: `20`

Mutable: `false`

Master only: `false`

### `scheduler_mtmv_job_expired`

TODO

Type: `long`

Default: `86400`

Mutable: `true`

Master only: `true`

### `scheduler_mtmv_task_expired`

TODO

Type: `long`

Default: `86400`

Mutable: `true`

Master only: `true`

### `show_details_for_unaccessible_tablet`

When set to true, if a query is unable to select a healthy replica, the detailed information of all the replicas of the tablet, including the specific reason why they are unqueryable, will be printed out.

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `skip_compaction_slower_replica`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `skip_localhost_auth_check`

TODO

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `small_file_dir`

TODO

Type: `String`

Default: `null/small_files`

Mutable: `false`

Master only: `false`

### `spark_dpp_version`

Default spark dpp version

Type: `String`

Default: `1.2-SNAPSHOT`

Mutable: `false`

Master only: `false`

### `spark_home_default_dir`

Spark dir for Spark Load

Type: `String`

Default: `null/lib/spark2x`

Mutable: `true`

Master only: `true`

### `spark_launcher_log_dir`

Spark launcher log dir

Type: `String`

Default: `null/log/spark_launcher_log`

Mutable: `false`

Master only: `false`

### `spark_load_checker_interval_second`

The interval of spark load job scheduler, in seconds.

Type: `int`

Default: `60`

Mutable: `false`

Master only: `false`

### `spark_load_default_timeout_second`

Default timeout for spark load job, in seconds.

Type: `int`

Default: `86400`

Mutable: `true`

Master only: `true`

### `spark_resource_path`

Spark dependencies dir for Spark Load

Type: `String`

Default: ``

Mutable: `false`

Master only: `false`

### `ssl_force_client_auth`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `statistics_simultaneously_running_task_num`

TODO

Type: `int`

Default: `5`

Mutable: `false`

Master only: `false`

### `statistics_sql_mem_limit_in_bytes`

TODO

Type: `long`

Default: `2147483648`

Mutable: `false`

Master only: `false`

### `statistics_sql_parallel_exec_instance_num`

TODO

Type: `int`

Default: `1`

Mutable: `false`

Master only: `false`

### `stats_cache_size`

TODO

Type: `long`

Default: `100000`

Mutable: `false`

Master only: `false`

### `storage_flood_stage_left_capacity_bytes`

TODO

Type: `long`

Default: `1073741824`

Mutable: `true`

Master only: `true`

### `storage_flood_stage_usage_percent`

TODO

Type: `int`

Default: `95`

Mutable: `true`

Master only: `true`

### `storage_high_watermark_usage_percent`

TODO

Type: `int`

Default: `85`

Mutable: `true`

Master only: `true`

### `storage_min_left_capacity_bytes`

TODO

Type: `long`

Default: `2147483648`

Mutable: `true`

Master only: `true`

### `stream_load_default_precommit_timeout_second`

Default pre-commit timeout for stream load job, in seconds.

Type: `int`

Default: `3600`

Mutable: `true`

Master only: `true`

### `stream_load_default_timeout_second`

Default timeout for stream load job, in seconds.

Type: `int`

Default: `259200`

Mutable: `true`

Master only: `true`

### `streaming_label_keep_max_second`

For some high frequency load jobs such as INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK, DELETERemove the finished job or task if expired. The removed job or task can be reused.

Type: `int`

Default: `43200`

Mutable: `true`

Master only: `true`

### `sync_checker_interval_second`

The interval of sync job scheduler, in seconds.

Type: `int`

Default: `5`

Mutable: `false`

Master only: `false`

### `sync_commit_interval_second`

Maximal intervals between two sync job's commits.

Type: `long`

Default: `10`

Mutable: `true`

Master only: `true`

### `sync_image_timeout_second`

The timeout for FE Follower/Observer synchronizing an image file from the FE Master, can be adjusted by the user on the size of image file in the ${meta_dir}/image and the network environment between nodes. The default values is 300.

Type: `int`

Default: `300`

Mutable: `true`

Master only: `true`

### `sys_log_delete_age`

The maximum survival time of the FE log file. After exceeding this time, the log file will be deleted. Supported formats include: 7d, 10h, 60m, 120s

Type: `String`

Default: `7d`

Mutable: `false`

Master only: `false`

### `sys_log_dir`

The path of the FE log file, used to store fe.log

Type: `String`

Default: `null/log`

Mutable: `false`

Master only: `false`

### `sys_log_enable_compress`

enable compression for FE log file

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `sys_log_level`

The level of FE log

Type: `String`

Default: `INFO`

Options: `INFO`, `WARN`, `ERROR`, `FATAL`

Mutable: `false`

Master only: `false`

### `sys_log_mode`

The output mode of FE log, and NORMAL mode is the default output mode, which means the logs are synchronized and contain location information. BRIEF mode is synchronized and does not contain location information. ASYNC mode is asynchronous and does not contain location information. The performance of the three log output modes increases in sequence

Type: `String`

Default: `NORMAL`

Options: `NORMAL`, `BRIEF`, `ASYNC`

Mutable: `false`

Master only: `false`

### `sys_log_roll_interval`

The split cycle of the FE log file

Type: `String`

Default: `DAY`

Options: `DAY`, `HOUR`

Mutable: `false`

Master only: `false`

### `sys_log_roll_num`

The maximum number of FE log files. After exceeding this number, the oldest log file will be deleted

Type: `int`

Default: `10`

Mutable: `false`

Master only: `false`

### `sys_log_verbose_modules`

Verbose module. The VERBOSE level log is implemented by the DEBUG level of log4j. If set to `org.apache.doris.catalog`, the DEBUG log of the class under this package will be printed.

Type: `String[]`

Default: `[]`

Mutable: `false`

Master only: `false`

### `table_name_length_limit`

TODO

Type: `int`

Default: `64`

Mutable: `true`

Master only: `true`

### `table_stats_health_threshold`

TODO

Type: `int`

Default: `80`

Mutable: `false`

Master only: `false`

### `tablet_checker_interval_ms`

TODO

Type: `long`

Default: `20000`

Mutable: `false`

Master only: `true`

### `tablet_create_timeout_second`

Maximal waiting time for creating a single replica, in seconds. eg. if you create a table with #m tablets and #n replicas for each tablet, the create table request will run at most (m * n * tablet_create_timeout_second) before timeout

Type: `int`

Default: `2`

Mutable: `true`

Master only: `true`

### `tablet_delete_timeout_second`

The same meaning as `tablet_create_timeout_second`, but used when delete a tablet.

Type: `int`

Default: `2`

Mutable: `true`

Master only: `true`

### `tablet_further_repair_max_times`

TODO

Type: `int`

Default: `5`

Mutable: `true`

Master only: `true`

### `tablet_further_repair_timeout_second`

TODO

Type: `long`

Default: `1200`

Mutable: `true`

Master only: `true`

### `tablet_rebalancer_type`

TODO

Type: `String`

Default: `BeLoad`

Mutable: `false`

Master only: `true`

### `tablet_repair_delay_factor_second`

TODO

Type: `long`

Default: `60`

Mutable: `true`

Master only: `true`

### `tablet_schedule_interval_ms`

TODO

Type: `long`

Default: `1000`

Mutable: `false`

Master only: `true`

### `tablet_stat_update_interval_second`

TODO

Type: `int`

Default: `60`

Mutable: `false`

Master only: `false`

### `thrift_backlog_num`

The backlog number of thrift server. If you enlarge this value, you should enlarge the value in `/proc/sys/net/core/somaxconn` at the same time

Type: `int`

Default: `1024`

Mutable: `false`

Master only: `false`

### `thrift_client_timeout_ms`

The connection timeout of thrift client, in milliseconds. 0 means no timeout.

Type: `int`

Default: `0`

Mutable: `false`

Master only: `false`

### `thrift_server_max_worker_threads`

The max worker threads of thrift server

Type: `int`

Default: `4096`

Mutable: `false`

Master only: `false`

### `thrift_server_type`

TODO

Type: `String`

Default: `THREAD_POOL`

Mutable: `false`

Master only: `false`

### `tmp_dir`

The directory to save Doris temp data

Type: `String`

Default: `null/temp_dir`

Mutable: `false`

Master only: `false`

### `token_generate_period_hour`

TODO

Type: `int`

Default: `12`

Mutable: `false`

Master only: `true`

### `token_queue_size`

TODO

Type: `int`

Default: `6`

Mutable: `false`

Master only: `true`

### `trace_export_url`

TODO

Type: `String`

Default: `http://127.0.0.1:9411/api/v2/spans`

Mutable: `false`

Master only: `false`

### `trace_exporter`

TODO

Type: `String`

Default: `zipkin`

Mutable: `false`

Master only: `false`

### `transaction_clean_interval_second`

The clean interval of transaction, in seconds. In each cycle, the expired history transaction will be cleaned

Type: `int`

Default: `30`

Mutable: `false`

Master only: `false`

### `txn_rollback_limit`

The max txn number which bdbje can rollback when trying to rejoin the group. If the number of rollback txn is larger than this value, bdbje will not be able to rejoin the group, and you need to clean up bdbje data manually.

Type: `int`

Default: `100`

Mutable: `false`

Master only: `false`

### `use_compact_thrift_rpc`

TODO

Type: `boolean`

Default: `true`

Mutable: `true`

Master only: `false`

### `use_fuzzy_session_variable`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `use_mysql_bigint_for_largeint`

Whether to use mysql's bigint type to return Doris's largeint type

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `false`

### `use_new_tablet_scheduler`

TODO

Type: `boolean`

Default: `true`

Mutable: `false`

Master only: `false`

### `used_capacity_percent_max_diff`

The max diff of disk capacity used percent between BE. It is used for calculating load score of a backend.

Type: `double`

Default: `0.3`

Mutable: `true`

Master only: `true`

### `using_old_load_usage_pattern`

TODO

Type: `boolean`

Default: `false`

Mutable: `true`

Master only: `true`

### `valid_version_count_delta_ratio_between_replicas`

TODO

Type: `double`

Default: `0.5`

Mutable: `true`

Master only: `true`

### `virtual_node_number`

When file cache is enabled, the number of virtual nodes of each node in the consistent hash algorithm. The larger the value, the more uniform the distribution of the hash algorithm, but it will increase the memory overhead.

Type: `int`

Default: `2048`

Mutable: `true`

Master only: `false`

### `with_k8s_certs`

TODO

Type: `boolean`

Default: `false`

Mutable: `false`

Master only: `false`

### `yarn_client_path`

Yarn client path

Type: `String`

Default: `null/lib/yarn-client/hadoop/bin/yarn`

Mutable: `false`

Master only: `false`

### `yarn_config_dir`

Yarn config path

Type: `String`

Default: `null/lib/yarn-config`

Mutable: `false`

Master only: `false`


