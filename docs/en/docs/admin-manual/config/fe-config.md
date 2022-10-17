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

### max_dynamic_partition_num

Default：500

IsMutable：true

MasterOnly：true

Used to limit the maximum number of partitions that can be created when creating a dynamic partition table,  to avoid creating too many partitions at one time. The number is determined by "start" and "end" in the dynamic partition parameters.. 

### grpc_max_message_size_bytes

Default：1G

Used to set the initial flow window size of the GRPC client channel, and also used to max message size.  When the result set is large, you may need to increase this value.

### min_replication_num_per_tablet

Default: 1

Used to set minimal number of replication per tablet.

### max_replication_num_per_tablet

Default: 32767

Used to set maximal number of replication per tablet.

### enable_outfile_to_local

Default：false

Whether to allow the outfile function to export the results to the local disk.

### enable_access_file_without_broker

Default：false

IsMutable：true

MasterOnly：true

This config is used to try skip broker when access bos or other cloud storage via broker

### enable_bdbje_debug_mode

Default：false

If set to true, FE will be started in BDBJE debug mode

### enable_http_server_v2

Default：The default is true after the official 0.14.0 version is released, and the default is false before

HTTP Server V2 is implemented by SpringBoot. It uses an architecture that separates the front and back ends. Only when httpv2 is enabled can users use the new front-end UI interface.

### jetty_server_acceptors

Default：2

### jetty_server_selectors

Default：4

### jetty_server_workers

Default：0

With the above three parameters, Jetty's thread architecture model is very simple, divided into acceptors, selectors and workers three thread pools. Acceptors are responsible for accepting new connections, and then hand them over to selectors to process the unpacking of the HTTP message protocol, and finally workers process the request. The first two thread pools adopt a non-blocking model, and one thread can handle the read and write of many sockets, so the number of thread pools is small.

For most projects, only 1-2 acceptors threads are required, and 2 to 4 selectors threads are sufficient. Workers are obstructive business logic, often have more database operations, and require a large number of threads. The specific number depends on the proportion of QPS and IO events of the application. The higher the QPS, the more threads are required, the higher the proportion of IO, the more threads waiting, and the more total threads required.

Worker thread pool is not set by default, set according to your needs


### jetty_server_max_http_post_size

Default：`100 * 1024 * 1024`  （100MB）

This is the maximum number of bytes of the file uploaded by the put or post method, the default value: 100MB

### jetty_server_max_http_header_size

Default：10240  （10K）

http header size configuration parameter, the default value is 10K

### frontend_address

Status: Deprecated, not recommended use. This parameter may be deleted later Type: string Description: Explicitly set the IP address of FE instead of using *InetAddress.getByName* to get the IP address. Usually in *InetAddress.getByName* When the expected results cannot be obtained. Only IP address is supported, not hostname. Default value: 0.0.0.0

### default_max_filter_ratio

Default：0

IsMutable：true

MasterOnly：true

Maximum percentage of data that can be filtered (due to reasons such as data is irregularly) , The default value is 0.

### default_db_data_quota_bytes

Default：1PB

IsMutable：true

MasterOnly：true

Used to set the default database data quota size. To set the quota size of a single database, you can use:

```
Set the database data quota, the unit is:B/K/KB/M/MB/G/GB/T/TB/P/PB
ALTER DATABASE db_name SET DATA QUOTA quota;
View configuration
show data （Detail：HELP SHOW DATA）
```

### default_db_replica_quota_size

Default: 1073741824

IsMutable：true

MasterOnly：true

Used to set the default database replica quota. To set the quota size of a single database, you can use: 

```
Set the database replica quota
ALTER DATABASE db_name SET REPLICA QUOTA quota;
View configuration
show data （Detail：HELP SHOW DATA）
```

### enable_batch_delete_by_default

Default：false

IsMutable：true

MasterOnly：true

Whether to add a delete sign column when create unique table

### recover_with_empty_tablet

Default：false

IsMutable：true

MasterOnly：true

 In some very special circumstances, such as code bugs, or human misoperation, etc., all replicas of some tablets may be lost. In this case, the data has been substantially lost. However, in some scenarios, the business still hopes to ensure that the query will not report errors even if there is data loss, and reduce the perception of the user layer. At this point, we can use the blank Tablet to fill the missing replica to ensure that the query can be executed normally.

Set to true so that Doris will automatically use blank replicas to fill tablets which all replicas have been damaged or missing

### max_allowed_in_element_num_of_delete

Default：1024

IsMutable：true

MasterOnly：true

This configuration is used to limit element num of InPredicate in delete statement.

### cache_result_max_row_count

Default：3000

IsMutable：true

MasterOnly：false

In order to avoid occupying too much memory, the maximum number of rows that can be cached is 2000 by default. If this threshold is exceeded, the cache cannot be set

### cache_last_version_interval_second

Default：900

IsMutable：true

MasterOnly：false

The time interval of the latest partitioned version of the table refers to the time interval between the data update and the current version. It is generally set to 900 seconds, which distinguishes offline and real-time import

### cache_enable_partition_mode

Default：true

IsMutable：true

MasterOnly：false

When this switch is turned on, the query result set will be cached according to the partition. If the interval between the query table partition time and the query time is less than cache_last_version_interval_second, the result set will be cached according to the partition.

Part of the data will be obtained from the cache and some data from the disk when querying, and the data will be merged and returned to the client.

### cache_enable_sql_mode

Default：true

IsMutable：true

MasterOnly：false

If this switch is turned on, the SQL query result set will be cached. If the interval between the last visit version time in all partitions of all tables in the query is greater than cache_last_version_interval_second, and the result set is less than cache_result_max_row_count, the result set will be cached, and the next same SQL will hit the cache

If set to true, fe will enable sql result caching. This option is suitable for offline data update scenarios

|                        | case1 | case2 | case3 | case4 |
| ---------------------- | ----- | ----- | ----- | ----- |
| enable_sql_cache       | false | true  | true  | false |
| enable_partition_cache | false | false | true  | true  |

### min_clone_task_timeout_sec  和 max_clone_task_timeout_sec

Default：Minimum 3 minutes, maximum two hours

IsMutable：true

MasterOnly：true

Type: long Description: Used to control the maximum timeout of a clone task. The unit is second. Default value: 7200 Dynamic modification: yes

Can cooperate with `mix_clone_task_timeout_sec` to control the maximum and minimum timeout of a clone task. Under normal circumstances, the timeout of a clone task is estimated by the amount of data and the minimum transfer rate (5MB/s). In some special cases, these two configurations can be used to set the upper and lower bounds of the clone task timeout to ensure that the clone task can be completed successfully.

### agent_task_resend_wait_time_ms

Default：5000

IsMutable：true

MasterOnly：true

This configuration will decide whether to resend agent task when create_time for agent_task is set, only when current_time - create_time > agent_task_resend_wait_time_ms can ReportHandler do resend agent task.

This configuration is currently mainly used to solve the problem of repeated sending of `PUBLISH_VERSION` agent tasks. The current default value of this configuration is 5000, which is an experimental value.

Because there is a certain time delay between submitting agent tasks to AgentTaskQueue and submitting to be, Increasing the value of this configuration can effectively solve the problem of repeated sending of agent tasks,

But at the same time, it will cause the submission of failed or failed execution of the agent task to be executed again for an extended period of time

### enable_odbc_table

Default：false

IsMutable：true

MasterOnly：true

Whether to enable the ODBC table, it is not enabled by default. You need to manually configure it when you use it. This parameter can be set by: ADMIN SET FRONTEND CONFIG("key"="value")

### enable_spark_load

Default：false

IsMutable：true

MasterOnly：true

Whether to enable spark load temporarily, it is not enabled by default

### disable_storage_medium_check

Default：false

IsMutable：true

MasterOnly：true

If disable_storage_medium_check is true, ReportHandler would not check tablet's storage medium and disable storage cool down function, the default value is false. You can set the value true when you don't care what the storage medium of the tablet is.

### drop_backend_after_decommission

Default：false

IsMutable：true

MasterOnly：true

1. This configuration is used to control whether the system drops the BE after successfully decommissioning the BE. If true, the BE node will be deleted after the BE is successfully offline. If false, after the BE successfully goes offline, the BE will remain in the DECOMMISSION state, but will not be dropped.

   This configuration can play a role in certain scenarios. Assume that the initial state of a Doris cluster is one disk per BE node. After running for a period of time, the system has been vertically expanded, that is, each BE node adds 2 new disks. Because Doris currently does not support data balancing among the disks within the BE, the data volume of the initial disk may always be much higher than the data volume of the newly added disk. At this time, we can perform manual inter-disk balancing by the following operations:

   1. Set the configuration item to false.
   2. Perform a decommission operation on a certain BE node. This operation will migrate all data on the BE to other nodes.
   3. After the decommission operation is completed, the BE will not be dropped. At this time, cancel the decommission status of the BE. Then the data will start to balance from other BE nodes back to this node. At this time, the data will be evenly distributed to all disks of the BE.
   4. Perform steps 2 and 3 for all BE nodes in sequence, and finally achieve the purpose of disk balancing for all nodes

### period_of_auto_resume_min

Default：5 （s）

IsMutable：true

MasterOnly：true

Automatically restore the cycle of Routine load

### max_tolerable_backend_down_num

Default：0

IsMutable：true

MasterOnly：true

As long as one BE is down, Routine Load cannot be automatically restored 

### check_java_version

Default：true

Doris will check whether the compiled and run Java versions are compatible, if not, it will throw a Java version mismatch exception message and terminate the startup

### max_running_rollup_job_num_per_table

Default：1

IsMutable：true

MasterOnly：true

Control the concurrency limit of Rollup jobs

### dynamic_partition_enable

Default：true

IsMutable：true

MasterOnly：true

Whether to enable dynamic partition scheduler, enabled by default

### dynamic_partition_check_interval_seconds

Default：600 （s）

IsMutable：true

MasterOnly：true

Decide how often to check dynamic partition

### disable_cluster_feature

Default：true

IsMutable：true

The multi cluster feature will be deprecated in version 0.12 ，set this config to true will disable all operations related to cluster feature, include:
        create/drop cluster
        add free backend/add backend to cluster/decommission cluster balance
        change the backends num of cluster
        link/migration db

### force_do_metadata_checkpoint

Default：false

IsMutable：true

MasterOnly：true

If set to true, the checkpoint thread will make the checkpoint regardless of the jvm memory used percent

### metadata_checkpoint_memory_threshold

Default：60  （60%）

IsMutable：true

MasterOnly：true

 If the jvm memory used percent(heap or old mem pool) exceed this threshold, checkpoint thread will  not work to avoid OOM.

### max_distribution_pruner_recursion_depth

Default：100

IsMutable：true

MasterOnly：false

This will limit the max recursion depth of hash distribution pruner.
      eg: where a in (5 elements) and b in (4 elements) and c in (3 elements) and d in (2 elements).
      a/b/c/d are distribution columns, so the recursion depth will be 5 * 4 * 3 * 2 = 120, larger than 100,
      So that distribution pruner will no work and just return all buckets.
      Increase the depth can support distribution pruning for more elements, but may cost more CPU.

### max_backup_restore_job_num_per_db

Default: 10

This configuration is mainly used to control the number of backup/restore tasks recorded in each database.

### using_old_load_usage_pattern

Default：false

IsMutable：true

MasterOnly：true

If set to true, the insert stmt with processing error will still return a label to user.  And user can use this label to check the load job's status. The default value is false, which means if insert operation encounter errors,  exception will be thrown to user client directly without load label.

### small_file_dir

Default：DORIS_HOME_DIR/small_files

Save small files

### max_small_file_size_bytes

Default：1M

IsMutable：true

MasterOnly：true

The max size of a single file store in SmallFileMgr

### max_small_file_number

Default：100

IsMutable：true

MasterOnly：true

The max number of files store in SmallFileMgr

### max_routine_load_task_num_per_be

Default：5

IsMutable：true

MasterOnly：true

the max concurrent routine load task num per BE.  This is to limit the num of routine load tasks sending to a BE, and it should also less than BE config 'routine_load_thread_pool_size'(default 10), which is the routine load task thread pool size on BE.

### max_routine_load_task_concurrent_num

Default：5

IsMutable：true

MasterOnly：true

the max concurrent routine load task num of a single routine load job

### max_routine_load_job_num

Default：100

the max routine load job num, including NEED_SCHEDULED, RUNNING, PAUSE

### max_running_txn_num_per_db

Default：100

IsMutable：true

MasterOnly：true

This configuration is mainly used to control the number of concurrent load jobs of the same database.

When there are too many load jobs running in the cluster, the newly submitted load jobs may report errors:

```text
current running txns on db xxx is xx, larger than limit xx
```

When this error is encountered, it means that the load jobs currently running in the cluster exceeds the configuration value. At this time, it is recommended to wait on the business side and retry the load jobs.

Generally it is not recommended to increase this configuration value. An excessively high number of concurrency may cause excessive system load

### enable_metric_calculator

Default：true

If set to true, metric collector will be run as a daemon timer to collect metrics at fix interval

### report_queue_size

Default： 100

IsMutable：true

MasterOnly：true

 This threshold is to avoid piling up too many report task in FE, which may cause OOM exception.  In some large Doris cluster, eg: 100 Backends with ten million replicas, a tablet report may cost  several seconds after some modification of metadata(drop partition, etc..). And one Backend will report tablets info every 1 min, so unlimited receiving reports is unacceptable. we will optimize the processing speed of tablet report in future, but now, just discard the report if queue size exceeding limit.
    Some online time cost:
       1. disk report: 0-1 msta
       2. sk report: 0-1 ms
       3. tablet report
       4. 10000 replicas: 200ms

### partition_rebalance_max_moves_num_per_selection

Default：10

IsMutable：true

MasterOnly：true

Valid only if use PartitionRebalancer，

### partition_rebalance_move_expire_after_access

Default：600   (s)

IsMutable：true

MasterOnly：true

Valid only if use PartitionRebalancer. If this changed, cached moves will be cleared 

### tablet_rebalancer_type

Default：BeLoad

MasterOnly：true

Rebalancer type(ignore case): BeLoad, Partition. If type parse failed, use BeLoad as default

### max_balancing_tablets

Default：100

IsMutable：true

MasterOnly：true

if the number of balancing tablets in TabletScheduler exceed max_balancing_tablets, no more balance check 

### max_scheduling_tablets

Default：2000

IsMutable：true

MasterOnly：true

if the number of scheduled tablets in TabletScheduler exceed max_scheduling_tablets skip checking.

### disable_balance

Default：false

IsMutable：true

MasterOnly：true

if set to true, TabletScheduler will not do balance.

### balance_load_score_threshold

Default：0.1 (10%)

IsMutable：true

MasterOnly：true

the threshold of cluster balance score, if a backend's load score is 10% lower than average score,  this backend will be marked as LOW load, if load score is 10% higher than average score, HIGH load  will be marked

### schedule_slot_num_per_path

Default：2

the default slot number per path in tablet scheduler , remove this config and dynamically adjust it by clone task statistic

### tablet_repair_delay_factor_second

Default：60 （s）

IsMutable：true

MasterOnly：true

the factor of delay time before deciding to repair tablet.  if priority is VERY_HIGH, repair it immediately.

-  HIGH, delay tablet_repair_delay_factor_second * 1;
-  NORMAL: delay tablet_repair_delay_factor_second * 2;
-  LOW: delay tablet_repair_delay_factor_second * 3;

### es_state_sync_interval_second

Default：10

fe will call es api to get es index shard info every es_state_sync_interval_secs

### disable_hadoop_load

Default：false

IsMutable：true

MasterOnly：true

Load using hadoop cluster will be deprecated in future. Set to true to disable this kind of load.

### db_used_data_quota_update_interval_secs

Default：300 (s)

IsMutable：true

MasterOnly：true

For better data load performance, in the check of whether the amount of data used by the database before data load exceeds the quota, we do not calculate the amount of data already used by the database in real time, but obtain the periodically updated value of the daemon thread.

This configuration is used to set the time interval for updating the value of the amount of data used by the database

### disable_load_job

Default：false

IsMutable：true

MasterOnly：true

if this is set to true

- all pending load job will failed when call begin txn api
-  all prepare load job will failed when call commit txn api
-  all committed load job will waiting to be published 

### catalog_try_lock_timeout_ms

Default：5000  （ms）

IsMutable：true

The tryLock timeout configuration of catalog lock.  Normally it does not need to change, unless you need to test something.

### max_query_retry_time

Default：1

IsMutable：true

The number of query retries.  A query may retry if we encounter RPC exception and no result has been sent to user.  You may reduce this number to avoid Avalanche disaster

### remote_fragment_exec_timeout_ms

Default：5000  （ms）

IsMutable：true

The timeout of executing async remote fragment.  In normal case, the async remote fragment will be executed in a short time. If system are under high load condition，try to set this timeout longer.

### enable_local_replica_selection

Default：false

IsMutable：true

If set to true, Planner will try to select replica of tablet on same host as this Frontend. This may reduce network transmission in following case: 

-  N hosts with N Backends and N Frontends deployed. 
- The data has N replicas. 
-  High concurrency queries are syyuyuient to all Frontends evenly 
-  In this case, all Frontends can only use local replicas to do the query. If you want to allow fallback to nonlocal replicas when no local replicas available, set enable_local_replica_selection_fallback to true.

### enable_local_replica_selection_fallback

Default：false

IsMutable：true

Used with enable_local_replica_selection. If the local replicas is not available, fallback to the nonlocal replicas.

### max_unfinished_load_job

Default：1000

IsMutable：true

MasterOnly：true

 Max number of load jobs, include PENDING、ETL、LOADING、QUORUM_FINISHED. If exceed this number, load job is not allowed to be submitted

### max_bytes_per_broker_scanner

Default：`3 * 1024 * 1024 * 1024L`  （3G）

IsMutable：true

MasterOnly：true

Max bytes a broker scanner can process in one broker load job. Commonly, each Backends has one broker scanner.

### enable_auth_check

Default：true

if set to false, auth check will be disable, in case some goes wrong with the new privilege system.

### tablet_stat_update_interval_second

Default：300（5min）

update interval of tablet stat , All frontends will get tablet stat from all backends at each interval

### storage_flood_stage_usage_percent  

Default：95 （95%）

IsMutable：true

MasterOnly：true

###  storage_flood_stage_left_capacity_bytes

Default：

	storage_flood_stage_usage_percent  : 95  (95%)
	
	storage_flood_stage_left_capacity_bytes :  1 * 1024 * 1024 * 1024 (1GB)

IsMutable：true

MasterOnly：true

If capacity of disk reach the 'storage_flood_stage_usage_percent' and  'storage_flood_stage_left_capacity_bytes', the following operation will be rejected: 

1. load job
2. restore job

### storage_high_watermark_usage_percent

Default：85  (85%)

IsMutable：true

MasterOnly：true

### storage_min_left_capacity_bytes

Default： `2 * 1024 * 1024 * 1024`  (2GB)

IsMutable：true

MasterOnly：true

 'storage_high_watermark_usage_percent' limit the max capacity usage percent of a Backend storage path.  'storage_min_left_capacity_bytes' limit the minimum left capacity of a Backend storage path.  If both limitations are reached, this storage path can not be chose as tablet balance destination. But for tablet recovery, we may exceed these limit for keeping data integrity as much as possible.

### backup_job_default_timeout_ms

Default：86400 * 1000  (1day)

IsMutable：true

MasterOnly：true

default timeout of backup job

### with_k8s_certs

Default：false

If use k8s deploy manager locally, set this to true and prepare the certs files

### dpp_hadoop_client_path

Default：/lib/hadoop-client/hadoop/bin/hadoop

### dpp_bytes_per_reduce

Default：`100 * 1024 * 1024L` (100M)

### dpp_default_cluster

Default：palo-dpp

### dpp_default_config_str

Default：{
            hadoop_configs : 'mapred.job.priority=NORMAL;mapred.job.map.capacity=50;mapred.job.reduce.capacity=50;mapred.hce.replace.streaming=false;abaci.long.stored.job=true;dce.shuffle.enable=false;dfs.client.authserver.force_stop=true;dfs.client.auth.method=0'
        }

### dpp_config_str

Default：{
            palo-dpp : {
                    hadoop_palo_path : '/dir',
                    hadoop_configs : 'fs.default.name=hdfs://host:port;mapred.job.tracker=host:port;hadoop.job.ugi=user,password'
                }
        }

### enable_deploy_manager

Default：disable

 Set to true if you deploy Palo using thirdparty deploy manager Valid options are:

- disable:    no deploy manager 
-  k8s:        Kubernetes 
- ambari:     Ambari 
- local:      Local File (for test or Boxer2 BCC version)

### enable_token_check

Default：true

For forward compatibility, will be removed later. check token when download image file.

### expr_depth_limit

Default：3000

IsMutable：true

Limit on the depth of an expr tree.  Exceed this limit may cause long analysis time while holding db read lock.  Do not set this if you know what you are doing

### expr_children_limit

Default：10000

IsMutable：true

Limit on the number of expr children of an expr tree.  Exceed this limit may cause long analysis time while holding database read lock.  

### proxy_auth_magic_prefix

Default：x@8

### proxy_auth_enable

Default：false

### meta_publish_timeout_ms

Default：1000 （ms）

The default user resource publishing timeout 

### disable_colocate_balance

Default：false

IsMutable：true

MasterOnly：true

This configs can set to true to disable the automatic colocate tables's relocate and balance.  If 'disable_colocate_balance' is set to true,   ColocateTableBalancer will not relocate and balance colocate tables.
      **Attention**: 

- Under normal circumstances, there is no need to turn off balance at all. 
-  Because once the balance is turned off, the unstable colocate table may not be restored 
-  Eventually the colocate plan cannot be used when querying.

### query_colocate_join_memory_limit_penalty_factor

Default：1

IsMutable：true

colocote join PlanFragment instance的memory_limit = exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num) 

### max_connection_scheduler_threads_num

Default：4096

Maximal number of thread in connection-scheduler-pool.

### qe_max_connection

Default：1024

Maximal number of connections per FE.

### check_consistency_default_timeout_second

Default：600 （10 minutes）

IsMutable：true

MasterOnly：true

Default timeout of a single consistency check task. Set long enough to fit your tablet size

### consistency_check_start_time

Default：23

IsMutable：true

MasterOnly：true

Consistency checker will run from *consistency_check_start_time* to *consistency_check_end_time*.

If the two times are the same, no consistency check will be triggered.

### consistency_check_end_time

Default：23

IsMutable：true

MasterOnly：true

Consistency checker will run from *consistency_check_start_time* to *consistency_check_end_time*.

If the two times are the same, no consistency check will be triggered.

### export_tablet_num_per_task

Default：5

IsMutable：true

MasterOnly：true

Number of tablets per export query plan

### export_task_default_timeout_second

Default：2 * 3600   （2 hour）

IsMutable：true

MasterOnly：true

Default timeout of export jobs.

### export_running_job_num_limit

Default：5

IsMutable：true

MasterOnly：true

Limitation of the concurrency of running export jobs.  Default is 5.  0 is unlimited

### export_checker_interval_second

Default：5

Export checker's running interval.

### default_load_parallelism

Default: 1

IsMutable：true

MasterOnly：true

Default parallelism of the broker load execution plan on a single node.
If the user to set the parallelism when the broker load is submitted, this parameter will be ignored.

### max_broker_concurrency

Default：10

IsMutable：true

MasterOnly：true

Maximal concurrency of broker scanners.  

### min_bytes_per_broker_scanner

Default：67108864L (64M)

IsMutable：true

MasterOnly：true

Minimum bytes that a single broker scanner will read.  

### catalog_trash_expire_second

Default：86400L (1day)

IsMutable：true

MasterOnly：true

After dropping database(table/partition), you can recover it by using RECOVER stmt. And this specifies the maximal data retention time. After time, the data will be deleted permanently.

### storage_cooldown_second

Default：`30 * 24 * 3600L`  （30 days）

When create a table(or partition), you can specify its storage medium(HDD or SSD). If set to SSD, this specifies the default duration that tablets will stay on SSD.  After that, tablets will be moved to HDD automatically.  You can set storage cooldown time in CREATE TABLE stmt.

### default_storage_medium

Default：HDD

When create a table(or partition), you can specify its storage medium(HDD or SSD). If not set, this specifies the default medium when creat.

### max_backend_down_time_second

Default：3600  （1 hours）

IsMutable：true

MasterOnly：true

If a backend is down for *max_backend_down_time_second*, a BACKEND_DOWN event will be triggered. 

### alter_table_timeout_second

Default：86400   （1 day）

IsMutable：true

MasterOnly：true

Maximal timeout of ALTER TABLE request. Set long enough to fit your table data size. 

### capacity_used_percent_high_water

Default：0.75  （75%）

IsMutable：true

MasterOnly：true

The high water of disk capacity used percent. This is used for calculating load score of a backend

### clone_distribution_balance_threshold

Default：0.2 

IsMutable：true

MasterOnly：true

Balance threshold of num of replicas in Backends.

### clone_capacity_balance_threshold

Default：0.2  

IsMutable：true

MasterOnly：true

Balance threshold of data size in BE.
      The balance algorithm is: 

1. Calculate the average used capacity(AUC) of the entire cluster. (total data size / total backends num)
2. The high water level is (AUC * (1 + clone_capacity_balance_threshold)) 
3. The low water level is (AUC * (1 - clone_capacity_balance_threshold)) 
4. The Clone checker will try to move replica from high water level BE to low water level BE.

### replica_delay_recovery_second

Default：0

IsMutable：true

MasterOnly：true

the minimal delay seconds between a replica is failed and fe try to recovery it using clone.

### clone_high_priority_delay_second

Default：0

IsMutable：true

MasterOnly：true

HIGH priority clone job's delay trigger time.

### clone_normal_priority_delay_second 

Default：300 （5min）

IsMutable：true

MasterOnly：true

NORMAL priority clone job's delay trigger time

### clone_low_priority_delay_second

Default：600 （10min）

IsMutable：true

MasterOnly：true

LOW priority clone job's delay trigger time. A clone job contains a tablet which need to be cloned(recovery or migration).  If the priority is LOW, it will be delayed *clone_low_priority_delay_second*  after the job creation and then be executed.  This is to avoid a large number of clone jobs running at same time only because a host is down for a short time. 
 **NOTICE** that this config(and *clone_normal_priority_delay_second* as well)  will not work if it's smaller then *clone_checker_interval_second*

### clone_max_job_num

Default：100

IsMutable：true

MasterOnly：true

Concurrency of LOW priority clone jobs.  Concurrency of High priority clone jobs is currently unlimited.

### clone_job_timeout_second

Default：7200  (2小时)

IsMutable：true

MasterOnly：true

Default timeout of a single clone job. Set long enough to fit your replica size.  The larger the replica data size is, the more time is will cost to finish clone

### clone_checker_interval_second

Default：300 （5min）

Clone checker's running interval

### tablet_delete_timeout_second

Default：2

IsMutable：true

MasterOnly：true

Same meaning as *tablet_create_timeout_second*, but used when delete a tablet.

### async_loading_load_task_pool_size

Default：10

IsMutable：false

MasterOnly：true

The loading_load task executor pool size. This pool size limits the max running loading_load tasks.

Currently, it only limits the loading_load task of broker load

### async_pending_load_task_pool_size

Default：10

IsMutable：false

MasterOnly：true

The pending_load task executor pool size. This pool size limits the max running pending_load tasks.

Currently, it only limits the pending_load task of broker load and spark load.

It should be less than 'max_running_txn_num_per_db'

### async_load_task_pool_size

Default：10

IsMutable：false

MasterOnly：true

This configuration is just for compatible with old version, this config has been replaced by async_loading_load_task_pool_size, it will be removed in the future.

###  disable_show_stream_load

Default：false

IsMutable：true

MasterOnly：true

Whether to disable show stream load and clear stream load records in memory.

### max_stream_load_record_size

Default：5000

IsMutable：true

MasterOnly：true

Default max number of recent stream load record that can be stored in memory.

### fetch_stream_load_record_interval_second

Default：120

IsMutable：true

MasterOnly：true

fetch stream load record interval.

### `enable_single_replica_stream_load`

Default：false

IsMutable：true

MasterOnly：true

Whether to enable the function of single replica load for stream load.

### `enable_single_replica_broker_load`

Default：false

IsMutable：true

MasterOnly：true

Whether to enable the function of single replica load for broker load.

### `enable_single_replica_insert`

Default：false

IsMutable：true

MasterOnly：true

Whether to enable the function of single replica load for insert.

### desired_max_waiting_jobs

Default：100

IsMutable：true

MasterOnly：true

Default number of waiting jobs for routine load and version 2 of load ， This is a desired number.  In some situation, such as switch the master, the current number is maybe more than desired_max_waiting_jobs.

### yarn_config_dir

Default：PaloFe.DORIS_HOME_DIR + "/lib/yarn-config"

Default yarn config file directory ，Each time before running the yarn command, we need to check that the  config file exists under this path, and if not, create them.


### yarn_client_path

Default：DORIS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"

Default yarn client path

### spark_launcher_log_dir

Default： sys_log_dir + "/spark_launcher_log"

The specified spark launcher log dir

### spark_resource_path

Default：none

Default spark dependencies path

### spark_home_default_dir

Default：DORIS_HOME_DIR + "/lib/spark2x"

Default spark home dir

### spark_load_default_timeout_second

Default：86400  (1天)

IsMutable：true

MasterOnly：true

Default spark load timeout

### spark_dpp_version

Default：1.0.0

Default spark dpp version

### hadoop_load_default_timeout_second

Default：86400 * 3   (3天)

IsMutable：true

MasterOnly：true

Default hadoop load timeout

### min_load_timeout_second

Default：1 （1s）

IsMutable：true

MasterOnly：true

Min stream load timeout applicable to all type of load

### max_stream_load_timeout_second

Default：259200 （3天）

IsMutable：true

MasterOnly：true

This configuration is specifically used to limit timeout setting for stream load. It is to prevent that failed stream load transactions cannot be canceled within a short time because of the user's large timeout setting

### max_load_timeout_second

Default：259200 （3天）

IsMutable：true

MasterOnly：true

Max load timeout applicable to all type of load except for stream load

### stream_load_default_timeout_second

Default：600 （s）

IsMutable：true

MasterOnly：true

Default stream load and streaming mini load timeout

### insert_load_default_timeout_second

Default：3600    （1 hour）

IsMutable：true

MasterOnly：true

Default insert load timeout

### mini_load_default_timeout_second

Default：3600    （1 hour）

IsMutable：true

MasterOnly：true

Default non-streaming mini load timeout

### broker_load_default_timeout_second

Default：14400   （4 hour）

IsMutable：true

MasterOnly：true

Default broker load timeout

### broker_timeout_ms

Default：10000   （10s）

Default broker RPC timeout

### load_running_job_num_limit

Default：0

IsMutable：true

MasterOnly：true

The number of loading tasks is limited, the default is 0, no limit

### load_input_size_limit_gb

Default：0

IsMutable：true

MasterOnly：true

The size of the data entered by the Load job, the default is 0, unlimited

### delete_thread_num

Default：10

Concurrency of delete jobs.

### load_etl_thread_num_normal_priority

Default：10

Concurrency of NORMAL priority etl load jobs. Do not change this if you know what you are doing.

### load_etl_thread_num_high_priority

Default：3

Concurrency of HIGH priority etl load jobs. Do not change this if you know what you are doing

### load_pending_thread_num_normal_priority

Default：10

Concurrency of NORMAL priority pending load jobs.  Do not change this if you know what you are doing.

### load_pending_thread_num_high_priority

Default：3

 Concurrency of HIGH priority pending load jobs. Load job priority is defined as HIGH or NORMAL.  All mini batch load jobs are HIGH priority, other types of load jobs are NORMAL priority.  Priority is set to avoid that a slow load job occupies a thread for a long time.  This is just a internal optimized scheduling policy.  Currently, you can not specified the job priority manually, and do not change this if you know what you are doing.

### load_checker_interval_second

Default：5 （s）

The load scheduler running interval. A load job will transfer its state from PENDING to LOADING to FINISHED.  The load scheduler will transfer load job from PENDING to LOADING while the txn callback will transfer load job from LOADING to FINISHED.  So a load job will cost at most one interval to finish when the concurrency has not reached the upper limit.

### max_layout_length_per_row

Default：100000

IsMutable：true

MasterOnly：true

Maximal memory layout length of a row. default is 100 KB. In BE, the maximal size of a RowBlock is 100MB(Configure as max_unpacked_row_block_size in be.conf). And each RowBlock contains 1024 rows. So the maximal size of a row is approximately 100 KB.
     eg.
     schema: k1(int), v1(decimal), v2(varchar(2000))
     then the memory layout length of a row is: 4(int) + 16(decimal) + 2000(varchar) = 2020 (Bytes)
     See memory layout length of all types, run 'help create table' in mysql-client.
     If you want to increase this number to support more columns in a row, you also need to increase the
     max_unpacked_row_block_size in be.conf. But the performance impact is unknown.

### load_straggler_wait_second

Default：300

IsMutable：true

MasterOnly：true

Maximal wait seconds for straggler node in load
     eg.
           there are 3 replicas A, B, C
           load is already quorum finished(A,B) at t1 and C is not finished
           if (current_time - t1) > 300s, then palo will treat C as a failure node
           will call transaction manager to commit the transaction and tell transaction manager
           that C is failed
     
      This is also used when waiting for publish tasks
      this parameter is the default value for all job and the DBA could specify it for separate job

### thrift_server_max_worker_threads

Default：4096

The thrift server max worker threads

### publish_version_interval_ms

Default：10 （ms）

minimal intervals between two publish version action

### publish_version_timeout_second

Default：30 （s）

IsMutable：true

MasterOnly：true

Maximal waiting time for all publish version tasks of one transaction to be finished

### max_create_table_timeout_second

Default：60 （s）

IsMutable：true

MasterOnly：true

In order not to wait too long for create table(index), set a max timeout.

### tablet_create_timeout_second

Default：1（s）

IsMutable：true

MasterOnly：true

Maximal waiting time for creating a single replica.
      eg.
           if you create a table with #m tablets and #n replicas for each tablet,
           the create table request will run at most (m * n * tablet_create_timeout_second) before timeout.

### max_mysql_service_task_threads_num

Default：4096

When FeEstarts the MySQL server based on NIO model, the number of threads responsible for Task events. Only `mysql_service_nio_enabled` is true takes effect.

### rewrite_count_distinct_to_bitmap_hll

Default: true

This variable is a session variable, and the session level takes effect.

- Type: boolean
- Description: **Only for the table of the AGG model**, when the variable is true, when the user query contains aggregate functions such as count(distinct c1), if the type of the c1 column itself is bitmap, count distnct will be rewritten It is bitmap_union_count(c1). When the type of the c1 column itself is hll, count distinct will be rewritten as hll_union_agg(c1) If the variable is false, no overwriting occurs..

### cluster_id

Default：-1

node(FE or BE) will be considered belonging to the same Palo cluster if they have same cluster id.  Cluster id is usually a random integer generated when master FE start at first time. You can also specify one.

### auth_token

Default：empty

Cluster token used for internal authentication.

### cluster_name

Default： Apache doris

Cluster name will be shown as the title of web page

### mysql_service_io_threads_num

Default：4

When FeEstarts the MySQL server based on NIO model, the number of threads responsible for IO events. Only `mysql_service_nio_enabled` is true takes effect.

### mysql_service_nio_enabled

Default：true

Whether FE starts the MySQL server based on NiO model. It is recommended to turn off this option when the query connection is less than 1000 or the concurrency scenario is not high

### query_port

Default：9030

FE MySQL server port

### rpc_port

Default：9020

FE Thrift Server port

### thrift_server_type

This configuration represents the service model used by The Thrift Service of FE, is of type String and is case-insensitive.

If this parameter is 'SIMPLE', then the 'TSimpleServer' model is used, which is generally not suitable for production and is limited to test use.

If the parameter is 'THREADED', then the 'TThreadedSelectorServer' model is used, which is a non-blocking I/O model, namely the master-slave Reactor model, which can timely respond to a large number of concurrent connection requests and performs well in most scenarios.

If this parameter is `THREAD_POOL`, then the `TThreadPoolServer` model is used, the model for blocking I/O model, use the thread pool to handle user connections, the number of simultaneous connections are limited by the number of thread pool, if we can estimate the number of concurrent requests in advance, and tolerant enough thread resources cost, this model will have a better performance, the service model is used by default

### thrift_backlog_num

Default：1024

The backlog_num for thrift server ， When you enlarge this backlog_num, you should ensure it's value larger than the linux /proc/sys/net/core/somaxconn config

### thrift_client_timeout_ms

Default：0

The connection timeout and socket timeout config for thrift server.

The value for thrift_client_timeout_ms is set to be zero to prevent read timeout.

### mysql_nio_backlog_num

Default：1024

The backlog_num for mysql nio server, When you enlarge this backlog_num, you should enlarge the value in the linux /proc/sys/net/core/somaxconn file at the same time

### http_port

Default：8030

HTTP bind port. Defaults to 8030

### http_api_extra_base_path

In some deployment environments, user need to specify an additional base path as the unified prefix of the HTTP API. This parameter is used by the user to specify additional prefixes. After setting, user can get the parameter value through the `GET /api/basepath` interface. And the new UI will also try to get this base path first to assemble the URL. Only valid when `enable_http_server_v2` is true.

The default is empty, that is, not set

### max_bdbje_clock_delta_ms

Default：5000 （5s）

Set the maximum acceptable clock skew between non-master FE to Master FE host. This value is checked whenever a non-master FE establishes a connection to master FE via BDBJE. The connection is abandoned if the clock skew is larger than this value.

### ignore_meta_check

Default：false

IsMutable：true

If true, non-master FE will ignore the meta data delay gap between Master FE and its self,  even if the metadata delay gap exceeds *meta_delay_toleration_second*.  Non-master FE will still offer read service.
This is helpful when you try to stop the Master FE for a relatively long time for some reason,  but still wish the non-master FE can offer read service.

### metadata_failure_recovery

Default：false

If true, FE will reset bdbje replication group(that is, to remove all electable nodes info)  and is supposed to start as Master.  If all the electable nodes can not start, we can copy the meta data to another node and set this config to true to try to restart the FE..

### priority_networks

Default：none

Declare a selection strategy for those servers have many ips.  Note that there should at most one ip match this list.  this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24 ， If no ip match this rule, will choose one randomly..

### txn_rollback_limit

Default：100

the max txn number which bdbje can rollback when trying to rejoin the group

### max_agent_task_threads_num

Default：4096

MasterOnly：true

max num of thread to handle agent task in agent task thread-pool.

### heartbeat_mgr_blocking_queue_size

Default：1024

MasterOnly：true

blocking queue size to store heartbeat task in heartbeat_mgr.

### heartbeat_mgr_threads_num

Default：8

MasterOnly：true

num of thread to handle heartbeat events in heartbeat_mgr.

### bdbje_replica_ack_timeout_second

Default：10  (s)

The replica ack timeout when writing to bdbje ， When writing some relatively large logs, the ack time may time out, resulting in log writing failure.  At this time, you can increase this value appropriately.

### bdbje_lock_timeout_second

Default：1

The lock timeout of bdbje operation， If there are many LockTimeoutException in FE WARN log, you can try to increase this value

### bdbje_heartbeat_timeout_second

Default：30

The heartbeat timeout of bdbje between master and follower. the default is 30 seconds, which is same as default value in bdbje. If the network is experiencing transient problems, of some unexpected long java GC annoying you,  you can try to increase this value to decrease the chances of false timeouts

### replica_ack_policy

Default：SIMPLE_MAJORITY

OPTION：ALL, NONE, SIMPLE_MAJORITY

Replica ack policy of bdbje. more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html

### replica_sync_policy

Default：SYNC

选项：SYNC, NO_SYNC, WRITE_NO_SYNC

Follower FE sync policy of bdbje.

### master_sync_policy

Default：SYNC

选项：SYNC, NO_SYNC, WRITE_NO_SYNC

Master FE sync policy of bdbje. If you only deploy one Follower FE, set this to 'SYNC'. If you deploy more than 3 Follower FE,  you can set this and the following 'replica_sync_policy' to WRITE_NO_SYNC.  more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html

### meta_delay_toleration_second

Default：300 （5分钟）

Non-master FE will stop offering service  if meta data delay gap exceeds *meta_delay_toleration_second*

### edit_log_roll_num

Default：50000

IsMutable：true

MasterOnly：true

Master FE will save image every *edit_log_roll_num* meta journals.

### edit_log_port

Default：9010

bdbje port

### edit_log_type

Default：BDB

Edit log type.
      BDB: write log to bdbje
      LOCAL: deprecated..

### tmp_dir

Default：PaloFe.DORIS_HOME_DIR + "/temp_dir"

temp dir is used to save intermediate results of some process, such as backup and restore process.  file in this dir will be cleaned after these process is finished.

### meta_dir

Default：DORIS_HOME_DIR + "/doris-meta"

Type: string Description: Doris meta data will be saved here.The storage of this dir is highly recommended as to be:

- High write performance (SSD)
- Safe (RAID）

### custom_config_dir

Default：PaloFe.DORIS_HOME_DIR + "/conf"

Configure the location of the `fe_custom.conf` file. The default is in the `conf/` directory.

In some deployment environments, the `conf/` directory may be overwritten due to system upgrades. This will cause the user modified configuration items to be overwritten. At this time, we can store `fe_custom.conf` in another specified directory to prevent the configuration file from being overwritten.

### log_roll_size_mb

Default：1024  （1G）

The max size of one sys log and audit log

### sys_log_dir

Default：PaloFe.DORIS_HOME_DIR + "/log"

sys_log_dir:
      This specifies FE log dir. FE will produces 2 log files:
       fe.log:      all logs of FE process.
       fe.warn.log  all WARNING and ERROR log of FE process.

### sys_log_level

Default：INFO

log level：INFO, WARNING, ERROR, FATAL

### sys_log_roll_num

Default：10

Maximal FE log files to be kept within an sys_log_roll_interval. default is 10, which means there will be at most 10 log files in a day

### sys_log_verbose_modules

Default：{}

Verbose modules. VERBOSE level is implemented by log4j DEBUG level.

eg：
    sys_log_verbose_modules = org.apache.doris.catalog
    This will only print debug log of files in package org.apache.doris.catalog and all its sub packages.

### sys_log_roll_interval

Default：DAY

sys_log_roll_interval:

- DAY:  log suffix is  yyyyMMdd
- HOUR: log suffix is  yyyyMMddHH

### sys_log_delete_age

Default：7d

sys_log_delete_age:
       default is 7 days, if log's last modify time is 7 days ago, it will be deleted.

      support format:
                7d      7 day
                10h     10 hours
                60m     60 min
                120s    120 seconds


### audit_log_dir

Default：DORIS_HOME_DIR + "/log"

audit_log_dir：
           This specifies FE audit log dir..
            Audit log fe.audit.log contains all requests with related infos such as user, host, cost, status, etc

### audit_log_roll_num

Default：90

Maximal FE audit log files to be kept within an audit_log_roll_interval. 

### audit_log_modules

Default：{"slow_query", "query", "load", "stream_load"}

Slow query contains all queries which cost exceed *qe_slow_log_ms*

### qe_slow_log_ms

Default：5000 （5 seconds）

If the response time of a query exceed this threshold, it will be recorded in audit log as slow_query.

### audit_log_roll_interval

Default：DAY

DAY:  logsuffix is ：yyyyMMdd
HOUR: logsuffix is ：yyyyMMddHH

### audit_log_delete_age

Default：30d

default is 30 days, if log's last modify time is 30 days ago, it will be deleted.

      support format:
                7d      7 day
                10h     10 hours
                60m     60 min
                120s    120 seconds

### plugin_dir

Default：DORIS_HOME + "/plugins

plugin install directory

### plugin_enable

Default:true

IsMutable：true

MasterOnly：true

Whether the plug-in is enabled, enabled by default

### label_keep_max_second

Default：`3 * 24 * 3600`  (3 days)

IsMutable：true

MasterOnly：true

labels of finished or cancelled load jobs will be removed after *label_keep_max_second* ， The removed labels can be reused.  Set a short time will lower the FE memory usage.  (Because all load jobs' info is kept in memory before being removed)

In the case of high concurrent writes, if there is a large backlog of jobs and call frontend service failed, check the log. If the metadata write takes too long to lock, you can adjust this value to 12 hours, or 6 hours less

### streaming_label_keep_max_second

Default：43200 （12 hour）

IsMutable：true

MasterOnly：true

For some high-frequency load work, such as: INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK. If it expires, delete the completed job or task.

### history_job_keep_max_second

Default：`7 * 24 * 3600`   （7 day）

IsMutable：true

MasterOnly：true

The max keep time of some kind of jobs. like schema change job and rollup job.

### label_clean_interval_second

Default：4 * 3600  （4 hour）

Load label cleaner will run every *label_clean_interval_second* to clean the outdated jobs.

### delete_info_keep_max_second

Default：`3 * 24 * 3600`  (3day)

IsMutable：true

MasterOnly：false

Delete all deleteInfo older than *delete_info_keep_max_second* , Setting a shorter time will reduce FE memory usage and image file size. (Because all deleteInfo is stored in memory and image files before being deleted)

### transaction_clean_interval_second

Default：30

the transaction will be cleaned after transaction_clean_interval_second seconds if the transaction is visible or aborted  we should make this interval as short as possible and each clean cycle as soon as possible


### default_max_query_instances

The default value when user property max_query_instances is equal or less than 0. This config is used to limit the max number of instances for a user. This parameter is less than or equal to 0 means unlimited.

The default value is -1

### use_compact_thrift_rpc

Default: true

Whether to use compressed format to send query plan structure. After it is turned on, the size of the query plan structure can be reduced by about 50%, thereby avoiding some "send fragment timeout" errors.
However, in some high-concurrency small query scenarios, the concurrency may be reduced by about 10%.

### enable_force_drop_redundant_replica

Default: false

Dynamically configured: true

Only for Master FE: true

If set to true, the system will immediately drop redundant replicas in the tablet scheduling logic. This may cause some load jobs that are writing to the corresponding replica to fail, but it will speed up the balance and repair speed of the tablet.
When there are a large number of replicas waiting to be balanced or repaired in the cluster, you can try to set this config to speed up the balance and repair of replicas at the expense of partial load success rate.

### repair_slow_replica

Default: false

IsMutable：true

MasterOnly: true

If set to true, the replica with slower compaction will be automatically detected and migrated to other machines. The detection condition is that the version count of the fastest replica exceeds the value of `min_version_count_indicate_replica_compaction_too_slow`, and the ratio of the version count difference from the fastest replica exceeds the value of `valid_version_count_delta_ratio_between_replicas`

### colocate_group_relocate_delay_second

Default: 1800

Dynamically configured: true

Only for Master FE: true

The relocation of a colocation group may involve a large number of tablets moving within the cluster. Therefore, we should use a more conservative strategy to avoid relocation of colocation groups as much as possible.
Reloaction usually occurs after a BE node goes offline or goes down. This parameter is used to delay the determination of BE node unavailability. The default is 30 minutes, i.e., if a BE node recovers within 30 minutes, relocation of the colocation group will not be triggered.

### allow_replica_on_same_host

Default: false

Dynamically configured: false

Only for Master FE: false

Whether to allow multiple replicas of the same tablet to be distributed on the same host. This parameter is mainly used for local testing, to facilitate building multiple BEs to test certain multi-replica situations. Do not use it for non-test environments.

### min_version_count_indicate_replica_compaction_too_slow

Default: 200

Dynamically configured: true

Only for Master FE: false

The version count threshold used to judge whether replica compaction is too slow

### valid_version_count_delta_ratio_between_replicas

Default: 0.5

Dynamically configured: true

Only for Master FE: true

The valid ratio threshold of the difference between the version count of the slowest replica and the fastest replica. If `repair_slow_replica` is set to true, it is used to determine whether to repair the slowest replica

### min_bytes_indicate_replica_too_large

Default: `2 * 1024 * 1024 * 1024` (2G)

Dynamically configured: true

Only for Master FE: true

The data size threshold used to judge whether replica is too large

### skip_compaction_slower_replica

Default: true

Dynamically configured: true

Only for Master FE: false

If set to true, the compaction slower replica will be skipped when select get queryable replicas

### sync_commit_interval_second

The maximum time interval for committing transactions. If there is still data in the channel that has not been submitted after this time, the consumer will notify the channel to submit the transaction.

Default: 10 (seconds)

Is it possible to configure dynamically: true

Whether it is a configuration item unique to the Master FE node: true

### min_sync_commit_size

The minimum number of events that must be satisfied to commit a transaction. If the number of events received by Fe is less than it, it will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 10000 events. If you want to modify this configuration, please make sure that this value is smaller than the `canal.instance.memory.buffer.size` configuration on the canal side (default 16384), otherwise Fe will try to get the queue length longer than the store before ack More events cause the store queue to block until it times out.

Default: 10000

Is it possible to configure dynamically: true

Whether it is a configuration item unique to the Master FE node: true

### min_bytes_sync_commit

The minimum data size required to commit a transaction. If the data size received by Fe is smaller than it, it will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 15MB, if you want to modify this configuration, please make sure this value is less than the product of `canal.instance.memory.buffer.size` and `canal.instance.memory.buffer.memunit` on the canal side (default 16MB), otherwise Before the ack, Fe will try to obtain data that is larger than the store space, causing the store queue to block until it times out.

Default: `15*1024*1024` (15M)

Is it possible to configure dynamically: true

Whether it is a configuration item unique to the Master FE node: true

### max_bytes_sync_commit

 The maximum number of threads in the data synchronization job thread pool. There is only one thread pool in the entire FE, which is used to process all data synchronization tasks in the FE that send data to the BE. The implementation of the thread pool is in the `SyncTaskPool` class.

Default: 10

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: false

### backend_rpc_timeout_ms

 Timeout millisecond for Fe sending rpc request to BE

Default: 60000

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: true

### `bdbje_reserved_disk_bytes`

The desired upper limit on the number of bytes of reserved space to retain in a replicated JE Environment.

Default: 1073741824

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: false

### `be_exec_version`

Used to define the serialization format for passing blocks between fragments.

Sometimes some of our code changes will change the data format of the block. In order to make the BE compatible with each other during the rolling upgrade process, we need to issue a data version from the FE to decide what format to send the data in.

Specifically, for example, there are 2 BEs in the cluster, one of which can support the latest $v_1$ after being upgraded, while the other only supports $v_0$. At this time, since the FE has not been upgraded yet, $v_0 is issued uniformly. $, BE interact in the old data format. After all BEs are upgraded, we will upgrade FE. At this time, the new FE will issue $v_1$, and the cluster will be uniformly switched to the new data format.


The default value is `max_be_exec_version`. If there are special needs, we can manually set the format version to lower, but it should not be lower than `min_be_exec_version`.

Note that we should always keep the value of this variable between `HeartbeatServer::min_be_exec_version` and `HeartbeatServer::max_be_exec_version` for all BEs. (That is to say, if a cluster that has completed the update needs to be downgraded, it should ensure the order of downgrading FE and then downgrading BE, or manually lower the variable in the settings and downgrade BE)

### `max_be_exec_version`

The latest data version currently supported, cannot be modified, and should be consistent with the `HeartbeatServer::max_be_exec_version` in the BE of the matching version.

### `min_be_exec_version`

The oldest data version currently supported, which cannot be modified, should be consistent with the `HeartbeatServer::min_be_exec_version` in the BE of the matching version.
