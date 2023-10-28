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

Default：100

the max txn number which bdbje can rollback when trying to rejoin the group

### `grpc_threadmgr_threads_nums`

Default: 4096

Num of thread to handle grpc events in grpc_threadmgr.

#### `bdbje_replica_ack_timeout_second`

Default：10  (s)

The replica ack timeout when writing to bdbje ， When writing some relatively large logs, the ack time may time out, resulting in log writing failure.  At this time, you can increase this value appropriately.

#### `bdbje_lock_timeout_second`

Default：5

The lock timeout of bdbje operation， If there are many LockTimeoutException in FE WARN log, you can try to increase this value

#### `bdbje_heartbeat_timeout_second`

Default：30

The heartbeat timeout of bdbje between master and follower. the default is 30 seconds, which is same as default value in bdbje. If the network is experiencing transient problems, of some unexpected long java GC annoying you,  you can try to increase this value to decrease the chances of false timeouts

#### `replica_ack_policy`

Default：SIMPLE_MAJORITY

OPTION：ALL, NONE, SIMPLE_MAJORITY

Replica ack policy of bdbje. more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.ReplicaAckPolicy.html

#### `replica_sync_policy`

Default：SYNC

选项：SYNC, NO_SYNC, WRITE_NO_SYNC

Follower FE sync policy of bdbje.

#### `master_sync_policy`

Default：SYNC

选项：SYNC, NO_SYNC, WRITE_NO_SYNC

Master FE sync policy of bdbje. If you only deploy one Follower FE, set this to 'SYNC'. If you deploy more than 3 Follower FE,  you can set this and the following 'replica_sync_policy' to WRITE_NO_SYNC.  more info, see: http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Durability.SyncPolicy.html

#### `bdbje_reserved_disk_bytes`

The desired upper limit on the number of bytes of reserved space to retain in a replicated JE Environment.

Default: 1073741824

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: false

#### `ignore_meta_check`

Default：false

IsMutable：true

If true, non-master FE will ignore the meta data delay gap between Master FE and its self,  even if the metadata delay gap exceeds *meta_delay_toleration_second*.  Non-master FE will still offer read service.
This is helpful when you try to stop the Master FE for a relatively long time for some reason,  but still wish the non-master FE can offer read service.

#### `meta_delay_toleration_second`

Default：300 （5 min）

Non-master FE will stop offering service  if meta data delay gap exceeds *meta_delay_toleration_second*

#### `edit_log_port`

Default：9010

bdbje port

#### `edit_log_type`

Default：BDB

Edit log type.
BDB: write log to bdbje
LOCAL: deprecated..

#### `edit_log_roll_num`

Default：50000

IsMutable：true

MasterOnly：true

Master FE will save image every *edit_log_roll_num* meta journals.

#### `force_do_metadata_checkpoint`

Default：false

IsMutable：true

MasterOnly：true

If set to true, the checkpoint thread will make the checkpoint regardless of the jvm memory used percent

#### `metadata_checkpoint_memory_threshold`

Default：60  （60%）

IsMutable：true

MasterOnly：true

If the jvm memory used percent(heap or old mem pool) exceed this threshold, checkpoint thread will  not work to avoid OOM.

#### `max_same_name_catalog_trash_num`

It is used to set the maximum number of meta information with the same name in the catalog recycle bin. When the maximum value is exceeded, the earliest deleted meta trash will be completely deleted and cannot be recovered. 0 means not to keep objects of the same name. < 0 means no limit.

Note: The judgment of metadata with the same name will be limited to a certain range. For example, the judgment of the database with the same name will be limited to the same cluster, the judgment of the table with the same name will be limited to the same database (with the same database id), the judgment of the partition with the same name will be limited to the same database (with the same database id) and the same table (with the same table) same table id).

Default: 3

Is it possible to dynamically configure: true

Is it a configuration item unique to the Master FE node: true

#### `cluster_id`

Default：-1

node(FE or BE) will be considered belonging to the same Palo cluster if they have same cluster id.  Cluster id is usually a random integer generated when master FE start at first time. You can also specify one.

#### `heartbeat_mgr_blocking_queue_size`

Default：1024

MasterOnly：true

blocking queue size to store heartbeat task in heartbeat_mgr.

#### `heartbeat_mgr_threads_num`

Default：8

MasterOnly：true

num of thread to handle heartbeat events in heartbeat_mgr.

#### `disable_cluster_feature`

Default：true

IsMutable：true

The multi cluster feature will be deprecated in version 0.12 ，set this config to true will disable all operations related to cluster feature, include:

1. create/drop cluster
2. add free backend/add backend to cluster/decommission cluster balance
3. change the backends num of cluster
4. link/migration db

#### `enable_deploy_manager`

Default：disable

Set to true if you deploy Doris using thirdparty deploy manager

Valid options are:

- disable:    no deploy manager
-  k8s:        Kubernetes
- ambari:     Ambari
- local:      Local File (for test or Boxer2 BCC version)

#### `with_k8s_certs`

Default：false

If use k8s deploy manager locally, set this to true and prepare the certs files

#### `enable_fqdn_mode`

This configuration is mainly used in the k8s cluster environment. When enable_fqdn_mode is true, the name of the pod where the be is located will remain unchanged after reconstruction, while the ip can be changed.

Default: false

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: true

#### `enable_token_check`

Default：true

For forward compatibility, will be removed later. check token when download image file.

#### `enable_multi_tags`

Default: false

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: true

Whether to enable the multi-tags function of a single BE

### Service

#### `query_port`

Default：9030

FE MySQL server port

#### `arrow_flight_sql_port`

Default：-1

Arrow Flight SQL server port

#### `frontend_address`

Status: Deprecated, not recommended use. This parameter may be deleted later

Type: string

Description: Explicitly set the IP address of FE instead of using *InetAddress.getByName* to get the IP address. Usually in *InetAddress.getByName* When the expected results cannot be obtained. Only IP address is supported, not hostname.

Default value: 0.0.0.0

#### `priority_networks`

Default：none

Declare a selection strategy for those servers have many ips.  Note that there should at most one ip match this list.  this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24 ， If no ip match this rule, will choose one randomly.

#### `http_port`

Default：8030

HTTP bind port. All FE http ports must be same currently.

#### `https_port`

Default：8050

HTTPS bind port. All FE https ports must be same currently.

#### `enable_https`

Default：false

Https enable flag. If the value is false, http is supported. Otherwise, both http and https are supported, and http requests are automatically redirected to https.
If enable_https is true, you need to configure ssl certificate information in fe.conf.

#### `enable_ssl`

Default：true

If set to ture, doris will establish an encrypted channel based on the SSL protocol with mysql.

#### `qe_max_connection`

Default：1024

Maximal number of connections per FE.

#### `check_java_version`

Default：true

Doris will check whether the compiled and run Java versions are compatible, if not, it will throw a Java version mismatch exception message and terminate the startup

#### `rpc_port`

Default：9020

FE Thrift Server port

#### `thrift_server_type`

This configuration represents the service model used by The Thrift Service of FE, is of type String and is case-insensitive.

If this parameter is 'SIMPLE', then the 'TSimpleServer' model is used, which is generally not suitable for production and is limited to test use.

If the parameter is 'THREADED', then the 'TThreadedSelectorServer' model is used, which is a non-blocking I/O model, namely the master-slave Reactor model, which can timely respond to a large number of concurrent connection requests and performs well in most scenarios.

If this parameter is `THREAD_POOL`, then the `TThreadPoolServer` model is used, the model for blocking I/O model, use the thread pool to handle user connections, the number of simultaneous connections are limited by the number of thread pool, if we can estimate the number of concurrent requests in advance, and tolerant enough thread resources cost, this model will have a better performance, the service model is used by default

#### `thrift_server_max_worker_threads`

Default：4096

The thrift server max worker threads

#### `thrift_backlog_num`

Default：1024

The backlog_num for thrift server ， When you enlarge this backlog_num, you should ensure it's value larger than the linux /proc/sys/net/core/somaxconn config

#### `thrift_client_timeout_ms`

Default：0

The connection timeout and socket timeout config for thrift server.

The value for thrift_client_timeout_ms is set to be zero to prevent read timeout.

#### `use_compact_thrift_rpc`

Default: true

Whether to use compressed format to send query plan structure. After it is turned on, the size of the query plan structure can be reduced by about 50%, thereby avoiding some "send fragment timeout" errors.
However, in some high-concurrency small query scenarios, the concurrency may be reduced by about 10%.

#### `grpc_max_message_size_bytes`

Default：1G

Used to set the initial flow window size of the GRPC client channel, and also used to max message size.  When the result set is large, you may need to increase this value.

#### `max_mysql_service_task_threads_num`

Default：4096

The number of threads responsible for Task events.

#### `mysql_service_io_threads_num`

Default：4

When FE starts the MySQL server based on NIO model, the number of threads responsible for IO events.

#### `mysql_nio_backlog_num`

Default：1024

The backlog_num for mysql nio server, When you enlarge this backlog_num, you should enlarge the value in the linux /proc/sys/net/core/somaxconn file at the same time

#### `broker_timeout_ms`

Default：10000   （10s）

Default broker RPC timeout

#### `backend_rpc_timeout_ms`

Timeout millisecond for Fe sending rpc request to BE

Default: 60000

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: true

#### `drop_backend_after_decommission`

Default：false

IsMutable：true

MasterOnly：true

1. This configuration is used to control whether the system drops the BE after successfully decommissioning the BE. If true, the BE node will be deleted after the BE is successfully offline. If false, after the BE successfully goes offline, the BE will remain in the DECOMMISSION state, but will not be dropped.

   This configuration can play a role in certain scenarios. Assume that the initial state of a Doris cluster is one disk per BE node. After running for a period of time, the system has been vertically expanded, that is, each BE node adds 2 new disks. Because Doris currently does not support data balancing among the disks within the BE, the data volume of the initial disk may always be much higher than the data volume of the newly added disk. At this time, we can perform manual inter-disk balancing by the following operations:

   1. Set the configuration item to false.
   2. Perform a decommission operation on a certain BE node. This operation will migrate all data on the BE to other nodes.
   3. After the decommission operation is completed, the BE will not be dropped. At this time, cancel the decommission status of the BE. Then the data will start to balance from other BE nodes back to this node. At this time, the data will be evenly distributed to all disks of the BE.
   4. Perform steps 2 and 3 for all BE nodes in sequence, and finally achieve the purpose of disk balancing for all nodes

#### `max_backend_down_time_second`

Default：3600  （1 hour）

IsMutable：true

MasterOnly：true

If a backend is down for *max_backend_down_time_second*, a BACKEND_DOWN event will be triggered.

#### `disable_backend_black_list`

Used to disable the BE blacklist function. After this function is disabled, if the query request to the BE fails, the BE will not be added to the blacklist.
This parameter is suitable for regression testing environments to reduce occasional bugs that cause a large number of regression tests to fail.

Default: false

Is it possible to configure dynamically: true

Is it a configuration item unique to the Master FE node: false

#### `max_backend_heartbeat_failure_tolerance_count`

The maximum tolerable number of BE node heartbeat failures. If the number of consecutive heartbeat failures exceeds this value, the BE state will be set to dead.
This parameter is suitable for regression test environments to reduce occasional heartbeat failures that cause a large number of regression test failures.

Default: 1

Is it possible to configure dynamically: true

Whether it is a configuration item unique to the Master FE node: true

#### `enable_access_file_without_broker`

Default：false

IsMutable：true

MasterOnly：true

This config is used to try skip broker when access bos or other cloud storage via broker

#### `agent_task_resend_wait_time_ms`

Default：5000

IsMutable：true

MasterOnly：true

This configuration will decide whether to resend agent task when create_time for agent_task is set, only when current_time - create_time > agent_task_resend_wait_time_ms can ReportHandler do resend agent task.

This configuration is currently mainly used to solve the problem of repeated sending of `PUBLISH_VERSION` agent tasks. The current default value of this configuration is 5000, which is an experimental value.

Because there is a certain time delay between submitting agent tasks to AgentTaskQueue and submitting to be, Increasing the value of this configuration can effectively solve the problem of repeated sending of agent tasks,

But at the same time, it will cause the submission of failed or failed execution of the agent task to be executed again for an extended period of time

#### `max_agent_task_threads_num`

Default：4096

MasterOnly：true

max num of thread to handle agent task in agent task thread-pool.

#### `remote_fragment_exec_timeout_ms`

Default：30000  （ms）

IsMutable：true

The timeout of executing async remote fragment.  In normal case, the async remote fragment will be executed in a short time. If system are under high load condition，try to set this timeout longer.

#### `auth_token`

Default：empty

Cluster token used for internal authentication.

#### `enable_http_server_v2`

Default：The default is true after the official 0.14.0 version is released, and the default is false before

HTTP Server V2 is implemented by SpringBoot. It uses an architecture that separates the front and back ends. Only when httpv2 is enabled can users use the new front-end UI interface.

#### `http_api_extra_base_path`

In some deployment environments, user need to specify an additional base path as the unified prefix of the HTTP API. This parameter is used by the user to specify additional prefixes.
After setting, user can get the parameter value through the `GET /api/basepath` interface. And the new UI will also try to get this base path first to assemble the URL. Only valid when `enable_http_server_v2` is true.

The default is empty, that is, not set

#### `jetty_server_acceptors`

Default：2

#### `jetty_server_selectors`

Default：4

#### `jetty_server_workers`

Default：0

With the above three parameters, Jetty's thread architecture model is very simple, divided into acceptors, selectors and workers three thread pools. Acceptors are responsible for accepting new connections, and then hand them over to selectors to process the unpacking of the HTTP message protocol, and finally workers process the request. The first two thread pools adopt a non-blocking model, and one thread can handle the read and write of many sockets, so the number of thread pools is small.

For most projects, only 1-2 acceptors threads are required, and 2 to 4 selectors threads are sufficient. Workers are obstructive business logic, often have more database operations, and require a large number of threads. The specific number depends on the proportion of QPS and IO events of the application. The higher the QPS, the more threads are required, the higher the proportion of IO, the more threads waiting, and the more total threads required.

Worker thread pool is not set by default, set according to your needs

#### `jetty_server_max_http_post_size`

Default：`100 * 1024 * 1024`  （100MB）

This is the maximum number of bytes of the file uploaded by the put or post method, the default value: 100MB

#### `jetty_server_max_http_header_size`

Default：1048576  （1M）

http header size configuration parameter, the default value is 1M.

#### `enable_tracing`

Default：false

IsMutable：false

MasterOnly：false

Whether to enable tracking

If this configuration is enabled, you should also specify the trace_export_url.

#### `trace_exporter`

Default：zipkin

IsMutable：false

MasterOnly：false

Current support for exporting traces:
  zipkin: Export traces directly to zipkin, which is used to enable the tracing feature quickly.
  collector: The collector can be used to receive and process traces and support export to a variety of third-party systems.
If this configuration is enabled, you should also specify the enable_tracing=true and trace_export_url.

#### `trace_export_url`

Default：`http://127.0.0.1:9411/api/v2/spans`

IsMutable：false

MasterOnly：false

trace export to zipkin like: `http://127.0.0.1:9411/api/v2/spans`

trace export to collector like: `http://127.0.0.1:4318/v1/traces`

### Query Engine

#### `default_max_query_instances`

The default value when user property max_query_instances is equal or less than 0. This config is used to limit the max number of instances for a user. This parameter is less than or equal to 0 means unlimited.

The default value is -1

#### `max_query_retry_time`

Default：1

IsMutable：true

The number of query retries.  A query may retry if we encounter RPC exception and no result has been sent to user.  You may reduce this number to avoid Avalanche disaster

#### `max_dynamic_partition_num`

Default：500

IsMutable：true

MasterOnly：true

Used to limit the maximum number of partitions that can be created when creating a dynamic partition table,  to avoid creating too many partitions at one time. The number is determined by "start" and "end" in the dynamic partition parameters.

#### `dynamic_partition_enable`

Default：true

IsMutable：true

MasterOnly：true

Whether to enable dynamic partition scheduler, enabled by default

#### `dynamic_partition_check_interval_seconds`

Default：600 （s）

IsMutable：true

MasterOnly：true

Decide how often to check dynamic partition

<version since="1.2.0">

#### `max_multi_partition_num`

Default：4096

IsMutable：true

MasterOnly：true

Use this parameter to set the partition name prefix for multi partition,Only multi partition takes effect, not dynamic partitions. The default prefix is "p_".
</version>

#### `multi_partition_name_prefix`

Default：p_

IsMutable：true

MasterOnly：true

Use this parameter to set the partition name prefix for multi partition, Only multi partition takes effect, not dynamic partitions.The default prefix is "p_".

#### `partition_in_memory_update_interval_secs`

Default：300 (s)

IsMutable：true

MasterOnly：true

Time to update global partition information in memory

#### `enable_concurrent_update`

Default：false

IsMutable：false

MasterOnly：true

Whether to enable concurrent update

#### `lower_case_table_names`

Default：0

IsMutable：false

MasterOnly：true

This configuration can only be configured during cluster initialization and cannot be modified during cluster
restart and upgrade after initialization is complete.

0: table names are stored as specified and comparisons are case sensitive.
1: table names are stored in lowercase and comparisons are not case sensitive.
2: table names are stored as given but compared in lowercase.

#### `table_name_length_limit`

Default：64

IsMutable：true

MasterOnly：true

Used to control the maximum table name length

#### `cache_enable_sql_mode`

Default：true

IsMutable：true

MasterOnly：false

If this switch is turned on, the SQL query result set will be cached. If the interval between the last visit version time in all partitions of all tables in the query is greater than cache_last_version_interval_second, and the result set is less than cache_result_max_row_count, and the data size is less than cache_result_max_data_size, the result set will be cached, and the next same SQL will hit the cache

If set to true, fe will enable sql result caching. This option is suitable for offline data update scenarios

|                        | case1 | case2 | case3 | case4 |
| ---------------------- | ----- | ----- | ----- | ----- |
| enable_sql_cache       | false | true  | true  | false |
| enable_partition_cache | false | false | true  | true  |

#### `cache_enable_partition_mode`

Default：true

IsMutable：true

MasterOnly：false

If set to true, fe will get data from be cache, This option is suitable for real-time updating of partial partitions.

#### `cache_result_max_row_count`

Default：3000

IsMutable：true

MasterOnly：false

In order to avoid occupying too much memory, the maximum number of rows that can be cached is 3000 by default. If this threshold is exceeded, the cache cannot be set

#### `cache_result_max_data_size`

Default: 31457280

IsMutable: true

MasterOnly: false

In order to avoid occupying too much memory, the maximum data size of rows that can be cached is 10MB by default. If this threshold is exceeded, the cache cannot be set

#### `cache_last_version_interval_second`

Default：900

IsMutable：true

MasterOnly：false

The time interval of the latest partitioned version of the table refers to the time interval between the data update and the current version. It is generally set to 900 seconds, which distinguishes offline and real-time import

#### `enable_batch_delete_by_default`

Default：false

IsMutable：true

MasterOnly：true

Whether to add a delete sign column when create unique table

#### `max_allowed_in_element_num_of_delete`

Default：1024

IsMutable：true

MasterOnly：true

This configuration is used to limit element num of InPredicate in delete statement.

#### `max_running_rollup_job_num_per_table`

Default：1

IsMutable：true

MasterOnly：true

Control the concurrency limit of Rollup jobs

#### `max_distribution_pruner_recursion_depth`

Default：100

IsMutable：true

MasterOnly：false

This will limit the max recursion depth of hash distribution pruner.
eg: where a in (5 elements) and b in (4 elements) and c in (3 elements) and d in (2 elements).
a/b/c/d are distribution columns, so the recursion depth will be 5 * 4 * 3 * 2 = 120, larger than 100,
So that distribution pruner will no work and just return all buckets.
Increase the depth can support distribution pruning for more elements, but may cost more CPU.

#### `enable_local_replica_selection`

Default：false

IsMutable：true

If set to true, Planner will try to select replica of tablet on same host as this Frontend.
This may reduce network transmission in following case:

-  N hosts with N Backends and N Frontends deployed.

-  The data has N replicas.

-  High concurrency queries are syyuyuient to all Frontends evenly

In this case, all Frontends can only use local replicas to do the query. If you want to allow fallback to nonlocal replicas when no local replicas available, set enable_local_replica_selection_fallback to true.

#### `enable_local_replica_selection_fallback`

Default：false

IsMutable：true

Used with enable_local_replica_selection. If the local replicas is not available, fallback to the nonlocal replicas.

#### `expr_depth_limit`

Default：3000

IsMutable：true

Limit on the depth of an expr tree.  Exceed this limit may cause long analysis time while holding db read lock.  Do not set this if you know what you are doing

#### `expr_children_limit`

Default：10000

IsMutable：true

Limit on the number of expr children of an expr tree.  Exceed this limit may cause long analysis time while holding database read lock.

#### `be_exec_version`

Used to define the serialization format for passing blocks between fragments.

Sometimes some of our code changes will change the data format of the block. In order to make the BE compatible with each other during the rolling upgrade process, we need to issue a data version from the FE to decide what format to send the data in.

Specifically, for example, there are 2 BEs in the cluster, one of which can support the latest $v_1$ after being upgraded, while the other only supports $v_0$. At this time, since the FE has not been upgraded yet, $v_0 is issued uniformly. $, BE interact in the old data format. After all BEs are upgraded, we will upgrade FE. At this time, the new FE will issue $v_1$, and the cluster will be uniformly switched to the new data format.


The default value is `max_be_exec_version`. If there are special needs, we can manually set the format version to lower, but it should not be lower than `min_be_exec_version`.

Note that we should always keep the value of this variable between `BeExecVersionManager::min_be_exec_version` and `BeExecVersionManager::max_be_exec_version` for all BEs. (That is to say, if a cluster that has completed the update needs to be downgraded, it should ensure the order of downgrading FE and then downgrading BE, or manually lower the variable in the settings and downgrade BE)

#### `max_be_exec_version`

The latest data version currently supported, cannot be modified, and should be consistent with the `BeExecVersionManager::max_be_exec_version` in the BE of the matching version.

#### `min_be_exec_version`

The oldest data version currently supported, which cannot be modified, should be consistent with the `BeExecVersionManager::min_be_exec_version` in the BE of the matching version.

#### `max_query_profile_num`

The max number of query profile.

Default: 100

Is it possible to dynamically configure: true

Is it a configuration item unique to the Master FE node: false

#### `publish_version_interval_ms`

Default：10 （ms）

minimal intervals between two publish version action

#### `publish_version_timeout_second`

Default：30 （s）

IsMutable：true

MasterOnly：true

Maximal waiting time for all publish version tasks of one transaction to be finished

#### `query_colocate_join_memory_limit_penalty_factor`

Default：1

IsMutable：true

colocote join PlanFragment instance的memory_limit = exec_mem_limit / min (query_colocate_join_memory_limit_penalty_factor, instance_num)

#### `rewrite_count_distinct_to_bitmap_hll`

Default: true

This variable is a session variable, and the session level takes effect.

- Type: boolean
- Description: **Only for the table of the AGG model**, when the variable is true, when the user query contains aggregate functions such as count(distinct c1), if the type of the c1 column itself is bitmap, count distnct will be rewritten It is bitmap_union_count(c1). When the type of the c1 column itself is hll, count distinct will be rewritten as hll_union_agg(c1) If the variable is false, no overwriting occurs..

### Load And Export

#### `enable_vectorized_load`

Default: true

Whether to enable vectorized load

#### `enable_new_load_scan_node`

Default: true

Whether to enable file scan node

#### `default_max_filter_ratio`

Default：0

IsMutable：true

MasterOnly：true

Maximum percentage of data that can be filtered (due to reasons such as data is irregularly) , The default value is 0.

#### `max_running_txn_num_per_db`

Default：1000

IsMutable：true

MasterOnly：true

This configuration is mainly used to control the number of concurrent load jobs of the same database.

When there are too many load jobs running in the cluster, the newly submitted load jobs may report errors:

```text
current running txns on db xxx is xx, larger than limit xx
```

When this error is encountered, it means that the load jobs currently running in the cluster exceeds the configuration value. At this time, it is recommended to wait on the business side and retry the load jobs.

If you use the Connector, the value of this parameter can be adjusted appropriately, and there is no problem with thousands

#### `using_old_load_usage_pattern`

Default：false

IsMutable：true

MasterOnly：true

If set to true, the insert stmt with processing error will still return a label to user.  And user can use this label to check the load job's status. The default value is false, which means if insert operation encounter errors,  exception will be thrown to user client directly without load label.

#### `disable_load_job`

Default：false

IsMutable：true

MasterOnly：true

if this is set to true

- all pending load job will failed when call begin txn api
- all prepare load job will failed when call commit txn api
- all committed load job will waiting to be published

#### `commit_timeout_second`

Default：30

IsMutable：true

MasterOnly：true

Maximal waiting time for all data inserted before one transaction to be committed
This is the timeout second for the command "commit"

#### `max_unfinished_load_job`

Default：1000

IsMutable：true

MasterOnly：true

Max number of load jobs, include PENDING、ETL、LOADING、QUORUM_FINISHED. If exceed this number, load job is not allowed to be submitted

#### `db_used_data_quota_update_interval_secs`

Default：300 (s)

IsMutable：true

MasterOnly：true

One master daemon thread will update database used data quota for db txn manager every `db_used_data_quota_update_interval_secs`

For better data load performance, in the check of whether the amount of data used by the database before data load exceeds the quota, we do not calculate the amount of data already used by the database in real time, but obtain the periodically updated value of the daemon thread.

This configuration is used to set the time interval for updating the value of the amount of data used by the database

#### `disable_show_stream_load`

Default：false

IsMutable：true

MasterOnly：true

Whether to disable show stream load and clear stream load records in memory.

#### `max_stream_load_record_size`

Default：5000

IsMutable：true

MasterOnly：true

Default max number of recent stream load record that can be stored in memory.

#### `fetch_stream_load_record_interval_second`

Default：120

IsMutable：true

MasterOnly：true

fetch stream load record interval.

#### `max_bytes_per_broker_scanner`

Default：`500 * 1024 * 1024 * 1024L`  （500G）

IsMutable：true

MasterOnly：true

Max bytes a broker scanner can process in one broker load job. Commonly, each Backends has one broker scanner.

#### `default_load_parallelism`

Default: 1

IsMutable：true

MasterOnly：true

Default parallelism of the broker load execution plan on a single node.
If the user to set the parallelism when the broker load is submitted, this parameter will be ignored.
This parameter will determine the concurrency of import tasks together with multiple configurations such as `max broker concurrency`, `min bytes per broker scanner`.

#### `max_broker_concurrency`

Default：10

IsMutable：true

MasterOnly：true

Maximal concurrency of broker scanners.

#### `min_bytes_per_broker_scanner`

Default：67108864L (64M)

IsMutable：true

MasterOnly：true

Minimum bytes that a single broker scanner will read.

#### `period_of_auto_resume_min`

Default：5 （s）

IsMutable：true

MasterOnly：true

Automatically restore the cycle of Routine load

#### `max_tolerable_backend_down_num`

Default：0

IsMutable：true

MasterOnly：true

As long as one BE is down, Routine Load cannot be automatically restored

#### `max_routine_load_task_num_per_be`

Default：5

IsMutable：true

MasterOnly：true

the max concurrent routine load task num per BE.  This is to limit the num of routine load tasks sending to a BE, and it should also less than BE config 'routine_load_thread_pool_size'(default 10), which is the routine load task thread pool size on BE.

#### `max_routine_load_task_concurrent_num`

Default：5

IsMutable：true

MasterOnly：true

the max concurrent routine load task num of a single routine load job

#### `max_routine_load_job_num`

Default：100

the max routine load job num, including NEED_SCHEDULED, RUNNING, PAUSE

#### `desired_max_waiting_jobs`

Default：100

IsMutable：true

MasterOnly：true

Default number of waiting jobs for routine load and version 2 of load ， This is a desired number.  In some situation, such as switch the master, the current number is maybe more than desired_max_waiting_jobs.

#### `disable_hadoop_load`

Default：false

IsMutable：true

MasterOnly：true

Load using hadoop cluster will be deprecated in future. Set to true to disable this kind of load.

#### `enable_spark_load`

Default：false

IsMutable：true

MasterOnly：true

Whether to enable spark load temporarily, it is not enabled by default

**Note:** This parameter has been deleted in version 1.2, spark_load is enabled by default

#### `spark_load_checker_interval_second`

Default：60

Spark load scheduler run interval, default 60 seconds

#### `async_loading_load_task_pool_size`

Default：10

IsMutable：false

MasterOnly：true

The loading_load task executor pool size. This pool size limits the max running loading_load tasks.

Currently, it only limits the loading_load task of broker load

#### `async_pending_load_task_pool_size`

Default：10

IsMutable：false

MasterOnly：true

The pending_load task executor pool size. This pool size limits the max running pending_load tasks.

Currently, it only limits the pending_load task of broker load and spark load.

It should be less than 'max_running_txn_num_per_db'

#### `async_load_task_pool_size`

Default：10

IsMutable：false

MasterOnly：true

This configuration is just for compatible with old version, this config has been replaced by async_loading_load_task_pool_size, it will be removed in the future.

#### `enable_single_replica_load`

Default：false

IsMutable：true

MasterOnly：true

Whether to enable to write single replica for stream load and broker load.

#### `min_load_timeout_second`

Default：1 （1s）

IsMutable：true

MasterOnly：true

Min stream load timeout applicable to all type of load

#### `max_stream_load_timeout_second`

Default：259200 （3 day）

IsMutable：true

MasterOnly：true

This configuration is specifically used to limit timeout setting for stream load. It is to prevent that failed stream load transactions cannot be canceled within a short time because of the user's large timeout setting

#### `max_load_timeout_second`

Default：259200 （3 day）

IsMutable：true

MasterOnly：true

Max load timeout applicable to all type of load except for stream load

#### `stream_load_default_timeout_second`

Default：86400 * 3 （3 day）

IsMutable：true

MasterOnly：true

Default stream load and streaming mini load timeout

#### `stream_load_default_precommit_timeout_second`

Default：3600（s）

IsMutable：true

MasterOnly：true

Default stream load pre-submission timeout

#### `insert_load_default_timeout_second`

Default：3600（1 hour）

IsMutable：true

MasterOnly：true

Default insert load timeout

#### `mini_load_default_timeout_second`

Default：3600（1 hour）

IsMutable：true

MasterOnly：true

Default non-streaming mini load timeout

#### `broker_load_default_timeout_second`

Default：14400（4 hour）

IsMutable：true

MasterOnly：true

Default broker load timeout

#### `spark_load_default_timeout_second`

Default：86400  (1 day)

IsMutable：true

MasterOnly：true

Default spark load timeout

#### `hadoop_load_default_timeout_second`

Default：86400 * 3   (3 day)

IsMutable：true

MasterOnly：true

Default hadoop load timeout

#### `load_running_job_num_limit`

Default：0

IsMutable：true

MasterOnly：true

The number of loading tasks is limited, the default is 0, no limit

#### `load_input_size_limit_gb`

Default：0

IsMutable：true

MasterOnly：true

The size of the data entered by the Load job, the default is 0, unlimited

#### `load_etl_thread_num_normal_priority`

Default：10

Concurrency of NORMAL priority etl load jobs. Do not change this if you know what you are doing.

#### `load_etl_thread_num_high_priority`

Default：3

Concurrency of HIGH priority etl load jobs. Do not change this if you know what you are doing

#### `load_pending_thread_num_normal_priority`

Default：10

Concurrency of NORMAL priority pending load jobs.  Do not change this if you know what you are doing.

#### `load_pending_thread_num_high_priority`

Default：3

Concurrency of HIGH priority pending load jobs. Load job priority is defined as HIGH or NORMAL.  All mini batch load jobs are HIGH priority, other types of load jobs are NORMAL priority.  Priority is set to avoid that a slow load job occupies a thread for a long time.  This is just a internal optimized scheduling policy.  Currently, you can not specified the job priority manually, and do not change this if you know what you are doing.

#### `load_checker_interval_second`

Default：5 （s）

The load scheduler running interval. A load job will transfer its state from PENDING to LOADING to FINISHED.  The load scheduler will transfer load job from PENDING to LOADING while the txn callback will transfer load job from LOADING to FINISHED.  So a load job will cost at most one interval to finish when the concurrency has not reached the upper limit.

#### `load_straggler_wait_second`

Default：300

IsMutable：true

MasterOnly：true

Maximal wait seconds for straggler node in load
   eg.
      there are 3 replicas A, B, C
      load is already quorum finished(A,B) at t1 and C is not finished
      if (current_time - t1) > 300s, then palo will treat C as a failure node
      will call transaction manager to commit the transaction and tell transaction manager that C is failed

This is also used when waiting for publish tasks

**Note:** this parameter is the default value for all job and the DBA could specify it for separate job

#### `label_keep_max_second`

Default：`3 * 24 * 3600`  (3 day)

IsMutable：true

MasterOnly：true

labels of finished or cancelled load jobs will be removed after `label_keep_max_second` ，

1. The removed labels can be reused.
2. Set a short time will lower the FE memory usage.  (Because all load jobs' info is kept in memory before being removed)

In the case of high concurrent writes, if there is a large backlog of jobs and call frontend service failed, check the log. If the metadata write takes too long to lock, you can adjust this value to 12 hours, or 6 hours less

#### `streaming_label_keep_max_second`

Default：43200 （12 hour）

IsMutable：true

MasterOnly：true

For some high-frequency load work, such as: INSERT, STREAMING LOAD, ROUTINE_LOAD_TASK. If it expires, delete the completed job or task.

#### `label_clean_interval_second`

Default：1 * 3600  （1 hour）

Load label cleaner will run every *label_clean_interval_second* to clean the outdated jobs.

#### `transaction_clean_interval_second`

Default：30

the transaction will be cleaned after transaction_clean_interval_second seconds if the transaction is visible or aborted  we should make this interval as short as possible and each clean cycle as soon as possible

#### `sync_commit_interval_second`

The maximum time interval for committing transactions. If there is still data in the channel that has not been submitted after this time, the consumer will notify the channel to submit the transaction.

Default: 10 (seconds)

Is it possible to configure dynamically: true

Whether it is a configuration item unique to the Master FE node: true

#### `sync_checker_interval_second`

Data synchronization job running status check.

Default: 10（s）

#### `max_sync_task_threads_num`

The maximum number of threads in the data synchronization job thread pool.

默认值：10

#### `min_sync_commit_size`

The minimum number of events that must be satisfied to commit a transaction. If the number of events received by Fe is less than it, it will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 10000 events. If you want to modify this configuration, please make sure that this value is smaller than the `canal.instance.memory.buffer.size` configuration on the canal side (default 16384), otherwise Fe will try to get the queue length longer than the store before ack More events cause the store queue to block until it times out.

Default: 10000

Is it possible to configure dynamically: true

Whether it is a configuration item unique to the Master FE node: true

#### `min_bytes_sync_commit`

The minimum data size required to commit a transaction. If the data size received by Fe is smaller than it, it will continue to wait for the next batch of data until the time exceeds `sync_commit_interval_second`. The default value is 15MB, if you want to modify this configuration, please make sure this value is less than the product of `canal.instance.memory.buffer.size` and `canal.instance.memory.buffer.memunit` on the canal side (default 16MB), otherwise Before the ack, Fe will try to obtain data that is larger than the store space, causing the store queue to block until it times out.

Default: `15*1024*1024` (15M)

Is it possible to configure dynamically: true

Whether it is a configuration item unique to the Master FE node: true

#### `max_bytes_sync_commit`

The maximum number of threads in the data synchronization job thread pool. There is only one thread pool in the entire FE, which is used to process all data synchronization tasks in the FE that send data to the BE. The implementation of the thread pool is in the `SyncTaskPool` class.

Default: 10

Is it possible to dynamically configure: false

Is it a configuration item unique to the Master FE node: false

#### `enable_outfile_to_local`

Default：false

Whether to allow the outfile function to export the results to the local disk.

#### `export_tablet_num_per_task`

Default：5

IsMutable：true

MasterOnly：true

Number of tablets per export query plan

#### `export_task_default_timeout_second`

Default：2 * 3600   （2 hour）

IsMutable：true

MasterOnly：true

Default timeout of export jobs.

#### `export_running_job_num_limit`

Default：5

IsMutable：true

MasterOnly：true

Limitation of the concurrency of running export jobs.  Default is 5.  0 is unlimited

#### `export_checker_interval_second`

Default：5

Export checker's running interval.

### Log

#### `log_roll_size_mb`

Default：1024  （1G）

The max size of one sys log and audit log

#### `sys_log_dir`

Default：DorisFE.DORIS_HOME_DIR + "/log"

sys_log_dir:

This specifies FE log dir. FE will produces 2 log files:

fe.log:      all logs of FE process.
fe.warn.log  all WARNING and ERROR log of FE process.

#### `sys_log_level`

Default：INFO

log level：INFO, WARNING, ERROR, FATAL

#### `sys_log_roll_num`

Default：10

Maximal FE log files to be kept within an sys_log_roll_interval. default is 10, which means there will be at most 10 log files in a day

#### `sys_log_verbose_modules`

Default：{}

Verbose modules. VERBOSE level is implemented by log4j DEBUG level.

eg：
   sys_log_verbose_modules = org.apache.doris.catalog
   This will only print debug log of files in package org.apache.doris.catalog and all its sub packages.

#### `sys_log_roll_interval`

Default：DAY

sys_log_roll_interval:

- DAY:  log suffix is  yyyyMMdd
- HOUR: log suffix is  yyyyMMddHH

#### `sys_log_delete_age`

Default：7d

default is 7 days, if log's last modify time is 7 days ago, it will be deleted.

support format:

- 7d      7 day
- 10h     10 hours
- 60m     60 min
- 120s    120 seconds

#### `sys_log_roll_mode`

Default：SIZE-MB-1024

The size of the log split, split a log file every 1 G

#### `sys_log_enable_compress`

Default: false

If true, will compress fe.log & fe.warn.log by gzip

#### `audit_log_dir`

Default：DORIS_HOME_DIR + "/log"

audit_log_dir：
This specifies FE audit log dir..
Audit log fe.audit.log contains all requests with related infos such as user, host, cost, status, etc

#### `audit_log_roll_num`

Default：90

Maximal FE audit log files to be kept within an audit_log_roll_interval.

#### `audit_log_modules`

Default：{"slow_query", "query", "load", "stream_load"}

Slow query contains all queries which cost exceed *qe_slow_log_ms*

#### `qe_slow_log_ms`

Default：5000 （5 seconds）

If the response time of a query exceed this threshold, it will be recorded in audit log as slow_query.

#### `audit_log_roll_interval`

Default：DAY

DAY:  logsuffix is ：yyyyMMdd
HOUR: logsuffix is ：yyyyMMddHH

#### `audit_log_delete_age`

Default：30d

default is 30 days, if log's last modify time is 30 days ago, it will be deleted.

support format:
- 7d      7 day
- 10h     10 hours
- 60m     60 min
- 120s    120 seconds

#### `audit_log_enable_compress`

Default: false

If true, will compress fe.audit.log by gzip

### Storage

#### `min_replication_num_per_tablet`

Default: 1

Used to set minimal number of replication per tablet.

#### `max_replication_num_per_tablet`

Default: 32767

Used to set maximal number of replication per tablet.

#### `default_db_data_quota_bytes`

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

#### `default_db_replica_quota_size`

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

#### `recover_with_empty_tablet`

Default：false

IsMutable：true

MasterOnly：true

In some very special circumstances, such as code bugs, or human misoperation, etc., all replicas of some tablets may be lost. In this case, the data has been substantially lost. However, in some scenarios, the business still hopes to ensure that the query will not report errors even if there is data loss, and reduce the perception of the user layer. At this point, we can use the blank Tablet to fill the missing replica to ensure that the query can be executed normally.

Set to true so that Doris will automatically use blank replicas to fill tablets which all replicas have been damaged or missing

#### `min_clone_task_timeout_sec` `And max_clone_task_timeout_sec`

Default：Minimum 3 minutes, maximum two hours

IsMutable：true

MasterOnly：true

Can cooperate with `mix_clone_task_timeout_sec` to control the maximum and minimum timeout of a clone task. Under normal circumstances, the timeout of a clone task is estimated by the amount of data and the minimum transfer rate (5MB/s). In some special cases, these two configurations can be used to set the upper and lower bounds of the clone task timeout to ensure that the clone task can be completed successfully.

#### `disable_storage_medium_check`

Default：false

IsMutable：true

MasterOnly：true

If disable_storage_medium_check is true, ReportHandler would not check tablet's storage medium and disable storage cool down function, the default value is false. You can set the value true when you don't care what the storage medium of the tablet is.

#### `decommission_tablet_check_threshold`

Default：5000

IsMutable：true

MasterOnly：true

This configuration is used to control whether the Master FE need to check the status of tablets on decommissioned BE. If the size of tablets on decommissioned BE is lower than this threshold, FE will start a periodic check, if all tablets on decommissioned BE have been recycled, FE will drop this BE immediately.

For performance consideration, please don't set a very high value for this configuration.

#### `partition_rebalance_max_moves_num_per_selection`

Default：10

IsMutable：true

MasterOnly：true

Valid only if use PartitionRebalancer，

#### `partition_rebalance_move_expire_after_access`

Default：600   (s)

IsMutable：true

MasterOnly：true

Valid only if use PartitionRebalancer. If this changed, cached moves will be cleared

#### `tablet_rebalancer_type`

Default：BeLoad

MasterOnly：true

Rebalancer type(ignore case): BeLoad, Partition. If type parse failed, use BeLoad as default

#### `max_balancing_tablets`

Default：100

IsMutable：true

MasterOnly：true

if the number of balancing tablets in TabletScheduler exceed max_balancing_tablets, no more balance check

#### `max_scheduling_tablets`

Default：2000

IsMutable：true

MasterOnly：true

if the number of scheduled tablets in TabletScheduler exceed max_scheduling_tablets skip checking.

#### `disable_balance`

Default：false

IsMutable：true

MasterOnly：true

if set to true, TabletScheduler will not do balance.

#### `disable_disk_balance`

Default：true

IsMutable：true

MasterOnly：true

if set to true, TabletScheduler will not do disk balance.

#### `balance_load_score_threshold`

Default：0.1 (10%)

IsMutable：true

MasterOnly：true

the threshold of cluster balance score, if a backend's load score is 10% lower than average score,  this backend will be marked as LOW load, if load score is 10% higher than average score, HIGH load  will be marked

#### `capacity_used_percent_high_water`

Default：0.75  （75%）

IsMutable：true

MasterOnly：true

The high water of disk capacity used percent. This is used for calculating load score of a backend

#### `clone_distribution_balance_threshold`

Default：0.2

IsMutable：true

MasterOnly：true

Balance threshold of num of replicas in Backends.

#### `clone_capacity_balance_threshold`

Default：0.2

IsMutable：true

MasterOnly：true

* Balance threshold of data size in BE.

   The balance algorithm is:

     1. Calculate the average used capacity(AUC) of the entire cluster. (total data size / total backends num)

     2. The high water level is (AUC * (1 + clone_capacity_balance_threshold))

     3. The low water level is (AUC * (1 - clone_capacity_balance_threshold))

     4. The Clone checker will try to move replica from high water level BE to low water level BE.

#### `disable_colocate_balance`

Default：false

IsMutable：true

MasterOnly：true

This configs can set to true to disable the automatic colocate tables's relocate and balance.  If 'disable_colocate_balance' is set to true,   ColocateTableBalancer will not relocate and balance colocate tables.

**Attention**:

1. Under normal circumstances, there is no need to turn off balance at all.
2. Because once the balance is turned off, the unstable colocate table may not be restored
3. Eventually the colocate plan cannot be used when querying.

#### `balance_slot_num_per_path`

Default: 1

IsMutable：true

MasterOnly：true

Default number of slots per path during balance.

#### `disable_tablet_scheduler`

Default:false

IsMutable：true

MasterOnly：true

If set to true, the tablet scheduler will not work, so that all tablet repair/balance task will not work.

#### `enable_force_drop_redundant_replica`

Default: false

Dynamically configured: true

Only for Master FE: true

If set to true, the system will immediately drop redundant replicas in the tablet scheduling logic. This may cause some load jobs that are writing to the corresponding replica to fail, but it will speed up the balance and repair speed of the tablet.
When there are a large number of replicas waiting to be balanced or repaired in the cluster, you can try to set this config to speed up the balance and repair of replicas at the expense of partial load success rate.

#### `colocate_group_relocate_delay_second`

Default: 1800

Dynamically configured: true

Only for Master FE: true

The relocation of a colocation group may involve a large number of tablets moving within the cluster. Therefore, we should use a more conservative strategy to avoid relocation of colocation groups as much as possible.
Reloaction usually occurs after a BE node goes offline or goes down. This parameter is used to delay the determination of BE node unavailability. The default is 30 minutes, i.e., if a BE node recovers within 30 minutes, relocation of the colocation group will not be triggered.

####` allow_replica_on_same_host`

Default: false

Dynamically configured: false

Only for Master FE: false

Whether to allow multiple replicas of the same tablet to be distributed on the same host. This parameter is mainly used for local testing, to facilitate building multiple BEs to test certain multi-replica situations. Do not use it for non-test environments.

#### `repair_slow_replica`

Default: false

IsMutable：true

MasterOnly: true

If set to true, the replica with slower compaction will be automatically detected and migrated to other machines. The detection condition is that the version count of the fastest replica exceeds the value of `min_version_count_indicate_replica_compaction_too_slow`, and the ratio of the version count difference from the fastest replica exceeds the value of `valid_version_count_delta_ratio_between_replicas`

#### `min_version_count_indicate_replica_compaction_too_slow`

Default: 200

Dynamically configured: true

Only for Master FE: false

The version count threshold used to judge whether replica compaction is too slow

#### `skip_compaction_slower_replica`

Default: true

Dynamically configured: true

Only for Master FE: false

If set to true, the compaction slower replica will be skipped when select get queryable replicas

#### `valid_version_count_delta_ratio_between_replicas`

Default: 0.5

Dynamically configured: true

Only for Master FE: true

The valid ratio threshold of the difference between the version count of the slowest replica and the fastest replica. If `repair_slow_replica` is set to true, it is used to determine whether to repair the slowest replica

#### `min_bytes_indicate_replica_too_large`

Default: `2 * 1024 * 1024 * 1024` (2G)

Dynamically configured: true

Only for Master FE: true

The data size threshold used to judge whether replica is too large

#### `schedule_slot_num_per_path`

Default：2

the default slot number per path in tablet scheduler , remove this config and dynamically adjust it by clone task statistic

#### `tablet_repair_delay_factor_second`

Default：60 （s）

IsMutable：true

MasterOnly：true

the factor of delay time before deciding to repair tablet.

-  if priority is VERY_HIGH, repair it immediately.
-  HIGH, delay tablet_repair_delay_factor_second * 1;
-  NORMAL: delay tablet_repair_delay_factor_second * 2;
-  LOW: delay tablet_repair_delay_factor_second * 3;

#### `tablet_stat_update_interval_second`

Default：300（5min）

update interval of tablet stat,
All frontends will get tablet stat from all backends at each interval

#### `storage_flood_stage_usage_percent`

Default：95 （95%）

IsMutable：true

MasterOnly：true

##### `storage_flood_stage_left_capacity_bytes`

Default：1 * 1024 * 1024 * 1024 (1GB)

IsMutable：true

MasterOnly：true

If capacity of disk reach the 'storage_flood_stage_usage_percent' and  'storage_flood_stage_left_capacity_bytes', the following operation will be rejected:

1. load job
2. restore job

#### `storage_high_watermark_usage_percent`

Default：85  (85%)

IsMutable：true

MasterOnly：true

#### `storage_min_left_capacity_bytes`

Default： `2 * 1024 * 1024 * 1024`  (2GB)

IsMutable：true

MasterOnly：true

'storage_high_watermark_usage_percent' limit the max capacity usage percent of a Backend storage path.  'storage_min_left_capacity_bytes' limit the minimum left capacity of a Backend storage path.  If both limitations are reached, this storage path can not be chose as tablet balance destination. But for tablet recovery, we may exceed these limit for keeping data integrity as much as possible.

#### `catalog_trash_expire_second`

Default：86400L (1 day)

IsMutable：true

MasterOnly：true

After dropping database(table/partition), you can recover it by using RECOVER stmt. And this specifies the maximal data retention time. After time, the data will be deleted permanently.

#### `storage_cooldown_second`

<version deprecated="2.0"></version>

Default：`30 * 24 * 3600L`  （30 day）

When create a table(or partition), you can specify its storage medium(HDD or SSD). If set to SSD, this specifies the default duration that tablets will stay on SSD.  After that, tablets will be moved to HDD automatically.  You can set storage cooldown time in CREATE TABLE stmt.

#### `default_storage_medium`

Default：HDD

When create a table(or partition), you can specify its storage medium(HDD or SSD). If not set, this specifies the default medium when creat.

#### `enable_storage_policy`

- Whether to enable the Storage Policy feature. This config allows users to separate hot and cold data.
Default: false

Is it possible to dynamically configure: true

Is it a configuration item unique to the Master FE node: true

#### `check_consistency_default_timeout_second`

Default：600 （10 minutes）

IsMutable：true

MasterOnly：true

Default timeout of a single consistency check task. Set long enough to fit your tablet size

#### `consistency_check_start_time`

Default：23

IsMutable：true

MasterOnly：true

Consistency check start time

Consistency checker will run from *consistency_check_start_time* to *consistency_check_end_time*.

If the two times are the same, no consistency check will be triggered.

#### `consistency_check_end_time`

Default：23

IsMutable：true

MasterOnly：true

Consistency check end time

Consistency checker will run from *consistency_check_start_time* to *consistency_check_end_time*.

If the two times are the same, no consistency check will be triggered.

#### `replica_delay_recovery_second`

Default：0

IsMutable：true

MasterOnly：true

the minimal delay seconds between a replica is failed and fe try to recovery it using clone.

#### `tablet_create_timeout_second`

Default：1（s）

IsMutable：true

MasterOnly：true

Maximal waiting time for creating a single replica.

eg.
   if you create a table with #m tablets and #n replicas for each tablet,
   the create table request will run at most (m * n * tablet_create_timeout_second) before timeout.

#### `tablet_delete_timeout_second`

Default：2

IsMutable：true

MasterOnly：true

Same meaning as *tablet_create_timeout_second*, but used when delete a tablet.

#### `alter_table_timeout_second`

Default：86400 * 30（1 month）

IsMutable：true

MasterOnly：true

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


