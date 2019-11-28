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

# Variable

This document focuses on currently supported variables.

Variables in Doris refer to variable settings in MySQL. However, some of the variables are only used to be compatible with some MySQL client protocols, and do not produce their actual meaning in the MySQL database.

## Variable setting and viewing

### View

All or specified variables can be viewed via `SHOW VARIABLES [LIKE 'xxx'];`. Such as:

```
SHOW VARIABLES;
SHOW VARIABLES LIKE '%time_zone%';
```

### Settings

Some variables can be set at global-level or session-only. For global-level, the set value will be used in subsequent new session connections. For session-only, the variable only works for the current session.

For session-only, set by the `SET var_name=xxx;` statement. Such as:

```
SET exec_mem_limit = 137438953472;
SET forward_to_master = true;
SET time_zone = "Asia/Shanghai";
```

For global-level, set by `SET GLOBALE var_name=xxx;`. Such as:

```
SET GLOBAL exec_mem_limit = 137438953472
```

> Note 1: Only ADMIN users can set variable at global-level.
> Note 2: Global-level variables do not affect variable values in the current session, only variables in new sessions.

Variables that support global-level setting include:

* `time_zone`
* `wait_timeout`
* `sql_mode`
* `is_report_success`
* `query_timeout`
* `exec_mem_limit`
* `batch_size`
* `parallel_fragment_exec_instance_num`
* `parallel_exchange_instance_num`

At the same time, variable settings also support constant expressions. Such as:

```
SET exec_mem_limit = 10 * 1024 * 1024 * 1024;
SET forward_to_master = concat('tr', 'u', 'e');
```

## Supported variables

* `SQL_AUTO_IS_NULL`

    Used for compatible JDBC connection pool C3P0. No practical effect.
    
* `auto_increment_increment`

    Used for compatibility with MySQL clients. No practical effect.
    
* `autocommit`

    Used for compatibility with MySQL clients. No practical effect.
    
* `batch_size`

    Used to specify the number of rows of a single packet transmitted by each node during query execution. By default, the number of rows of a packet is 1024 rows. That is, after the source node generates 1024 rows of data, it is packaged and sent to the destination node.
    
    A larger number of rows will increase the throughput of the query in the case of scanning large data volumes, but may increase the query delay in small query scenario. At the same time, it also increases the memory overhead of the query. The recommended setting range is 1024 to 4096.
    
* `character_set_client`

     Used for compatibility with MySQL clients. No practical effect.

* `character_set_connection`

    Used for compatibility with MySQL clients. No practical effect.

* `character_set_results`

    Used for compatibility with MySQL clients. No practical effect.

* `character_set_server`

    Used for compatibility with MySQL clients. No practical effect.
    
* `codegen_level`

    Used to set the level of LLVM codegen. (Not currently in effect).
    
* `collation_connection`

    Used for compatibility with MySQL clients. No practical effect.

* `collation_database`

    Used for compatibility with MySQL clients. No practical effect.

* `collation_server`

    Used for compatibility with MySQL clients. No practical effect.

* `disable_colocate_join`

    Controls whether the [Colocation Join] (./colocation-join.md) function is enabled. The default is false, which means that the feature is enabled. True means that the feature is disabled. When this feature is disabled, the query plan will not attempt to perform a Colocation Join.
    
* `disable_streaming_preaggregations`

    Controls whether streaming pre-aggregation is turned on. The default is false, which is enabled. Currently not configurable and enabled by default.
    
* `enable_insert_strict`

    Used to set the `strict` mode when loadingdata via INSERT statement. The default is false, which means that the `strict` mode is not turned on. For an introduction to this mode, see [here] (./load-data/insert-into-manual.md).

* `enable_spilling`

    Used to set whether to enable external sorting. The default is false, which turns off the feature. This feature is enabled when the user does not specify a LIMIT condition for the ORDER BY clause and also sets `enable_spilling` to true. When this feature is enabled, the temporary data is stored in the `doris-scratch/` directory of the BE data directory and the temporary data is cleared after the query is completed.
    
    This feature is mainly used for sorting operations with large amounts of data using limited memory.
    
    Note that this feature is experimental and does not guarantee stability. Please turn it on carefully.
    
* `exec_mem_limit`

    Used to set the memory limit for a single query. The default is 2GB, in bytes.
    
    This parameter is used to limit the memory that can be used by an instance of a single query fragment in a query plan. A query plan may have multiple instances, and a BE node may execute one or more instances. Therefore, this parameter does not accurately limit the memory usage of a query across the cluster, nor does it accurately limit the memory usage of a query on a single BE node. The specific needs need to be judged according to the generated query plan.
    
    Usually, only some blocking nodes (such as sorting node, aggregation node, and join node) consume more memory, while in other nodes (such as scan node), data is streamed and does not occupy much memory.
    
    When a `Memory Exceed Limit` error occurs, you can try to increase the parameter exponentially, such as 4G, 8G, 16G, and so on.
    
* `forward_to_master`

    The user sets whether to forward some commands to the Master FE node for execution. The default is false, which means no forwarding. There are multiple FE nodes in Doris, one of which is the Master node. Usually users can connect to any FE node for full-featured operation. However, some of detail informationcan only be obtained from the Master FE node.
    
    For example, the `SHOW BACKENDS;` command, if not forwarded to the Master FE node, can only see some basic information such as whether the node is alive, and forwarded to the Master FE to obtain more detailed information including the node startup time and the last heartbeat time.
    
    The commands currently affected by this parameter are as follows:
    
    1. `SHOW FRONTEND;`

        Forward to Master to view the last heartbeat information.
    
    2. `SHOW BACKENDS;`

        Forward to Master to view startup time, last heartbeat information, and disk capacity information.
        
    3. `SHOW BROKERS;`

        Forward to Master to view the start time and last heartbeat information.
        
    4. `SHOW TABLET;`/`ADMIN SHOW REPLICA DISTRIBUTION;`/`ADMIN SHOW REPLICA STATUS;`

        Forward to Master to view the tablet information stored in the Master FE metadata. Under normal circumstances, the tablet information in different FE metadata should be consistent. When a problem occurs, this method can be used to compare the difference between the current FE and Master FE metadata.
        
    5. `SHOW PROC;`

        Forward to Master to view information about the relevant PROC stored in the Master FE metadata. Mainly used for metadata comparison.
        
* `init_connect`
   
    Used for compatibility with MySQL clients. No practical effect.
    
* `interactive_timeout`

    Used for compatibility with MySQL clients. No practical effect.
    
* `is_report_success`

    Used to set whether you need to view the profile of the query. The default is false, which means no profile is required.
    
    By default, the BE sends a profile to the FE for viewing errors only if an error occurs in the query. A successful query will not send a profile. Sending a profile will incur a certain amount of network overhead, which is detrimental to a high concurrent query scenario.
    
    When the user wants to analyze the profile of a query, the query can be sent after this variable is set to true. After the query is finished, you can view the profile on the web page of the currently connected FE:
    
    `fe_host:fe_http:port/query`
    
    It will display the most recent 100 queries which `is_report_success` is set to true.
    
* `language`

    Used for compatibility with MySQL clients. No practical effect.
    
* `license`
    
    Show Doris's license. No other effect.

* `load_mem_limit`

    Used to specify the memory limit of the load operation. The default is 0, which means that this variable is not used, and `exec_mem_limit` is used as the memory limit for the load operation.

    This variable is usually used for INSERT operations. Because the INSERT operation has both query and load part. If the user does not set this variable, the respective memory limits of the query and load part are `exec_mem_limit`. Otherwise, the memory of query part of INSERT is limited to `exec_mem_limit`, and the load part is limited to` load_mem_limit`.

    For other load methods, such as BROKER LOAD, STREAM LOAD, the memory limit still uses `exec_mem_limit`.
    
* `lower_case_table_names`

    Used for compatibility with MySQL clients. Cannot be set. Table names in current Doris are case sensitive by default.
    
* `max_allowed_packet`

    Used for compatible JDBC connection pool C3P0. No practical effect.
    
* `net_buffer_length`

    Used for compatibility with MySQL clients. No practical effect.

* `net_read_timeout`

    Used for compatibility with MySQL clients. No practical effect.
    
* `net_write_timeout`

    Used for compatibility with MySQL clients. No practical effect.
    
* `parallel_exchange_instance_num`

    Used to set the number of exchange nodes used by an upper node to receive data from the lower node in the execution plan. The default is -1, which means that the number of exchange nodes is equal to the number of execution instances of the lower nodes (default behavior). When the setting is greater than 0 and less than the number of execution instances of the lower node, the number of exchange nodes is equal to the set value.
    
    In a distributed query execution plan, the upper node usually has one or more exchange nodes for receiving data from the execution instances of the lower nodes on different BEs. Usually the number of exchange nodes is equal to the number of execution instances of the lower nodes.
    
    In some aggregate query scenarios, if the amount of data to be scanned at the bottom is large, but the amount of data after aggregation is small, you can try to modify this variable to a smaller value, which can reduce the resource overhead of such queries. Such as the scenario of aggregation query on the DUPLICATE KEY data model.

* `parallel_fragment_exec_instance_num`

    For the scan node, set its number of instances to execute on each BE node. The default is 1.
    
    A query plan typically produces a set of scan ranges, the range of data that needs to be scanned. These data are distributed across multiple BE nodes. A BE node will have one or more scan ranges. By default, a set of scan ranges for each BE node is processed by only one execution instance. When the machine resources are abundant, you can increase the variable and let more execution instances process a set of scan ranges at the same time, thus improving query efficiency.
    
    Modifying this parameter is only helpful for improving the efficiency of the scan node. Larger values ​​may consume more machine resources such as CPU, memory, and disk IO.
    
* `query_cache_size`

    Used for compatibility with MySQL clients. No practical effect.
    
* `query_cache_type`

    Used for compatible JDBC connection pool C3P0. No practical effect.
    
* `query_timeout`

    Used to set the query timeout. This variable applies to all query statements in the current connection, as well as INSERT statements. The default is 5 minutes, in seconds.

* `resource_group`

    Not used.
    
* `sql_mode`

    Used to specify SQL mode to accommodate certain SQL dialects. For the SQL mode, see [here] (./sql-mode.md).
    
* `sql_safe_updates`

    Used for compatibility with MySQL clients. No practical effect.
    
* `sql_select_limit`

    Used for compatibility with MySQL clients. No practical effect.

* `system_time_zone`

    Displays the current system time zone. Cannot be changed.
    
* `time_zone`

    Used to set the time zone of the current session. The time zone has an effect on the results of certain time functions. For the time zone, see [here] (./time-zone.md).
    
* `tx_isolation`

    Used for compatibility with MySQL clients. No practical effect.
    
* `version`

    Used for compatibility with MySQL clients. No practical effect.
    
* `version_comment`

    Used to display the version of Doris. Cannot be changed.
    
* `wait_timeout`

    The length of the connection used to set up an idle connection. When an idle connection does not interact with Doris for that length of time, Doris will actively disconnect the link. The default is 8 hours, in seconds.
