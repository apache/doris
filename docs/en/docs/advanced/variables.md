---
{
    "title": "Variable",
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

Note that before version 1.1, after the setting takes effect globally, the setting value will be inherited in subsequent new session connections, but the value in the current session will remain unchanged.
After version 1.1 (inclusive), after the setting takes effect globally, the setting value will be used in subsequent new session connections, and the value in the current session will also change.

For session-only, set by the `SET var_name=xxx;` statement. Such as:

```
SET exec_mem_limit = 137438953472;
SET forward_to_master = true;
SET time_zone = "Asia/Shanghai";
```

For global-level, set by `SET GLOBAL var_name=xxx;`. Such as:

```
SET GLOBAL exec_mem_limit = 137438953472
```

> Note 1: Only ADMIN users can set variable at global-level.

Variables that support both session-level and global-level setting include:

* `time_zone`
* `wait_timeout`
* `sql_mode`
* `enable_profile`
* `query_timeout`
* <version since="dev" type="inline">`insert_timeout`</version>
* `exec_mem_limit`
* `batch_size`
* `parallel_fragment_exec_instance_num`
* `parallel_exchange_instance_num`
* `allow_partition_column_nullable`
* `insert_visible_timeout_ms`
* `enable_fold_constant_by_be`

Variables that support only global-level setting include:

* `default_rowset_type`
* `default_password_lifetime`
* `password_history`
* `validate_password_policy`

At the same time, variable settings also support constant expressions. Such as:

```
SET exec_mem_limit = 10 * 1024 * 1024 * 1024;
SET forward_to_master = concat('tr', 'u', 'e');
```

### Set variables in the query statement

In some scenarios, we may need to set variables specifically for certain queries.
The SET_VAR hint sets the session value of a system variable temporarily (for the duration of a single statement). Examples:

```
SELECT /*+ SET_VAR(exec_mem_limit = 8589934592) */ name FROM people ORDER BY name;
SELECT /*+ SET_VAR(query_timeout = 1, enable_partition_cache=true) */ sleep(3);
```

Note that the comment must start with /*+ and can only follow the SELECT.

## Supported variables

* `SQL_AUTO_IS_NULL`

    Used for compatible JDBC connection pool C3P0. No practical effect.
    
* `auto_increment_increment`

    Used for compatibility with MySQL clients. No practical effect. Although Doris already has [AUTO_INCREMENT](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE#column_definition_list) feature, but this parameter does not affect the behavior of 'AUTO_INCREMENT'. Same as auto_increment_offset.
    
* `autocommit`

    Used for compatibility with MySQL clients. No practical effect.
    
* `auto_broadcast_join_threshold`

    The maximum size in bytes of the table that will be broadcast to all nodes when a join is performed, broadcast can be disabled by setting this value to -1.

    The system provides two join implementation methods, `broadcast join` and `shuffle join`.

    `broadcast join` means that after conditional filtering the small table, broadcast it to each node where the large table is located to form an in-memory Hash table, and then stream the data of the large table for Hash Join.

    `shuffle join` refers to hashing both small and large tables according to the join key, and then performing distributed join.

    `broadcast join` has better performance when the data volume of the small table is small. On the contrary, shuffle join has better performance.

    The system will automatically try to perform a Broadcast Join, or you can explicitly specify the implementation of each join operator. The system provides a configurable parameter `auto_broadcast_join_threshold`, which specifies the upper limit of the memory used by the hash table to the overall execution memory when `broadcast join` is used. The value ranges from 0 to 1, and the default value is 0.8. When the memory used by the system to calculate the hash table exceeds this limit, it will automatically switch to using `shuffle join`

    The overall execution memory here is: a fraction of what the query optimizer estimates

    > Note:
    >
    > It is not recommended to use this parameter to adjust, if you must use a certain join, it is recommended to use hint, such as join[shuffle]


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

* `have_query_cache`

  Used for compatibility with MySQL clients. No practical effect.

* `default_order_by_limit`

  Used to control the default number of items returned after OrderBy. The default value is -1, and the maximum number of records after the query is returned by default, and the upper limit is the MAX_VALUE of the long data type.

* `delete_without_partition`

    When set to true. When using the delete command to delete partition table data, no partition is required. The delete operation will be automatically applied to all partitions.

     Note, however, that the automatic application to all partitions may cause the delete command to take a long time to trigger a large number of subtasks and cause a long time. If it is not necessary, it is not recommended to turn it on.

* `disable_colocate_join`

    Controls whether the [Colocation Join](../query-acceleration/join-optimization/colocation-join.md) function is enabled. The default is false, which means that the feature is enabled. True means that the feature is disabled. When this feature is disabled, the query plan will not attempt to perform a Colocation Join.
    
* `enable_bucket_shuffle_join`

    Controls whether the [Bucket Shuffle Join](../query-acceleration/join-optimization/bucket-shuffle-join.md) function is enabled. The default is true, which means that the feature is enabled. False means that the feature is disabled. When this feature is disabled, the query plan will not attempt to perform a Bucket Shuffle Join.

* `disable_streaming_preaggregations`

    Controls whether streaming pre-aggregation is turned on. The default is false, which is enabled. Currently not configurable and enabled by default.
    
* `enable_insert_strict`

    Used to set the `strict` mode when loading data via INSERT statement. The default is false, which means that the `strict` mode is not turned on. For an introduction to this mode, see [here](../data-operate/import/import-way/insert-into-manual.md).

* `enable_spilling`

    Used to set whether to enable external sorting. The default is false, which turns off the feature. This feature is enabled when the user does not specify a LIMIT condition for the ORDER BY clause and also sets `enable_spilling` to true. When this feature is enabled, the temporary data is stored in the `doris-scratch/` directory of the BE data directory and the temporary data is cleared after the query is completed.
    
    This feature is mainly used for sorting operations with large amounts of data using limited memory.
    
    Note that this feature is experimental and does not guarantee stability. Please turn it on carefully.
    
* `exec_mem_limit`

    Used to set the memory limit for a single query. The default is 2GB, you can set it in B/K/KB/M/MB/G/GB/T/TB/P/PB, the default is B.
    
    This parameter is used to limit the memory that can be used by an instance of a single query fragment in a query plan. A query plan may have multiple instances, and a BE node may execute one or more instances. Therefore, this parameter does not accurately limit the memory usage of a query across the cluster, nor does it accurately limit the memory usage of a query on a single BE node. The specific needs need to be judged according to the generated query plan.
    
    Usually, only some blocking nodes (such as sorting node, aggregation node, and join node) consume more memory, while in other nodes (such as scan node), data is streamed and does not occupy much memory.
    
    When a `Memory Exceed Limit` error occurs, you can try to increase the parameter exponentially, such as 4G, 8G, 16G, and so on.

    It should be noted that this value may fluctuate by a few MB.
    
* `forward_to_master`

    The user sets whether to forward some commands to the Master FE node for execution. The default is `true`, which means forwarding. There are multiple FE nodes in Doris, one of which is the Master node. Usually users can connect to any FE node for full-featured operation. However, some of detail information can only be obtained from the Master FE node.
    
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
    
* `enable_profile`

    Used to set whether you need to view the profile of the query. The default is false, which means no profile is required.
    
    By default, the BE sends a profile to the FE for viewing errors only if an error occurs in the query. A successful query will not send a profile. Sending a profile will incur a certain amount of network overhead, which is detrimental to a high concurrent query scenario.
    
    When the user wants to analyze the profile of a query, the query can be sent after this variable is set to true. After the query is finished, you can view the profile on the web page of the currently connected FE:
    
    `fe_host:fe_http:port/query`
    
    It will display the most recent 100 queries which `enable_profile` is set to true.
    
* `language`

    Used for compatibility with MySQL clients. No practical effect.
    
* `license`
  
    Show Doris's license. No other effect.

* `lower_case_table_names`

    Used to control whether the user table name is case-sensitive.

    A value of 0 makes the table name case-sensitive. The default is 0.

    When the value is 1, the table name is case insensitive. Doris will convert the table name to lowercase when storing and querying.  
    The advantage is that any case of table name can be used in one statement. The following SQL is correct:
    ```
    mysql> show tables;
    +------------------+
    | Tables_ in_testdb|
    +------------------+
    | cost             |
    +------------------+
    mysql> select * from COST where COst.id < 100 order by cost.id;
    ```
    The disadvantage is that the table name specified in the table creation statement cannot be obtained after table creation. The table name viewed by 'show tables' is lower case of the specified table name.

    When the value is 2, the table name is case insensitive. Doris stores the table name specified in the table creation statement and converts it to lowercase for comparison during query.  
    The advantage is that the table name viewed by 'show tables' is the table name specified in the table creation statement;  
    The disadvantage is that only one case of table name can be used in the same statement. For example, the table name 'cost' can be used to query the 'cost' table:
    ```
    mysql> select * from COST where COST.id < 100 order by COST.id;
    ```

    This variable is compatible with MySQL and must be configured at cluster initialization by specifying `lower_case_table_names=` in fe.conf. It cannot be modified by the `set` statement after cluster initialization is complete, nor can it be modified by restarting or upgrading the cluster.

    The system view table names in information_schema are case-insensitive and behave as 2 when the value of `lower_case_table_names` is 0.


* `max_allowed_packet`

    Used for compatible JDBC connection pool C3P0. No practical effect.
    
* `max_pushdown_conditions_per_column`

    For the specific meaning of this variable, please refer to the description of `max_pushdown_conditions_per_column` in [BE Configuration](../admin-manual/config/be-config.md). This variable is set to -1 by default, which means that the configuration value in `be.conf` is used. If the setting is greater than 0, the query in the current session will use the variable value, and ignore the configuration value in `be.conf`.

* `max_scan_key_num`

    For the specific meaning of this variable, please refer to the description of `doris_max_scan_key_num` in [BE Configuration](../admin-manual/config/be-config.md). This variable is set to -1 by default, which means that the configuration value in `be.conf` is used. If the setting is greater than 0, the query in the current session will use the variable value, and ignore the configuration value in `be.conf`.

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
    
    The number of scan instances determines the number of other execution nodes in the upper layer, such as aggregate nodes and join nodes. Therefore, it is equivalent to increasing the concurrency of the entire query plan execution. Modifying this parameter will help improve the efficiency of large queries, but larger values will consume more machine resources, such as CPU, memory, and disk IO.
    
* `query_cache_size`

    Used for compatibility with MySQL clients. No practical effect.
    
* `query_cache_type`

    Used for compatible JDBC connection pool C3P0. No practical effect.
    
* `query_timeout`

    Used to set the query timeout. This variable applies to all query statements in the current connection. Particularly, timeout of INSERT statements is recommended to be managed by the insert_timeout below. The default is 15 minutes, in seconds.

* `insert_timeout`

  <version since="dev"></version>Used to set the insert timeout. This variable applies to INSERT statements particularly in the current connection, and is recommended to manage long-duration INSERT action. The default is 4 hours, in seconds. It will lose effect when query_timeout is
    greater than itself to make it compatible with the habits of older version users to use query_timeout to control the timeout of INSERT statements.

* `resource_group`

    Not used.
    
* `send_batch_parallelism`

    Used to set the default parallelism for sending batch when execute InsertStmt operation, if the value for parallelism exceed `max_send_batch_parallelism_per_job` in BE config, then the coordinator BE will use the value of `max_send_batch_parallelism_per_job`.

* `sql_mode`

    Used to specify SQL mode to accommodate certain SQL dialects. For the SQL mode, see [here](./sql-mode.md).
    
* `sql_safe_updates`

    Used for compatibility with MySQL clients. No practical effect.
    
* `sql_select_limit`

    Used to limit return rows of select stmt, including select clause of insert stmt.

* `system_time_zone`

    Displays the current system time zone. Cannot be changed.
    
* `time_zone`

    Used to set the time zone of the current session. The time zone has an effect on the results of certain time functions. For the time zone, see [here](./time-zone.md).
    
* `tx_isolation`

    Used for compatibility with MySQL clients. No practical effect.
    
* `tx_read_only`

    Used for compatibility with MySQL clients. No practical effect.
    
* `transaction_read_only`

    Used for compatibility with MySQL clients. No practical effect.
    
* `transaction_isolation`

    Used for compatibility with MySQL clients. No practical effect.
    
* `version`

    Used for compatibility with MySQL clients. No practical effect.

* `performance_schema`

    Used for compatibility with MySQL JDBC 8.0.16 or later version. No practical effect.    
    
* `version_comment`

    Used to display the version of Doris. Cannot be changed.
    
* `wait_timeout`

    The length of the connection used to set up an idle connection. When an idle connection does not interact with Doris for that length of time, Doris will actively disconnect the link. The default is 8 hours, in seconds.

* `default_rowset_type`

    Used for setting the default storage format of Backends storage engine. Valid options: alpha/beta

* `use_v2_rollup`

    Used to control the sql query to use segment v2 rollup index to get data. This variable is only used for validation when upgrading to segment v2 feature. Otherwise, not recommended to use.

* `rewrite_count_distinct_to_bitmap_hll`

    Whether to rewrite count distinct queries of bitmap and HLL types as bitmap_union_count and hll_union_agg.

* `prefer_join_method`

    When choosing the join method(broadcast join or shuffle join), if the broadcast join cost and shuffle join cost are equal, which join method should we prefer.

    Currently, the optional values for this variable are "broadcast" or "shuffle".

* `allow_partition_column_nullable`

    Whether to allow the partition column to be NULL when creating the table. The default is true, which means NULL is allowed. false means the partition column must be defined as NOT NULL.

* `insert_visible_timeout_ms`

    When execute insert statement, doris will wait for the transaction to commit and visible after the import is completed.
    This parameter controls the timeout of waiting for transaction to be visible. The default value is 10000, and the minimum value is 1000.

* `enable_exchange_node_parallel_merge`

    In a sort query, when an upper level node receives the ordered data of the lower level node, it will sort the corresponding data on the exchange node to ensure that the final data is ordered. However, when a single thread merges multiple channels of data, if the amount of data is too large, it will lead to a single point of exchange node merge bottleneck.

    Doris optimizes this part if there are too many data nodes in the lower layer. Exchange node will start multithreading for parallel merging to speed up the sorting process. This parameter is false by default, which means that exchange node does not adopt parallel merge sort to reduce the extra CPU and memory consumption.

* `extract_wide_range_expr`

    Used to control whether turn on the 'Wide Common Factors' rule. The value has two: true or false. On by default.

* `enable_fold_constant_by_be`

    Used to control the calculation method of constant folding. The default is `false`, that is, calculation is performed in `FE`; if it is set to `true`, it will be calculated by `BE` through `RPC` request.

* `cpu_resource_limit`

     Used to limit the resource overhead of a query. This is an experimental feature. The current implementation is to limit the number of scan threads for a query on a single node. The number of scan threads is limited, and the data returned from the bottom layer slows down, thereby limiting the overall computational resource overhead of the query. Assuming it is set to 2, a query can use up to 2 scan threads on a single node.

     This parameter will override the effect of `parallel_fragment_exec_instance_num`. That is, assuming that `parallel_fragment_exec_instance_num` is set to 4, and this parameter is set to 2. Then 4 execution instances on a single node will share up to 2 scanning threads.

     This parameter will be overridden by the `cpu_resource_limit` configuration in the user property.

     The default is -1, which means no limit.

* `disable_join_reorder`

    Used to turn off all automatic join reorder algorithms in the system. There are two values: true and false.It is closed by default, that is, the automatic join reorder algorithm of the system is adopted. After set to true, the system will close all automatic sorting algorithms, adopt the original SQL table order, and execute join

* `enable_infer_predicate`

    Used to control whether to perform predicate derivation. There are two values: true and false. It is turned off by default, that is, the system does not perform predicate derivation, and uses the original predicate to perform related operations. After it is set to true, predicate expansion is performed.

* `return_object_data_as_binary`
  Used to identify whether to return the bitmap/hll result in the select result. In the select into outfile statement, if the export file format is csv, the bimap/hll data will be base64-encoded, if it is the parquet file format, the data will be stored as a byte array. Below will be an example of Java, more examples can be found in [samples](https://github.com/apache/doris/tree/master/samples/read_bitmap).

  ```java
  try (Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:9030/test?user=root");
               Statement stmt = conn.createStatement()
  ) {
      stmt.execute("set return_object_data_as_binary=true"); // IMPORTANT!!!
      ResultSet rs = stmt.executeQuery("select uids from t_bitmap");
      while(rs.next()){
          byte[] bytes = rs.getBytes(1);
          RoaringBitmap bitmap32 = new RoaringBitmap();
          switch(bytes[0]) {
              case 0: // for empty bitmap
                  break;
              case 1: // for only 1 element in bitmap32
                  bitmap32.add(ByteBuffer.wrap(bytes,1,bytes.length-1)
                          .order(ByteOrder.LITTLE_ENDIAN)
                          .getInt());
                  break;
              case 2: // for more than 1 elements in bitmap32
                  bitmap32.deserialize(ByteBuffer.wrap(bytes,1,bytes.length-1));
                  break;
              // for more details, see https://github.com/apache/doris/tree/master/samples/read_bitmap
          }
      }
  }
  ```

* `block_encryption_mode`
  The block_encryption_mode variable controls the block encryption mode. The default setting is empty, when use AES equal to `AES_128_ECB`, when use SM4 equal to `SM3_128_ECB`
  available values:
  
  ```
    AES_128_ECB,
    AES_192_ECB,
    AES_256_ECB,
    AES_128_CBC,
    AES_192_CBC,
    AES_256_CBC,
    AES_128_CFB,
    AES_192_CFB,
    AES_256_CFB,
    AES_128_CFB1,
    AES_192_CFB1,
    AES_256_CFB1,
    AES_128_CFB8,
    AES_192_CFB8,
    AES_256_CFB8,
    AES_128_CFB128,
    AES_192_CFB128,
    AES_256_CFB128,
    AES_128_CTR,
    AES_192_CTR,
    AES_256_CTR,
    AES_128_OFB,
    AES_192_OFB,
    AES_256_OFB,
    SM4_128_ECB,
    SM4_128_CBC,
    SM4_128_CFB128,
    SM4_128_OFB,
    SM4_128_CTR,
  ```
  
* `enable_infer_predicate`
  
  Used to control whether predicate deduction is performed. There are two values: true and false. It is turned off by default, and the system does not perform predicate deduction, and uses the original predicate for related operations. When set to true, predicate expansion occurs.

* `trim_tailing_spaces_for_external_table_query`

  Used to control whether trim the tailing spaces while quering Hive external tables. The default is false.

* `skip_storage_engine_merge`

    For debugging purpose. In vectorized execution engine, in case of problems of reading data of Aggregate Key model and Unique Key model, setting value to `true` will read data as Duplicate Key model.

* `skip_delete_predicate`

    For debugging purpose. In vectorized execution engine, in case of problems of reading data, setting value to `true` will also read deleted data.

* `skip_delete_bitmap`

    For debugging purpose. In Unique Key MoW table, in case of problems of reading data, setting value to `true` will also read deleted data.

* `default_password_lifetime`

	Default password expiration time. The default value is 0, which means no expiration. The unit is days. This parameter is only enabled if the user's password expiration property has a value of DEFAULT. like:

   ````
   CREATE USER user1 IDENTIFIED BY "12345" PASSWORD_EXPIRE DEFAULT;
   ALTER USER user1 PASSWORD_EXPIRE DEFAULT;
   ````

* `password_history`

	The default number of historical passwords. The default value is 0, which means no limit. This parameter is enabled only when the user's password history attribute is the DEFAULT value. like:

   ````
   CREATE USER user1 IDENTIFIED BY "12345" PASSWORD_HISTORY DEFAULT;
   ALTER USER user1 PASSWORD_HISTORY DEFAULT;
   ````

* `validate_password_policy`

	Password strength verification policy. Defaults to `NONE` or `0`, i.e. no verification. Can be set to `STRONG` or `2`. When set to `STRONG` or `2`, when setting a password via the `ALTER USER` or `SET PASSWORD` commands, the password must contain any of "uppercase letters", "lowercase letters", "numbers" and "special characters". 3 items, and the length must be greater than or equal to 8. Special characters include: `~!@#$%^&*()_+|<>,.?/:;'[]{}"`.

* `group_concat_max_len`

    For compatible purpose. This variable has no effect, just enable some BI tools can query or set this session variable sucessfully.

* `rewrite_or_to_in_predicate_threshold`

    The default threshold of rewriting OR to IN. The default value is 2, which means that when there are 2 ORs, if they can be compact, they will be rewritten as IN predicate.

*   `group_by_and_having_use_alias_first`

    Specifies whether group by and having clauses use column aliases rather than searching for column name in From clause. The default value is false.

* `enable_file_cache`

    Set wether to use block file cache, default false. This variable takes effect only if the BE config enable_file_cache=true. The cache is not used when BE config enable_file_cache=false.

* `file_cache_base_path`

    Specify the storage path of the block file cache on BE, default 'random', and randomly select the storage path configured by BE.

* `enable_inverted_index_query`

    Set wether to use inverted index query, default true.

* `topn_opt_limit_threshold`

    Set threshold for limit of topn query (eg. SELECT * FROM t ORDER BY k LIMIT n). If n <= threshold, topn optimizations(runtime predicate pushdown, two phase result fetch and read order by key) will enable automatically, otherwise disable. Default value is 1024.

* `drop_table_if_ctas_failed`

    Controls whether create table as select deletes created tables when a insert error occurs, the default value is true.

* `show_user_default_role`

    <version since="dev"></version>

    Controls whether to show each user's implicit roles in the results of `show roles`. Default is false.

* `use_fix_replica`

    <version since="1.2.0"></version>

    Use a fixed replica to query. replica starts with 0 and if use_fix_replica is 0, the smallest is used, if use_fix_replica is 1, the second smallest is used, and so on. The default value is -1, indicating that the function is disabled.

* `dry_run_query`

    <version since="dev"></version>

    If set to true, for query requests, the actual result set will no longer be returned, but only the number of rows, while for load and insert, the data is discarded by sink node, no writing happens. The default is false.

    This parameter can be used to avoid the time-consuming result set transmission when testing a large number of data sets, and focus on the time-consuming underlying query execution.

    ```
    mysql> select * from bigtable;
    +--------------+
    | ReturnedRows |
    +--------------+
    | 10000000     |
    +--------------+
    ```
  
* `enable_parquet_lazy_materialization`

  Controls whether to use lazy materialization technology in parquet reader. The default value is true.

* `enable_orc_lazy_materialization`

  Controls whether to use lazy materialization technology in orc reader. The default value is true.

* `enable_strong_consistency_read`

  Used to enable strong consistent reading. By default, Doris supports strong consistency within the same session, that is, changes to data within the same session are visible in real time. If you want strong consistent reads between sessions, set this variable to true. 

* `truncate_char_or_varchar_columns`

  Whether to truncate char or varchar columns according to the table's schema. The default is false.

  Because the maximum length of the char or varchar column in the schema of the table is inconsistent with the schema in the underlying parquet or orc file. At this time, if the option is turned on, it will be truncated according to the maximum length in the schema of the table.

* `jdbc_clickhouse_query_final`

  Whether to add the final keyword when using the JDBC Catalog function to query ClickHouse,default is false.
  
  It is used for the ReplacingMergeTree table engine of ClickHouse to deduplicate queries.

* `enable_memtable_on_sink_node`

  <version since="2.1.0">
  Whether to enable MemTable on DataSink node when loading data, default is false.
  </version>

  Build MemTable on DataSink node, and send segments to other backends through brpc streaming.
  It reduces duplicate work among replicas, and saves time in data serialization & deserialization.

* `enable_unique_key_partial_update`

  <version since="2.0.2">
  Whether to enable partial columns update semantics for native insert into statement, default is false. Please note that the default value of the session variable `enable_insert_strict`, which controls whether the insert statement operates in strict mode, is true. In other words, the insert statement is in strict mode by default, and in this mode, updating non-existing keys in partial column updates is not allowed. Therefore, when using the insert statement for partial columns update and wishing to insert non-existing keys, it is necessary to set both `enable_unique_key_partial_update` and `enable_insert_strict` to true.
  </version>

***

#### Supplementary instructions on statement execution timeout control

* Means of control

     Currently doris supports timeout control through `variable` and `user property` two systems. Both include `qeury_timeout` and `insert_timeout`.

* Priority

     The order of priority for timeout to take effect is: `session variable` > `user property` > `global variable` > `default value`

     When a variable with a higher priority is not set, the value of the next priority is automatically adopted.

* Related semantics

     `query_timeout` is used to control the timeout of all statements, and `insert_timeout` is specifically used to control the timeout of the INSERT statement. When the INSERT statement is executed, the timeout time will take
    
     The maximum value of `query_timeout` and `insert_timeout`.

     `query_timeout` and `insert_timeout` in `user property` can only be specified by the ADMIN user for the target user, and its semantics is to change the default timeout time of the specified user, and it does not have `quota` semantics.

* Precautions

     The timeout set by `user property` needs to be triggered after the client reconnects.
