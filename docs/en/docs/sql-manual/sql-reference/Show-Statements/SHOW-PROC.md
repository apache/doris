---
{
    "title": "SHOW-PROC",
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

## SHOW-PROC

### Name

SHOW PROC

### Description

The Proc system is a unique feature of Doris. Students who have used Linux may understand this concept better. In Linux systems, proc is a virtual file system, usually mounted in the /proc directory. Users can view the internal data structure of the system through this file system. For example, you can view the details of the specified pid process through /proc/pid.

Similar to the proc system in Linux, the proc system in Doris is also organized into a directory-like structure to view different system information according to the "directory path (proc path)" specified by the user.

The proc system is designed mainly for system administrators, so that it is convenient for them to view some running states inside the system. Such as the tablet status of the table, the cluster balance status, the status of various jobs, and so on. is a very useful function

There are two ways to view the proc system in Doris.

1. View through the WEB UI interface provided by Doris, visit the address: `http://FE_IP:FE_HTTP_PORT`
2. Another way is by command

All commands supported by Doris PROC can be seen through ` SHOW PROC "/";`

After connecting to Doris through the MySQL client, you can execute the SHOW PROC statement to view the information of the specified proc directory. The proc directory is an absolute path starting with "/".

The results of the show proc statement are presented in a two-dimensional table. And usually the first column of the result table is the next subdirectory of proc.

````none
mysql> show proc "/";
+---------------------------+
| name                      |
+---------------------------+
| statistic                 |
| brokers                   |
| frontends                 |
| routine_loads             |
| auth                      |
| jobs                      |
| bdbje                     |
| resources                 |
| monitor                   |
| transactions              |
| colocation_group          |
| backends                  |
| trash                     |
| cluster_balance           |
| current_queries           |
| dbs                       |
| load_error_hub            |
| current_backend_instances |
| tasks                     |
| cluster_health            |
| current_query_stmts       |
| stream_loads              |
+---------------------------+
22 rows in set (0.00 sec)
````

illustrate:

1. Statistics: It is mainly used to summarize and view the number of databases, tables, partitions, shards, and replicas in the Doris cluster. and the number of unhealthy copies. This information helps us to control the size of the cluster meta-information in general. It helps us view the cluster sharding situation from an overall perspective, and can quickly check the health of the cluster sharding. This further locates problematic data shards.
2. brokers : View cluster broker node information, equivalent to [SHOW BROKER](./SHOW-BROKER.md)
3. frontends: Display all FE node information in the cluster, including IP address, role, status, whether it is a mater, etc., equivalent to [SHOW FRONTENDS](./SHOW-FRONTENDS.md)
4. routine_loads: Display all routine load job information, including job name, status, etc.
5. auth: User name and corresponding permission information
6. jobs:
7. bdbje: To view the bdbje database list, you need to modify the `fe.conf` file to add `enable_bdbje_debug_mode=true`, and then start `FE` through `sh start_fe.sh --daemon` to enter the `debug` mode. After entering `debug` mode, only `http server` and `MySQLServer` will be started and the `BDBJE` instance will be opened, but no metadata loading and subsequent startup processes will be entered.
8. dbs: Mainly used to view the metadata information of each database and the tables in the Doris cluster. This information includes table structure, partitions, materialized views, data shards and replicas, and more. Through this directory and its subdirectories, you can clearly display the table metadata in the cluster, and locate some problems such as data skew, replica failure, etc.
9. resources : View system resources, ordinary accounts can only see resources that they have USAGE_PRIV permission to use. Only the root and admin accounts can see all resources. Equivalent to [SHOW RESOURCES](./SHOW-RESOURCES.md)
10. monitor : shows the resource usage of FE JVM
11. transactions : used to view the transaction details of the specified transaction id, equivalent to [SHOW TRANSACTION](./SHOW-TRANSACTION.md)
12. colocation_group : This command can view the existing Group information in the cluster. For details, please refer to the [Colocation Join](../../../advanced/join-optimization/colocation-join.md) chapter
13. backends: Displays the node list of BE in the cluster, equivalent to [SHOW BACKENDS](./SHOW-BACKENDS.md)
14. trash: This statement is used to view the space occupied by garbage data in the backend. Equivalent to [SHOW TRASH](./SHOW-TRASH.md)
15. cluster_balance : To check the balance of the cluster, please refer to [Data Copy Management](../../../admin-manual/maint-monitor/tablet-repair-and-balance.md)
16. current_queries : View the list of queries being executed, the SQL statement currently running.
17. load_error_hub: Doris supports centralized storage of error information generated by load jobs in an error hub. Then view the error message directly through the <code>SHOW LOAD WARNINGS;</code> statement. Shown here is the configuration information of the error hub.
18. current_backend_instances : Displays a list of be nodes that are currently executing jobs
19. tasks : Displays the total number of tasks of various jobs and the number of failures.
20. Cluster_health: Run <code>SHOW PROC '/cluster_health/tablet_health';</code> statement to view the replica status of the entire cluster.
21. Current_query_stmts: Returns the currently executing query.
22. Stream_loads: Returns the stream load task being executed.

### Example

1. For example, "/dbs" displays all databases, and "/dbs/10002" displays all tables under the database with id 10002

   ```sql
   mysql> show proc "/dbs/10002";
   +---------+----------------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
   | TableId | TableName            | IndexNum | PartitionColumnName | PartitionNum | State  | Type | LastConsistencyCheckTime | ReplicaCount |
   +---------+----------------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
   | 10065   | dwd_product_live     | 1        | dt                  | 9            | NORMAL | OLAP | NULL                     | 18           |
   | 10109   | ODS_MR_BILL_COSTS_DO | 1        | NULL                | 1            | NORMAL | OLAP | NULL                     | 1            |
   | 10119   | test                 | 1        | NULL                | 1            | NORMAL | OLAP | NULL                     | 1            |
   | 10124   | test_parquet_import  | 1        | NULL                | 1            | NORMAL | OLAP | NULL                     | 1            |
   +---------+----------------------+----------+---------------------+--------------+--------+------+--------------------------+--------------+
   4 rows in set (0.00 sec)
   ````

2. Display information about the number of all database tables in the cluster.

   ```sql
   mysql> show proc '/statistic';
   +-------+----------------------+----------+--------------+----------+-----------+------------+
   | DbId  | DbName               | TableNum | PartitionNum | IndexNum | TabletNum | ReplicaNum |
   +-------+----------------------+----------+--------------+----------+-----------+------------+
   | 10002 | default_cluster:test | 4        | 12           | 12       | 21        | 21         |
   | Total | 1                    | 4        | 12           | 12       | 21        | 21         |
   +-------+----------------------+----------+--------------+----------+-----------+------------+
   2 rows in set (0.00 sec)
   ````

3. The following command can view the existing Group information in the cluster.

   ````
   SHOW PROC '/colocation_group';
   
   +-------------+--------------+--------------+------------+----------------+----------+----------+
   | GroupId     | GroupName    | TableIds     | BucketsNum | ReplicationNum | DistCols | IsStable |
   +-------------+--------------+--------------+------------+----------------+----------+----------+
   | 10005.10008 | 10005_group1 | 10007, 10040 | 10         | 3              | int(11)  | true     |
   +-------------+--------------+--------------+------------+----------------+----------+----------+
   ````

   - GroupId: The cluster-wide unique identifier of a group, the first half is the db id, and the second half is the group id.
   - GroupName: The full name of the Group.
   - TabletIds: The id list of Tables contained in this Group.
   - BucketsNum: The number of buckets.
   - ReplicationNum: The number of replicas.
   - DistCols: Distribution columns, that is, the bucket column type.
   - IsStable: Whether the Group is stable (for the definition of stability, see the `Colocation replica balance and repair` section).

4. Use the following commands to further view the data distribution of a Group:

   ```sql
   SHOW PROC '/colocation_group/10005.10008';
   
   +-------------+---------------------+
   | BucketIndex | BackendIds          |
   +-------------+---------------------+
   | 0           | 10004, 10002, 10001 |
   | 1           | 10003, 10002, 10004 |
   | 2           | 10002, 10004, 10001 |
   | 3           | 10003, 10002, 10004 |
   | 4           | 10002, 10004, 10003 |
   | 5           | 10003, 10002, 10001 |
   | 6           | 10003, 10004, 10001 |
   | 7           | 10003, 10004, 10002 |
   +-------------+---------------------+
   ````

   - BucketIndex: The index of the bucket sequence.
   - BackendIds: The list of BE node IDs where the data shards in the bucket are located.

5. Display the total number of tasks of various jobs and the number of failures.

   ```sql
   mysql> show proc '/tasks';
   +-------------------------+-----------+----------+
   | TaskType                | FailedNum | TotalNum |
   +-------------------------+-----------+----------+
   | CREATE                  | 0         | 0        |
   | DROP                    | 0         | 0        |
   | PUSH                    | 0         | 0        |
   | CLONE                   | 0         | 0        |
   | STORAGE_MEDIUM_MIGRATE  | 0         | 0        |
   | ROLLUP                  | 0         | 0        |
   | SCHEMA_CHANGE           | 0         | 0        |
   | CANCEL_DELETE           | 0         | 0        |
   | MAKE_SNAPSHOT           | 0         | 0        |
   | RELEASE_SNAPSHOT        | 0         | 0        |
   | CHECK_CONSISTENCY       | 0         | 0        |
   | UPLOAD                  | 0         | 0        |
   | DOWNLOAD                | 0         | 0        |
   | CLEAR_REMOTE_FILE       | 0         | 0        |
   | MOVE                    | 0         | 0        |
   | REALTIME_PUSH           | 0         | 0        |
   | PUBLISH_VERSION         | 0         | 0        |
   | CLEAR_ALTER_TASK        | 0         | 0        |
   | CLEAR_TRANSACTION_TASK  | 0         | 0        |
   | RECOVER_TABLET          | 0         | 0        |
   | STREAM_LOAD             | 0         | 0        |
   | UPDATE_TABLET_META_INFO | 0         | 0        |
   | ALTER                   | 0         | 0        |
   | INSTALL_PLUGIN          | 0         | 0        |
   | UNINSTALL_PLUGIN        | 0         | 0        |
   | Total                   | 0         | 0        |
   +-------------------------+-----------+----------+
   26 rows in set (0.01 sec)
   ````

5. Display the replica status of the entire cluster.
   ```sql
   mysql> show proc '/cluster_health/tablet_health';
   +----------+---------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
   | DbId     | DbName                    | TabletNum | HealthyNum | ReplicaMissingNum | VersionIncompleteNum | ReplicaRelocatingNum | RedundantNum | ReplicaMissingInClusterNum | ReplicaMissingForTagNum | ForceRedundantNum | ColocateMismatchNum | ColocateRedundantNum | NeedFurtherRepairNum | UnrecoverableNum | ReplicaCompactionTooSlowNum | InconsistentNum | OversizeNum | CloningNum |
   +----------+---------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
   | 25852112 | default_cluster:bowen     | 1920      | 1920       | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
   | 25342914 | default_cluster:bw        | 128       | 128        | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
   | 2575532  | default_cluster:cps       | 1440      | 1440       | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 16          | 0          |
   | 26150325 | default_cluster:db        | 38374     | 38374      | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 453         | 0          |
   +----------+---------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
   4 rows in set (0.01 sec)
   ```

   View the replica status under a database, such as a database with a DbId of 25852112.

   ```sql
   mysql> show proc '/cluster_health/tablet_health/25852112';
   ```
### Keywords

    SHOW, PROC

### Best Practice

