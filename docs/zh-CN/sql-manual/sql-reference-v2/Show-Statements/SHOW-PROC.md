---
{
    "title": "SHOW-PROC",
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

## SHOW-PROC

### Name

SHOW PROC 

### Description

Proc 系统是 Doris 的一个比较有特色的功能。使用过 Linux 的同学可能比较了解这个概念。在 Linux 系统中，proc 是一个虚拟的文件系统，通常挂载在 /proc 目录下。用户可以通过这个文件系统来查看系统内部的数据结构。比如可以通过 /proc/pid 查看指定 pid 进程的详细情况。

和 Linux 中的 proc 系统类似，Doris 中的 proc 系统也被组织成一个类似目录的结构，根据用户指定的“目录路径（proc 路径）”，来查看不同的系统信息。

proc 系统被设计为主要面向系统管理人员，方便其查看系统内部的一些运行状态。如表的tablet状态、集群均衡状态、各种作业的状态等等。是一个非常实用的功能

Doris 中有两种方式可以查看 proc 系统。

1. 通过 Doris 提供的 WEB UI 界面查看，访问地址：`http://FE_IP:FE_HTTP_PORT`
2. 另外一种方式是通过命令

通过 ` SHOW PROC  "/";` 可看到 Doris PROC支持的所有命令

通过 MySQL 客户端连接 Doris 后，可以执行 SHOW PROC 语句查看指定 proc 目录的信息。proc 目录是以 "/" 开头的绝对路径。

show proc 语句的结果以二维表的形式展现。而通常结果表的第一列的值为 proc 的下一级子目录。

```sql
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
+---------------------------+
19 rows in set (0.00 sec)
```

说明：

1. statistics：主要用于汇总查看 Doris 集群中数据库、表、分区、分片、副本的数量。以及不健康副本的数量。这个信息有助于我们总体把控集群元信息的规模。帮助我们从整体视角查看集群分片情况，能够快速查看集群分片的健康情况。从而进一步定位有问题的数据分片。
2. brokers : 查看集群 Broker 节点信息，等同于 [SHOW BROKER](./SHOW-BROKER.html)   
3. frontends ：显示集群中所有的 FE 节点信息，包括IP地址、角色、状态、是否是mater等，等同于 [SHOW FRONTENDS](./SHOW-FRONTENDS.html)   
4. routine_loads ： 显示所有的 routine load 作业信息，包括作业名称、状态等
5. auth：用户名称及对应的权限信息
6. jobs ：
7. bdbje：查看 bdbje 数据库列表，需要修改 `fe.conf` 文件增加 `enable_bdbje_debug_mode=true` , 然后通过 `sh start_fe.sh --daemon` 启动 `FE` 即可进入 `debug` 模式。 进入 `debug` 模式之后，仅会启动 `http server` 和  `MySQLServer` 并打开 `BDBJE` 实例，但不会进入任何元数据的加载及后续其他启动流程，
8. dbs ： 主要用于查看 Doris 集群中各个数据库以及其中的表的元数据信息。这些信息包括表结构、分区、物化视图、数据分片和副本等等。通过这个目录和其子目录，可以清楚的展示集群中的表元数据情况，以及定位一些如数据倾斜、副本故障等问题
9. resources : 查看系统资源，普通账户只能看到自己有 USAGE_PRIV 使用权限的资源。只有root和admin账户可以看到所有的资源。等同于 [SHOW RESOURCES](./SHOW-RESOURCES.html)
10.  monitor : 显示的是 FE JVM 的资源使用情况     
11.  transactions ：用于查看指定 transaction id 的事务详情，等同于 [SHOW TRANSACTION](./SHOW-TRANSACTION.html)
12.  colocation_group :   该命令可以查看集群内已存在的 Group 信息, 具体可以查看 [Colocation Join](../../../advanced/join-optimization/colocation-join.html) 章节
13.  backends ：显示集群中 BE 的节点列表  ， 等同于 [SHOW BACKENDS](./SHOW-BACKENDS.html)        
14.  trash ：该语句用于查看 backend 内的垃圾数据占用空间。 等同于 [SHOW TRASH](./SHOW-TRASH.html)    
15. cluster_balance  ： 查看集群均衡情况，具体参照 [数据副本管理](../../../admin-manual/maint-monitor/tablet-repair-and-balance.html)
16. current_queries  : 查看正在执行的查询列表，当前正在运行的SQL语句。                          
17.  load_error_hub ：Doris 支持将 load 作业产生的错误信息集中存储到一个 error hub 中。然后直接通过 <code>SHOW LOAD WARNINGS;</code> 语句查看错误信息。这里展示的就是 error hub 的配置信息。
18.  current_backend_instances ：显示当前正在执行作业的be节点列表
19.  tasks :  显示现在各种作业的任务总量，及失败的数量。

### Example

1. 如 "/dbs" 展示所有数据库，而 "/dbs/10002" 展示 id 为 10002 的数据库下的所有表

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
```

2. 展示集群中所有库表个数相关的信息。

   ```sql
   mysql> show proc '/statistic';
   +-------+----------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+------------------+--------------+
   | DbId  | DbName               | TableNum | PartitionNum | IndexNum | TabletNum | ReplicaNum | UnhealthyTabletNum | InconsistentTabletNum | CloningTabletNum | BadTabletNum |
   +-------+----------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+------------------+--------------+
   | 10002 | default_cluster:test | 4        | 12           | 12       | 21        | 21         | 0                  | 0                     | 0                | 0            |
   | Total | 1                    | 4        | 12           | 12       | 21        | 21         | 0                  | 0                     | 0                | 0            |
   +-------+----------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+------------------+--------------+
   2 rows in set (0.00 sec)
   ```

3. 以下命令可以查看集群内已存在的 Group 信息。

   ```sql
   SHOW PROC '/colocation_group';
   
   +-------------+--------------+--------------+------------+----------------+----------+----------+
   | GroupId     | GroupName    | TableIds     | BucketsNum | ReplicationNum | DistCols | IsStable |
   +-------------+--------------+--------------+------------+----------------+----------+----------+
   | 10005.10008 | 10005_group1 | 10007, 10040 | 10         | 3              | int(11)  | true     |
   +-------------+--------------+--------------+------------+----------------+----------+----------+
   ```

   - GroupId： 一个 Group 的全集群唯一标识，前半部分为 db id，后半部分为 group id。
   - GroupName： Group 的全名。
   - TabletIds： 该 Group 包含的 Table 的 id 列表。
   - BucketsNum： 分桶数。
   - ReplicationNum： 副本数。
   - DistCols： Distribution columns，即分桶列类型。
   - IsStable： 该 Group 是否稳定（稳定的定义，见 `Colocation 副本均衡和修复` 一节）。

4. 通过以下命令可以进一步查看一个 Group 的数据分布情况：

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
   ```

   - BucketIndex： 分桶序列的下标。
   - BackendIds： 分桶中数据分片所在的 BE 节点 id 列表。

5. 显示现在各种作业的任务总量，及失败的数量

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
   ```

### Keywords

    SHOW, PROC 

### Best Practice

