---
{
    "title": "外表统计信息",
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

# 外表统计信息

外表统计信息的收集方式和收集内容与内表基本一致，详细信息可以参考[内表统计信息](../query-acceleration/statistics.md)。目前支持对Hive，Iceberg和Hudi等外部表的收集。

外表暂不支持的功能包括

1. 暂不支持直方图收集
2. 暂不支持分区的增量收集和更新
3. 暂不支持自动收集（with auto)，用户可以使用周期性收集（with period）来代替
4. 暂不支持抽样收集

下面主要介绍一下外表统计信息收集的示例和实现原理。

## 使用示例

这里主要展示在Doris中通过执行analyze命令收集外表统计信息的相关示例。除了上文提到的外表暂不支持的4个功能，其余和内表使用方式相同。下面以hive.tpch100数据库为例进行展示。tpch100数据库中包含lineitem，orders，region等8张表。

### 信息收集

外表支持手动一次性收集和周期性收集两种收集方式。

#### 手动一次性收集

- 收集lineitem表的表信息以及全部列的信息：
```
mysql> ANALYZE TABLE hive.tpch100.lineitem;
+--------------+-------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| Catalog_Name | DB_Name                 | Table_Name | Columns                                                                                                                                                                                       | Job_Id |
+--------------+-------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| hive         | default_cluster:tpch100 | lineitem   | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] | 16990  |
+--------------+-------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
1 row in set (0.06 sec)
```
此操作是异步执行，会在后台创建收集任务，可以通过job_id查看任务进度。
```
mysql> SHOW ANALYZE 16990;
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+---------+---------------------------------------------+---------------+
| job_id | catalog_name | db_name                 | tbl_name | col_name                                                                                                                                                                                      | job_type | analysis_type | message | last_exec_time_in_ms | state   | progress                                    | schedule_type |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+---------+---------------------------------------------+---------------+
| 16990  | hive         | default_cluster:tpch100 | lineitem | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] | MANUAL   | FUNDAMENTALS  |         | 2023-07-27 16:01:52  | RUNNING | 2 Finished/0 Failed/15 In Progress/17 Total | ONCE          |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+---------+---------------------------------------------+---------------+
1 row in set (0.00 sec)
```
以及查看每一列的task状态。
```
mysql> SHOW ANALYZE TASK STATUS 16990;
+---------+-----------------+---------+------------------------+-----------------+----------+
| task_id | col_name        | message | last_state_change_time | time_cost_in_ms | state    |
+---------+-----------------+---------+------------------------+-----------------+----------+
| 16991   | l_receiptdate   |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 16992   | l_returnflag    |         | 2023-07-27 16:01:44    | 14394           | FINISHED |
| 16993   | l_tax           |         | 2023-07-27 16:01:52    | 7975            | FINISHED |
| 16994   | l_shipmode      |         | 2023-07-27 16:02:11    | 18961           | FINISHED |
| 16995   | l_suppkey       |         | 2023-07-27 16:02:17    | 6684            | FINISHED |
| 16996   | l_shipdate      |         | 2023-07-27 16:02:26    | 8518            | FINISHED |
| 16997   | l_commitdate    |         | 2023-07-27 16:02:26    | 0               | RUNNING  |
| 16998   | l_partkey       |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 16999   | l_quantity      |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17000   | l_orderkey      |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17001   | l_comment       |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17002   | l_linestatus    |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17003   | l_extendedprice |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17004   | l_linenumber    |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17005   | l_shipinstruct  |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17006   | l_discount      |         | 2023-07-27 16:01:29    | 0               | PENDING  |
| 17007   | TableRowCount   |         | 2023-07-27 16:01:29    | 0               | PENDING  |
+---------+-----------------+---------+------------------------+-----------------+----------+
17 rows in set (0.00 sec)
```

- 收集tpch100数据库所有表的信息

```
mysql> ANALYZE DATABASE hive.tpch100;
+--------------+---------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| Catalog_Name | DB_Name | Table_Name | Columns                                                                                                                                                                                       | Job_Id |
+--------------+---------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| hive         | tpch100 | supplier   | [s_comment,s_phone,s_nationkey,s_name,s_address,s_acctbal,s_suppkey]                                                                                                                          | 17018  |
| hive         | tpch100 | nation     | [n_comment,n_nationkey,n_regionkey,n_name]                                                                                                                                                    | 17027  |
| hive         | tpch100 | region     | [r_regionkey,r_comment,r_name]                                                                                                                                                                | 17033  |
| hive         | tpch100 | partsupp   | [ps_suppkey,ps_availqty,ps_comment,ps_partkey,ps_supplycost]                                                                                                                                  | 17038  |
| hive         | tpch100 | orders     | [o_orderstatus,o_clerk,o_orderdate,o_shippriority,o_custkey,o_totalprice,o_orderkey,o_comment,o_orderpriority]                                                                                | 17045  |
| hive         | tpch100 | lineitem   | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] | 17056  |
| hive         | tpch100 | part       | [p_partkey,p_container,p_name,p_comment,p_brand,p_type,p_retailprice,p_mfgr,p_size]                                                                                                           | 17074  |
| hive         | tpch100 | customer   | [c_custkey,c_phone,c_acctbal,c_mktsegment,c_address,c_nationkey,c_name,c_comment]                                                                                                             | 17085  |
+--------------+---------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
8 rows in set (0.29 sec)
```
此操作会批量提交tpch100数据库下所有表的收集任务，也是异步执行，会给每个表创建一个job_id，也可以通过job_id查看每张表的任务进度。

- 同步收集

可以使用with sync同步收集表或数据库的统计信息。这时不会创建后台任务，客户端在收集完成之前会block住，直到收集任务执行完成再返回。
```
mysql> analyze table hive.tpch100.orders with sync;
Query OK, 0 rows affected (33.19 sec)
```
需要注意的是，同步收集受query_timeout session变量影响，如果超时失败，需要调大该变量后重试。比如：
`set query_timeout=3600` (超时时间设置为1小时)

#### 周期性收集

使用with period可以设置周期性的执行收集任务：

`analyze table hive.tpch100.orders with period 86400;`

这条语句创建一个周期性收集的任务，周期是1天，每天自动收集和更新orders表的统计信。

### 任务管理

任务管理的方式也和内表相同，主要包括查看job，查看task，删除job等功能。请参考[内表统计信息](../query-acceleration/statistics.md)任务管理部分。

- 查看所有job状态

```
mysql> SHOW ANALYZE;
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+----------+---------------------------------------------+---------------+
| job_id | catalog_name | db_name                 | tbl_name | col_name                                                                                                                                                                                      | job_type | analysis_type | message | last_exec_time_in_ms | state    | progress                                    | schedule_type |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+----------+---------------------------------------------+---------------+
| 16990  | hive         | default_cluster:tpch100 | lineitem | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] | MANUAL   | FUNDAMENTALS  |         | 2023-07-27 16:05:02  | FINISHED | 17 Finished/0 Failed/0 In Progress/17 Total | ONCE          |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+----------+---------------------------------------------+---------------+
```

- 查看一个job的所有task状态

```
mysql> SHOW ANALYZE TASK STATUS 16990;
+---------+-----------------+---------+------------------------+-----------------+----------+
| task_id | col_name        | message | last_state_change_time | time_cost_in_ms | state    |
+---------+-----------------+---------+------------------------+-----------------+----------+
| 16991   | l_receiptdate   |         | 2023-07-27 16:05:02    | 9560            | FINISHED |
| 16992   | l_returnflag    |         | 2023-07-27 16:01:44    | 14394           | FINISHED |
| 16993   | l_tax           |         | 2023-07-27 16:01:52    | 7975            | FINISHED |
| 16994   | l_shipmode      |         | 2023-07-27 16:02:11    | 18961           | FINISHED |
| 16995   | l_suppkey       |         | 2023-07-27 16:02:17    | 6684            | FINISHED |
| 16996   | l_shipdate      |         | 2023-07-27 16:02:26    | 8518            | FINISHED |
| 16997   | l_commitdate    |         | 2023-07-27 16:02:34    | 8380            | FINISHED |
| 16998   | l_partkey       |         | 2023-07-27 16:02:40    | 6060            | FINISHED |
| 16999   | l_quantity      |         | 2023-07-27 16:02:50    | 9768            | FINISHED |
| 17000   | l_orderkey      |         | 2023-07-27 16:02:57    | 7200            | FINISHED |
| 17001   | l_comment       |         | 2023-07-27 16:03:36    | 38468           | FINISHED |
| 17002   | l_linestatus    |         | 2023-07-27 16:03:51    | 15226           | FINISHED |
| 17003   | l_extendedprice |         | 2023-07-27 16:04:00    | 8713            | FINISHED |
| 17004   | l_linenumber    |         | 2023-07-27 16:04:06    | 6659            | FINISHED |
| 17005   | l_shipinstruct  |         | 2023-07-27 16:04:36    | 29777           | FINISHED |
| 17006   | l_discount      |         | 2023-07-27 16:04:45    | 9212            | FINISHED |
| 17007   | TableRowCount   |         | 2023-07-27 16:04:52    | 6974            | FINISHED |
+---------+-----------------+---------+------------------------+-----------------+----------+
```

- 终止未完成的job

```
KILL ANALYZE [job_id]
```

- 删除周期性收集job

```
DROP ANALYZE JOB [JOB_ID]
```

### 信息查看

信息的查看包括表的统计信息（表的行数）查看和列统计信息查看，请参考[内表统计信息](../query-acceleration/statistics.md)查看统计信息部分。

#### 表统计信息
```
SHOW TALBE [cached] stats TABLE_NAME;
```

查看statistics表中指定table的行数，如果指定cached参数，则展示的是指定表已加载到缓存中的行数信息。

```
mysql> SHOW TABLE STATS hive.tpch100.orders;
+-----------+---------------------+---------------------+
| row_count | update_time         | last_analyze_time   |
+-----------+---------------------+---------------------+
| 150000000 | 2023-07-11 23:01:49 | 2023-07-11 23:01:44 |
+-----------+---------------------+---------------------+
```

#### 列统计信息
```
SHOW COLUMN [cached] stats TABLE_NAME;
```

查看statistics表中指定table的列统计信息，如果指定cached参数，则展示的是指定表已加载到缓存中的列信息。

```
mysql> SHOW COLUMN stats hive.tpch100.orders;
+-----------------+-------+--------------+----------+----------------------+---------------+-----------------------+----------------------------+
| column_name     | count | ndv          | num_null | data_size            | avg_size_byte | min                   | max                        |
+-----------------+-------+--------------+----------+----------------------+---------------+-----------------------+----------------------------+
| o_orderstatus   | 1.5E8 | 3.0          | 0.0      | 1.50000001E8         | 1.0           | 'F'                   | 'P'                        |
| o_clerk         | 1.5E8 | 100836.0     | 0.0      | 2.250000015E9        | 15.0          | 'Clerk#000000001'     | 'Clerk#000100000'          |
| o_orderdate     | 1.5E8 | 2417.0       | 0.0      | 6.00000004E8         | 4.0           | '1992-01-01'          | '1998-08-02'               |
| o_shippriority  | 1.5E8 | 1.0          | 0.0      | 6.00000004E8         | 4.0           | 0                     | 0                          |
| o_custkey       | 1.5E8 | 1.0023982E7  | 0.0      | 6.00000004E8         | 4.0           | 1                     | 14999999                   |
| o_totalprice    | 1.5E8 | 3.4424096E7  | 0.0      | 1.200000008E9        | 8.0           | 811.73                | 591036.15                  |
| o_orderkey      | 1.5E8 | 1.51621184E8 | 0.0      | 1.200000008E9        | 8.0           | 1                     | 600000000                  |
| o_comment       | 1.5E8 | 1.10204136E8 | 0.0      | 7.275038757500258E9  | 48.50025806   | ' Tiresias about the' | 'zzle? unusual requests w' |
| o_orderpriority | 1.5E8 | 5.0          | 0.0      | 1.2600248124001656E9 | 8.40016536    | '1-URGENT'            | '5-LOW'                    |
+-----------------+-------+--------------+----------+----------------------+---------------+-----------------------+----------------------------+
```

### 信息修改

修改信息支持用户手动修改列统计信息。可以修改指定列的row_count, ndv, num_nulls, min_value, max_value, data_size等信息。
请参考[内表统计信息](../query-acceleration/statistics.md)修改统计信息部分。

```
mysql> ALTER TABLE hive.tpch100.orders MODIFY COLUMN o_orderstatus SET STATS ('row_count'='6001215');
Query OK, 0 rows affected (0.03 sec)

mysql> SHOW COLUMN stats hive.tpch100.orders;
+-----------------+-----------+--------------+----------+----------------------+---------------+-----------------------+----------------------------+
| column_name     | count     | ndv          | num_null | data_size            | avg_size_byte | min                   | max                        |
+-----------------+-----------+--------------+----------+----------------------+---------------+-----------------------+----------------------------+
| o_orderstatus   | 6001215.0 | 0.0          | 0.0      | 0.0                  | 0.0           | 'NULL'                | 'NULL'                     |
| o_clerk         | 1.5E8     | 100836.0     | 0.0      | 2.250000015E9        | 15.0          | 'Clerk#000000001'     | 'Clerk#000100000'          |
| o_orderdate     | 1.5E8     | 2417.0       | 0.0      | 6.00000004E8         | 4.0           | '1992-01-01'          | '1998-08-02'               |
| o_shippriority  | 1.5E8     | 1.0          | 0.0      | 6.00000004E8         | 4.0           | 0                     | 0                          |
| o_custkey       | 1.5E8     | 1.0023982E7  | 0.0      | 6.00000004E8         | 4.0           | 1                     | 14999999                   |
| o_totalprice    | 1.5E8     | 3.4424096E7  | 0.0      | 1.200000008E9        | 8.0           | 811.73                | 591036.15                  |
| o_orderkey      | 1.5E8     | 1.51621184E8 | 0.0      | 1.200000008E9        | 8.0           | 1                     | 600000000                  |
| o_comment       | 1.5E8     | 1.10204136E8 | 0.0      | 7.275038757500258E9  | 48.50025806   | ' Tiresias about the' | 'zzle? unusual requests w' |
| o_orderpriority | 1.5E8     | 5.0          | 0.0      | 1.2600248124001656E9 | 8.40016536    | '1-URGENT'            | '5-LOW'                    |
+-----------------+-----------+--------------+----------+----------------------+---------------+-----------------------+----------------------------+
```

### 信息删除

删除外表统计信息支持用户删除一张表的表行数信息和列统计信息。如果用户指定了删除的列名，则只删除这些列的信息。如果不指定，则删除整张表所有列的统计信息以及表行数信息。
请参考[内表统计信息](../query-acceleration/statistics.md)删除统计信息部分。

- 删除整张表的信息

```
DROP STATS hive.tpch100.orders
```

- 删除表中某几列的信息

```
DROP STATS hive.tpch100.orders (o_orderkey, o_orderdate)
```

## 实现原理
### 统计信息数据来源

优化器（Nereids）通过Cache读取统计信息，cache的数据来源有两个。

第一个是内部的statistics表，statistics表的数据通过用户执行analyze语句收集而来。这一部分的架构与内表相同，用户可以像分析内表一样，对外表执行analyze语句来收集统计信息。

与内表不同的是，外表cache的数据还有第二个来源stats collector。stats collector定义了一些接口，用来从外部数据源获取统计信息。比如目前已经支持的hive metastore和Iceberg两种数据源，这些接口可以获取外部数据源中已有的统计信息。以hive为例，如果用户在hive中执行过analyze操作，那么在Doris中查询的时候，Doris可以直接从hive metastore中加载已有的统计信息到缓存中，包括表的行数、列的最大最小值等。如果外部数据源也没有统计信息，stats connector会根据表中数据文件的大小和表的schema，大致估算一个行数提供给优化器，在这种情况下，列的统计信息是缺失的，可能导致优化器生成比较低效的执行计划。

Stats collector在statistics表中无数据时自动执行，对用户透明，用户无需执行命令或进行设置。

### 缓存的加载

缓存的加载顺序是，首先通过Statistics表加载，如果Statistics表中有信息，说明用户在doris中执行过analyze操作，这样收集上来的统计信息是最准确的，所以我们优先从Statistics表中加载。如果发现Statistics中没有当前所需表的信息，再通过stats collector从外部数据源获取。如果外部数据源也没有，stats collector会估算一个行数。
由于缓存是异步加载的，第一次query的时候可能没法利用到任何统计信息，因为这时候刚刚触发缓存加载。但一般情况下，可以保证第二次查询某张表的时候，优化器可以从缓存中获取到它的统计信息。
