---
{
    "title": "External Table Statistics",
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

# External Table Statistics

The collection method and content of the external table statistics are basically the same as those of the internal table. For more information, please refer to [Internal table statistics](../query-acceleration/statistics.md). Currently supports the collection of Hive, Iceberg and Hudi external tables.

Features that are not currently supported by the external table include

1. Histogram collection is not supported yet
2. Does not support incremental collection and update of partitions
3. Automatic collection (with auto) is not supported for now, users can use periodic collection (with period) instead
4. Sampling collection is not currently supported

The following mainly introduces the examples and implementation principles of the collection of external table statistic information.

## Examples of usage.

Here is an example of collecting external table statistics by executing the analyze command in Doris. Except for the 4 functions that are not supported mentioned above, the rest are the same as the internal table. The following takes the hive.tpch100 database as an example to show how to use it. The tpch100 database contains 8 tables including lineitem, orders, region, etc.

### Collection of statistics

We supports two collection methods for external table: manual once collection and periodic collection.

#### Manual once collection

- Collect the row count of `lineitem` table and the statistics of all columns
```
mysql> ANALYZE TABLE hive.tpch100.lineitem;
+--------------+-------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| Catalog_Name | DB_Name                 | Table_Name | Columns                                                                                                                                                                                       | Job_Id |
+--------------+-------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| hive         | default_cluster:tpch100 | lineitem   | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] | 126039 |
+--------------+-------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
1 row in set (0.03 sec)
```
This operation is performed asynchronously, a collection job will be created in the background, and the progress of the job can be viewed using job_id
```
mysql> SHOW ANALYZE 126039;
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+---------+---------------+
| job_id | catalog_name | db_name                 | tbl_name | col_name                                                                                                                                                                                      | job_type | analysis_type | message | last_exec_time_in_ms | state   | schedule_type |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+---------+---------------+
| 126039 | hive         | default_cluster:tpch100 | lineitem | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] | MANUAL   | FUNDAMENTALS  |         | 2023-07-13 10:33:44  | PENDING | ONCE          |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+---------+---------------+
1 row in set (0.00 sec)
```
And view the task status of each column.
```
mysql> SHOW ANALYZE TASK STATUS 126039;
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+----------------------+----------+
| task_id | col_name                                                                                                                                                                                      | message | last_exec_time_in_ms | state    |
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+----------------------+----------+
| 126040  | l_receiptdate                                                                                                                                                                                 |         | 2023-07-13 10:33:44  | RUNNING  |
| 126041  | l_returnflag                                                                                                                                                                                  |         | 2023-07-13 10:33:44  | RUNNING  |
| 126042  | l_tax                                                                                                                                                                                         |         | 2023-07-13 10:33:44  | RUNNING  |
| 126043  | l_shipmode                                                                                                                                                                                    |         | 2023-07-13 10:33:44  | RUNNING  |
| 126044  | l_suppkey                                                                                                                                                                                     |         | 2023-07-13 10:33:44  | RUNNING  |
| 126045  | l_shipdate                                                                                                                                                                                    |         | 2023-07-13 10:33:44  | RUNNING  |
| 126046  | l_commitdate                                                                                                                                                                                  |         | 2023-07-13 10:33:44  | RUNNING  |
| 126047  | l_partkey                                                                                                                                                                                     |         | 2023-07-13 10:33:44  | RUNNING  |
| 126048  | l_quantity                                                                                                                                                                                    |         | 2023-07-13 10:33:44  | RUNNING  |
| 126049  | l_orderkey                                                                                                                                                                                    |         | 2023-07-13 10:33:44  | RUNNING  |
| 126050  | l_comment                                                                                                                                                                                     |         | 2023-07-13 10:33:44  | RUNNING  |
| 126051  | l_linestatus                                                                                                                                                                                  |         | 2023-07-13 10:33:44  | RUNNING  |
| 126052  | l_extendedprice                                                                                                                                                                               |         | 2023-07-13 10:33:44  | RUNNING  |
| 126053  | l_linenumber                                                                                                                                                                                  |         | 2023-07-13 10:33:44  | RUNNING  |
| 126054  | l_shipinstruct                                                                                                                                                                                |         | 2023-07-13 10:33:44  | RUNNING  |
| 126055  | l_discount                                                                                                                                                                                    |         | 2023-07-13 10:33:44  | RUNNING  |
| 126056  | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] |         | 2023-07-13 10:33:56  | FINISHED |
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+----------------------+----------+
17 rows in set (0.01 sec)
```

- Collect statistics about all tables in the tpch100 database

```
mysql> ANALYZE DATABASE hive.tpch100;
+--------------+---------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| Catalog_Name | DB_Name | Table_Name | Columns                                                                                                                                                                                       | Job_Id |
+--------------+---------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
| hive         | tpch100 | partsupp   | [ps_suppkey,ps_availqty,ps_comment,ps_partkey,ps_supplycost]                                                                                                                                  | 124192 |
| hive         | tpch100 | orders     | [o_orderstatus,o_clerk,o_orderdate,o_shippriority,o_custkey,o_totalprice,o_orderkey,o_comment,o_orderpriority]                                                                                | 124199 |
| hive         | tpch100 | lineitem   | [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct] | 124210 |
| hive         | tpch100 | part       | [p_partkey,p_container,p_name,p_comment,p_brand,p_type,p_retailprice,p_mfgr,p_size]                                                                                                           | 124228 |
| hive         | tpch100 | customer   | [c_custkey,c_phone,c_acctbal,c_mktsegment,c_address,c_nationkey,c_name,c_comment]                                                                                                             | 124239 |
| hive         | tpch100 | supplier   | [s_comment,s_phone,s_nationkey,s_name,s_address,s_acctbal,s_suppkey]                                                                                                                          | 124249 |
| hive         | tpch100 | nation     | [n_comment,n_nationkey,n_regionkey,n_name]                                                                                                                                                    | 124258 |
| hive         | tpch100 | region     | [r_regionkey,r_comment,r_name]                                                                                                                                                                | 124264 |
+--------------+---------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------+
8 rows in set (0.29 sec)
```
This operation will submit the collection jobs of all tables under the tpch100 database in batches, which is also executed asynchronously. A job_id will be created for each table, and the job and task progress of each table can also be viewed through the job_id.

- synchronous collection

You can use `with sync` to collect table or database statistics synchronously. At this time, no background task will be created, and the client will block before the collection is completed, and return until the collection job is completed.
```
mysql> analyze table hive.tpch100.orders with sync;
Query OK, 0 rows affected (33.19 sec)
```
It should be noted that synchronous collection is affected by the query_timeout session variable. If the collection failed because of timeout, you need to increase the variable value and try again. for example
`set query_timeout=3600` (This set timeout interval to 1 hour)

#### periodic collection

Use `with period` to create periodic execution collection job. 

`analyze table hive.tpch100.orders with period 86400;`

This statement creates a periodic collection job, the cycle is 1 day, and the statistical information of the `orders` table is automatically collected and updated every 24 hours. 

### Job management

The method of job management is also the same as that of the internal table, including functions such as viewing jobs, viewing tasks, and deleting jobs. Please refer to the manage job section of [Internal table statistics](../query-acceleration/statistics.md)

- Show all job status

```
mysql> SHOW ANALYZE;
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+----------+---------------+
| job_id | catalog_name | db_name                 | tbl_name | col_name                                                                                                                                                                                      | job_type | analysis_type | message | last_exec_time_in_ms | state    | schedule_type |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+----------+---------------+
| 12152  | hive         | tpch100                 | partsupp | [ps_suppkey,ps_availqty,ps_comment,ps_partkey,ps_supplycost]                                                                                                                                  | MANUAL   | FUNDAMENTALS  |         | 2023-07-11 15:57:16  | FINISHED | ONCE          |
| 12159  | hive         | tpch100                 | orders   | [o_orderstatus,o_clerk,o_orderdate,o_shippriority,o_custkey,o_totalprice,o_orderkey,o_comment,o_orderpriority]                                                                                | MANUAL   | FUNDAMENTALS  |         | 2023-07-11 15:57:24  | FINISHED | ONCE          |
+--------+--------------+-------------------------+----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+---------------+---------+----------------------+----------+---------------+
```

- Show all tasks status of a job

```
mysql> SHOW ANALYZE TASK STATUS 12152;
+---------+--------------------------------------------------------------+---------+----------------------+----------+
| task_id | col_name                                                     | message | last_exec_time_in_ms | state    |
+---------+--------------------------------------------------------------+---------+----------------------+----------+
| 12153   | ps_availqty                                                  |         | 2023-07-11 15:56:58  | FINISHED |
| 12154   | ps_suppkey                                                   |         | 2023-07-11 15:56:57  | FINISHED |
| 12155   | ps_comment                                                   |         | 2023-07-11 15:57:16  | FINISHED |
| 12156   | ps_supplycost                                                |         | 2023-07-11 15:56:57  | FINISHED |
| 12157   | ps_partkey                                                   |         | 2023-07-11 15:56:58  | FINISHED |
| 12158   | [ps_suppkey,ps_availqty,ps_comment,ps_partkey,ps_supplycost] |         | 2023-07-11 15:56:57  | FINISHED |
+---------+--------------------------------------------------------------+---------+----------------------+----------+
```

- Terminate unfinished jobs

```
KILL ANALYZE [job_id]
```

- Delete periodic collection job

```
DROP ANALYZE JOB [JOB_ID]
```

### Show statistics

Show statistics includes show table statistics (number of rows) and column statistics. Please refer to View statistics in [Internal Table Statistics](../query-acceleration/statistics.md)

#### Table statistics

```
mysql> SHOW TABLE STATS hive.tpch100.orders;
+-----------+---------------------+---------------------+
| row_count | update_time         | last_analyze_time   |
+-----------+---------------------+---------------------+
| 150000000 | 2023-07-11 23:01:49 | 2023-07-11 23:01:44 |
+-----------+---------------------+---------------------+
```

#### Column statistics
```
SHOW COLUMN [cached] stats hive.tpch100.orders;
```

View the column statistics of a table. If the cached parameter is specified, the column information of the specified table that has been loaded into the cache is displayed.

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

### Modify the statistics

Modify statistics supports users to manually modify column statistics. You can modify the row_count, ndv, num_nulls, min_value, max_value, data_size and other information of the specified column.

Please refer to Modify the statistics in [Internal Table Statistics](../query-acceleration/statistics.md)

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

### Delete stattistics

Deleting table statistics supports users to delete table row count and column statistics of a table. If the user specifies the column names to be deleted, only the statistics of these columns will be deleted. If not specified, the statistical information of all columns of the entire table and the number of rows of the table will be deleted.

Please refer to Delete statistics in [Internal Table Statistics](../query-acceleration/statistics.md)

- Delete statistics of the entire table

```
DROP STATS hive.tpch100.orders
```

- Delete the statistics of certain columns in the table

```
DROP STATS hive.tpch100.orders (o_orderkey, o_orderdate)
```

## Implementation principle
### Statistics Data Sources

The optimizer (Nereids) reads statistical information through the cache, and there are two data sources for the cache to load data from.

The first data source is the internal statistics table, and the data in the statistics table is collected by the user to execute analyze statement. The structure of this part is the same as that of the internal table. Users can execute the analyze statement on the external table to collect statistical information just like analyzing the internal tables.

Different from the internal table, the statistic cache for external table has a second data source, the stats collector. The stats collector defines some interfaces for obtaining statistical information from external catalog. Hive metastore and Iceberg metadata. These interfaces can obtain existing statistical information in external catalog. Take hive as an example. If the user has performed the analyze operation in hive, then when querying in Doris, Doris can directly load the existing statistical information from the hive metastore into the cache, including the number of rows in the table, the maximum and minimum values of columns, etc. If the external data source does not have statistical information, the stats connector will roughly estimate row count based on the size of the data file and the schema of the table. In this case, the column statistics are missing, which may cause the optimizer to generate a relatively inefficient execution plan.

The Stats collector is automatically executed when there is no data in the statistics table, and it is transparent to the user, user does not need to execute commands or make any settings.

### Cache loading

The loading sequence of the cache is firstly loaded through the Statistics table. If there is information in the Statistics table, it means that the user has performed analyze operation in doris. The collected statistical information through analyze is the most accurate, so we prioritize loading from the Statistics table. If you find that there is no information about the currently required table in Statistics, Doris will try to obtain it from an external data source through stats collector. If the external data source does not have column statistics either, the stats collector will estimate a row count based on file size and table schema.

Since the cache is loaded asynchronously, the statistical information may not be available for the first query, because the cache loading has just been triggered at this time. But in general, it can be guaranteed that when a table is queried for the second time, the optimizer can obtain its statistical information from the cache.
