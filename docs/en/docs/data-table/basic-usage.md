---
{
    "title": "Guidelines for Basic Use",
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


# User Guide

Doris uses MySQL protocol for communication. Users can connect to Doris clusters through MySQL Client or MySQL JDBC. MySQL Client 5.1 or newer versions are recommended because they support user names of more than 16 characters. This topic walks you through how to use Doris with the example of MySQL Client.

## Create Users

### Root User Login and Change Password

Doris has its built-in root, and the default password is empty. 

>Note:
>
>Doris provides a default root.
>
>The root user has all the privileges about the clusters by default. Users who have both Grant_priv and Node_priv can grant these privileges to other users. Node changing privileges include adding, deleting, and offlining FE, BE, and BROKER nodes.
>
>For more instructions on privileges, please refer to [Privilege Management](../admin-manual/privilege-ldap/user-privilege.md)

After starting the Doris program, root or admin users can connect to Doris clusters. You can use the following command to log in to Doris. After login, you will enter the corresponding MySQL command line interface.

```sql
[root@doris ~]# mysql  -h FE_HOST -P9030 -uroot
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 41
Server version: 5.1.0 Doris version 1.0.0-preview2-b48ee2734

Copyright (c) 2000, 2022, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```

>1. ` FE_HOST` is the IP address of any FE node. ` 9030 ` is the query_port configuration in fe.conf.

After login, you can change the root password by the following command:

```sql
mysql> SET PASSWORD FOR 'root' = PASSWORD('your_password');
Query OK, 0 rows affected (0.00 sec)
```

> `your_password` is a new password for the `root` user, which can be set at will. A strong password is recommended for security. The new password is required in the next login.

### Create New Users

You can create a regular user named  `test` with the following command:

```sql
mysql> CREATE USER 'test' IDENTIFIED BY 'test_passwd';
Query OK, 0 rows affected (0.00 sec)
```

Follow-up logins can be performed with the following connection commands.

```sql
[root@doris ~]# mysql -h FE_HOST -P9030 -utest -ptest_passwd
```

> By default, the newly created regular users do not have any privileges. Privileges can be granted to these users.

## Create Data Table and Import Data

### Create a Database

Initially, root or admin users can create a database by the following command:

```sql
CREATE DATABASE example_db;
```

> You can use the  `HELP` command to check the syntax of all commands. For example, `HELP CREATE DATABASE;`. Or you can refer to the [SHOW CREATE DATABASE](../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-DATABASE.md) command manual.
>
> If you don't know the full name of the command, you can use "HELP + a field of the command" for fuzzy query. For example, if you type in  `HELP CREATE`, you can find commands including `CREATE DATABASE`, `CREATE TABLE`, and `CREATE USER`.
>
> ```
>mysql> HELP CREATE;
>Many help items for your request exist.
>To make a more specific request, please type 'help <item>',
>where <item> is one of the following
>topics:
>   CREATE CATALOG
>   CREATE DATABASE
>   CREATE ENCRYPTKEY
>   CREATE EXTERNAL TABLE
>   CREATE FILE
>   CREATE FUNCTION
>   CREATE INDEX
>   CREATE MATERIALIZED VIEW
>   CREATE POLICY
>   CREATE REPOSITORY
>   CREATE RESOURCE
>   CREATE ROLE
>   CREATE ROUTINE LOAD
>   CREATE SQL BLOCK RULE
>   CREATE SYNC JOB
>   CREATE TABLE
>   CREATE TABLE AS SELECT
>   CREATE TABLE LIKE
>   CREATE USER
>   CREATE VIEW
>   CREATE WORKLOAD GROUP
>   SHOW CREATE CATALOG
>   SHOW CREATE DATABASE
>   SHOW CREATE FUNCTION
>   SHOW CREATE LOAD
>   SHOW CREATE REPOSITORY
>   SHOW CREATE ROUTINE LOAD
>   SHOW CREATE TABLE
> ```

After the database is created, you can view the information about the database via the [SHOW DATABASES](../sql-manual/sql-reference/Show-Statements/SHOW-DATABASES.md) command.

```sql
MySQL> SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| example_db         |
| information_schema |
+--------------------+
2 rows in set (0.00 sec)
```

> `information_schema` exists for compatibility with MySQL protocol, so the information might not be 100% accurate in practice. Therefore, for information about the specific databases, please query the corresponding databases directly.

### Authorize an Account

After  `example_db`  is created, root/admin users can grant read/write privileges of `example_db`  to regular users, such as `test`, using the `GRANT` command. After authorization, user `test` can perform operations on `example_db`.

```sql
mysql> GRANT ALL ON example_db TO test;
Query OK, 0 rows affected (0.01 sec)
```

### Create a Table

You can create a table using the [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) command. For detailed parameters, you can send a  `HELP CREATE TABLE;` command.

Firstly, you need to switch to the target database using the [USE](../sql-manual/sql-reference/Utility-Statements/USE.md) command:

```sql
mysql> USE example_db;
Database changed
```

Doris supports two table creation methods: [compound partitioning and single partitioning](./data-partition.md). The following takes the Aggregate Model as an example to demonstrate how to create tables with these two methods, respectively.

#### Single Partitioning

Create a logical table named `table1`. The bucketing column is the `siteid` column, and the number of buckets is 10.

The table schema is as follows:

* `siteid`:  INT (4 bytes); default value: 10
* `citycode`: SMALLINT (2 bytes)
* `username`: VARCHAR, with a maximum length of 32; default value: empty string
* `pv`: BIGINT (8 bytes); default value: 0; This is a metric column, and Doris will aggregate the metric columns internally. The `pv` column is aggregated by SUM.

The corresponding CREATE TABLE statement is as follows:
```sql
CREATE TABLE table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

#### Compound Partitioning

Create a logical table named `table2`.

The table schema is as follows:

* `event_day`: DATE; no default value
* `siteid`: INT (4 bytes); default value: 10
* `citycode`: SMALLINT (2 bytes)
* `username`: VARCHAR, with a maximum length of 32; default value: empty string
* `pv`: BIGINT (8 bytes); default value: 0; This is a metric column, and Doris will aggregate the metric columns internally. The `pv` column is aggregated by SUM.

Use the `event_day` column as the partitioning column and create 3 partitions: p201706, p201707, and p201708.

* p201706: Range [Minimum, 2017-07-01)
* p201707: Range [2017-07-01, 2017-08-01)
* p201708: Range [2017-08-01, 2017-09-01)

> Note that the intervals are left-closed and right-open.

HASH bucket each partition based on `siteid`. The number of buckets per partition is 10.

The corresponding CREATE TABLE statement is as follows:
```sql
CREATE TABLE table2
(
    event_day DATE,
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(event_day, siteid, citycode, username)
PARTITION BY RANGE(event_day)
(
    PARTITION p201706 VALUES LESS THAN ('2017-07-01'),
    PARTITION p201707 VALUES LESS THAN ('2017-08-01'),
    PARTITION p201708 VALUES LESS THAN ('2017-09-01')
)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
```

After the table is created, you can view the information of the table in `example_db`:

```sql
MySQL> SHOW TABLES;
+----------------------+
| Tables_in_example_db |
+----------------------+
| table1               |
| table2               |
+----------------------+
2 rows in set (0.01 sec)

MySQL> DESC table1;
+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | Yes  | true  | 10      |       |
| citycode | smallint(6) | Yes  | true  | N/A     |       |
| username | varchar(32) | Yes  | true  |         |       |
| pv       | bigint(20)  | Yes  | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
4 rows in set (0.00 sec)

MySQL> DESC table2;
+-----------+-------------+------+-------+---------+-------+
| Field     | Type        | Null | Key   | Default | Extra |
+-----------+-------------+------+-------+---------+-------+
| event_day | date        | Yes  | true  | N/A     |       |
| siteid    | int(11)     | Yes  | true  | 10      |       |
| citycode  | smallint(6) | Yes  | true  | N/A     |       |
| username  | varchar(32) | Yes  | true  |         |       |
| pv        | bigint(20)  | Yes  | false | 0       | SUM   |
+-----------+-------------+------+-------+---------+-------+
5 rows in set (0.00 sec)
```

> Note:
>
> 1. As  `replication_num`  is set to `1`, the above tables are created with only one copy. We recommend that you adopt the default three-copy settings to ensure high availability.
> 2. You can dynamically add or delete partitions of compoundly partitioned tables. See  `HELP ALTER TABLE`.
> 3. You can import data into the specified Partition. See `HELP LOAD;`.
> 4. You can dynamically change the table schema. See `HELP ALTER TABLE;`.
> 5. You can add Rollups to Tables to improve query performance. See the Rollup-related section in "Advanced Usage".
> 6. The value of the column is nullable by default, which may affect query performance.

### Load Data

Doris supports a variety of data loading methods. You can refer to [Data Loading](../data-operate/import/load-manual.md) for more details. The following uses Stream Load and Broker Load as examples.

#### Stream Load 

The Stream Load method transfers data to Doris via HTTP protocol. It can import local data directly without relying on any other systems or components. For the detailed syntax, please see `HELP STREAM LOAD;`.

Example 1: Use "table1_20170707" as the Label, import the local file `table1_data` into `table1`.

```bash
curl --location-trusted -u test:test_passwd -H "label:table1_20170707" -H "column_separator:," -T table1_data http://FE_HOST:8030/api/example_db/table1/_stream_load
```

> 1. FE_HOST is the IP address of any FE node and 8030 is the http_port in fe.conf.
> 2. You can use the IP address of any BE and the webserver_port in be.conf for import. For example: `BE_HOST:8040`.

The local file `table1_data` uses `,` as the separator between data. The details are as follows:

```text
1,1,Jim,2
2,1,grace,2
3,2,tom,2
4,3,bush,3
5,3,helen,3
```

Example 2: Use "table2_20170707" as the Label, import the local file `table2_data` into `table2`.

```bash
curl --location-trusted -u test:test -H "label:table2_20170707" -H "column_separator:|" -T table2_data http://127.0.0.1:8030/api/example_db/table2/_stream_load
```

The local file `table2_data` uses `|` as the separator between data. The details are as follows:

```
2017-07-03|1|1|jim|2
2017-07-05|2|1|grace|2
2017-07-12|3|2|tom|2
2017-07-15|4|3|bush|3
2017-07-12|5|3|helen|3
```

> Note:
>
> 1. The recommended file size for Stream Load is less than 10GB. Excessive file size will result in higher retry cost.
> 2. Each batch of import data should have a Label. Label serves as the unique identifier of the load task, and guarantees that the same batch of data will only be successfully loaded into a database once. For more details, please see [Data Loading and Atomicity](https://doris.apache.org/docs/dev/data-operate/import/import-scenes/load-atomicity/). 
> 3. Stream Load is a synchronous command. The return of the command indicates that the data has been loaded successfully; otherwise the data has not been loaded.

#### Broker Load

The Broker Load method imports externally stored data via deployed Broker processes. For more details, please see `HELP BROKER LOAD;`

Example: Use "table1_20170708" as the Label, import files on HDFS into `table1` .

```sql
LOAD LABEL table1_20170708
(
    DATA INFILE("hdfs://your.namenode.host:port/dir/table1_data")
    INTO TABLE table1
)
WITH BROKER hdfs 
(
    "username"="hdfs_user",
    "password"="hdfs_password"
)
PROPERTIES
(
    "timeout"="3600",
    "max_filter_ratio"="0.1"
);
```

The Broker Load is an asynchronous command. Successful execution of it only indicates successful submission of the task. You can check if the import task has been completed by `SHOW LOAD;` . For example:

```sql
SHOW LOAD WHERE LABEL = "table1_20170708";
```

In the return result, if you find  `FINISHED` in the `State` field, that means the import is successful.

For more instructions on `SHOW LOAD`, see` HELP SHOW LOAD; `.

Asynchronous import tasks can be cancelled before it is completed:

```sql
CANCEL LOAD WHERE LABEL = "table1_20170708";
```

## Query the Data

### Simple Query

Query example::

```sql
MySQL> SELECT * FROM table1 LIMIT 3;
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      2 |        1 | 'grace'  |    2 |
|      5 |        3 | 'helen'  |    3 |
|      3 |        2 | 'tom'    |    2 |
+--------+----------+----------+------+
3 rows in set (0.01 sec)

MySQL> SELECT * FROM table1 ORDER BY citycode;
+--------+----------+----------+------+
| siteid | citycode | username | pv   |
+--------+----------+----------+------+
|      2 |        1 | 'grace'  |    2 |
|      1 |        1 | 'jim'    |    2 |
|      3 |        2 | 'tom'    |    2 |
|      4 |        3 | 'bush'   |    3 |
|      5 |        3 | 'helen'  |    3 |
+--------+----------+----------+------+
5 rows in set (0.01 sec)
```

### SELECT * EXCEPT

The `SELECT * EXCEPT` statement is used to exclude one or more columns from the result. The output will not include any of the specified columns.

```sql
MySQL> SELECT * except (username, citycode) FROM table1;
+--------+------+
| siteid | pv   |
+--------+------+
|      2 |    2 |
|      5 |    3 |
|      3 |    2 |
+--------+------+
3 rows in set (0.01 sec)
```

**Note**: `SELECT * EXCEPT` does not exclude columns that do not have a name.

###  Join Query

Query example::

```sql
MySQL> SELECT SUM(table1.pv) FROM table1 JOIN table2 WHERE table1.siteid = table2.siteid;
+--------------------+
| sum(`table1`.`pv`) |
+--------------------+
|                 12 |
+--------------------+
1 row in set (0.20 sec)
```

### Subquery

Query example::

```sql
MySQL> SELECT SUM(pv) FROM table2 WHERE siteid IN (SELECT siteid FROM table1 WHERE siteid > 2);
+-----------+
| sum(`pv`) |
+-----------+
|         8 |
+-----------+
1 row in set (0.13 sec)
```

## Change Table Schema

Use the [ALTER TABLE COLUMN](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-COLUMN.md) command to modify the table Schema, including the following changes.

- Adding columns
- Deleting columns
- Modify column types
- Changing the order of columns

The following table structure changes are illustrated by using the following example.

The Schema for the original table1 is as follows:

```text
+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | No   | true  | 10      |       |
| citycode | smallint(6) | No   | true  | N/A     |       |
| username | varchar(32) | No   | true  |         |       |
| pv       | bigint(20)  | No   | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
```

We add a new column uv, type BIGINT, aggregation type SUM, default value 0:

```sql
ALTER TABLE table1 ADD COLUMN uv BIGINT SUM DEFAULT '0' after pv;
```

After successful submission, you can check the progress of the job with the following command:

```sql
SHOW ALTER TABLE COLUMN;
```

When the job status is ``FINISHED``, the job is complete. The new Schema has taken effect.

After ALTER TABLE is completed, you can view the latest Schema via ``DESC TABLE``.

```
mysql> DESC table1;
+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | No   | true  | 10      |       |
| citycode | smallint(6) | No   | true  | N/A     |       |
| username | varchar(32) | No   | true  |         |       |
| pv       | bigint(20)  | No   | false | 0       | SUM   |
| uv       | bigint(20)  | No   | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
5 rows in set (0.00 sec)
```

You can cancel the currently executing job with the following command:

```sql
CANCEL ALTER TABLE COLUMN FROM table1;
```

For more help, see ``HELP ALTER TABLE``.

## Rollup

Rollup can be seen as a materialized index structure for a Table, **materialized** in the sense that its data is physically independent in storage, and **indexed** in the sense that Rollup can reorder columns to increase the hit rate of prefix indexes as well as reduce Key columns to increase the aggregation level of data.

You can perform various changes to Rollup using [ALTER TABLE ROLLUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-ROLLUP.md).

The following is an exemplified illustration.

The original schema of `table1` is as follows:

```
+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | No   | true  | 10      |       |
| citycode | smallint(6) | No   | true  | N/A     |       |
| username | varchar(32) | No   | true  |         |       |
| pv       | bigint(20)  | No   | false | 0       | SUM   |
| uv       | bigint(20)  | No   | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
```

For `table1`,  `siteid`, `citycode`, and `username`  constitute a set of Key, based on which the pv fields are aggregated; if you have a frequent need to view the total city pv, you can create a Rollup consisting of only `citycode` and  `pv`:

```sql
ALTER TABLE table1 ADD ROLLUP rollup_city(citycode, pv);
```

After successful submission, you can check the progress of the task with the following command:

```sql
SHOW ALTER TABLE ROLLUP;
```

If the task status is ``FINISHED``, the job is completed.

After the Rollup is created, you can use ``DESC table1 ALL`` to check the information of the Rollup.

```
mysql> desc table1 all;
+-------------+----------+-------------+------+-------+--------+-------+
| IndexName   | Field    | Type        | Null | Key   | Default | Extra |
+-------------+----------+-------------+------+-------+---------+-------+
| table1      | siteid   | int(11)     | No   | true  | 10      |       |
|             | citycode | smallint(6) | No   | true  | N/A     |       |
|             | username | varchar(32) | No   | true  |         |       |
|             | pv       | bigint(20)  | No   | false | 0       | SUM   |
|             | uv       | bigint(20)  | No   | false | 0       | SUM   |
|             |          |             |      |       |         |       |
| rollup_city | citycode | smallint(6) | No   | true  | N/A     |       |
|             | pv       | bigint(20)  | No   | false | 0       | SUM   |
+-------------+----------+-------------+------+-------+---------+-------+
8 rows in set (0.01 sec)
```

You can cancel the currently ongoing task using the following command:

```sql
CANCEL ALTER TABLE ROLLUP FROM table1;
```

With created Rollups, you do not need to specify the Rollup in queries, but only specify the original table for the query. The program will automatically determine if Rollup should be used. You can check whether Rollup is hit or not  using the ``EXPLAIN your_sql;`` command.

For more help, see `HELP ALTER TABLE`.



## Materialized Views

Materialized views are a space-for-time data analysis acceleration technique. Doris supports building materialized views on top of base tables. For example, a partial column-based aggregated view can be built on top of a table with a granular data model, allowing for fast querying of both granular and aggregated data.

Doris can automatically ensure data consistency between materialized views and base tables, and automatically match the appropriate materialized view at query time, greatly reducing the cost of data maintenance for users and providing a consistent and transparent query acceleration experience.

For more information about materialized views, see [Materialized Views](../query-acceleration/materialized-view.md)

## Data Table Queries

### Memory Limit

To prevent excessive memory usage of one single query, Doris imposes memory limit on queries. By default, one query task should consume no more than 2GB of memory on one single BE node.

If you find a `Memory limit exceeded` error, that means the program is trying to allocate more memory than the memory limit. You can solve this by optimizing your SQL statements.

You can change the 2GB memory limit by modifying the memory parameter settings.

Show the memory limit for one query:

```sql
mysql> SHOW VARIABLES LIKE "%mem_limit%";
+---------------+------------+
| Variable_name | Value      |
+---------------+------------+
| exec_mem_limit| 2147483648 |
+---------------+------------+
1 row in set (0.00 sec)
```

 `exec_mem_limit` is measured in byte. You can change the value of `exec_mem_limit` using the `SET` command. For example, you can change it to 8GB as follows:

```sql
mysql> SET exec_mem_limit = 8589934592;
Query OK, 0 rows affected (0.00 sec)
mysql> SHOW VARIABLES LIKE "%mem_limit%";
+---------------+------------+
| Variable_name | Value      |
+---------------+------------+
| exec_mem_limit| 8589934592 |
+---------------+------------+
1 row in set (0.00 sec)
```

>- The above change is executed at the session level and is only valid for the currently connected session. The default memory limit will restore after reconnection.
>- If you need to change the global variable, you can set: `SET GLOBAL exec_mem_limit = 8589934592;`. After this, you disconnect and log back in, and then the new parameter will take effect permanently.

### Query Timeout

The default query timeout is set to 300 seconds. If a query is not completed within 300 seconds, it will be cancelled by the Doris system. You change this parameter and customize the timeout for your application to achieve a blocking method similar to wait(timeout).

View the current timeout settings:

```sql
mysql> SHOW VARIABLES LIKE "%query_timeout%";
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| QUERY_TIMEOUT | 300   |
+---------------+-------+
1 row in set (0.00 sec)
```

Change query timeout to 1 minute:

```sql
mysql>  SET query_timeout = 60;
Query OK, 0 rows affected (0.00 sec)
```

>- The current timeout check interval is 5 seconds, so if you set the query timeout to less than 5 seconds, it might not be executed accurately.
>- The above changes are also performed at the session level. You can change the global variable by `SET GLOBAL`.

### Broadcast/Shuffle Join

The default way to implement Join is to filter the sub table conditionally, broadcast it to each node of the overall table to form a memory Hash table, and then stream read the data of the overall table for Hash Join, but if the filtered data of the sub table cannot be put into memory, the Join will not be completed, and then usually a memory overrun error will occur.

In this case, it is recommended to explicitly specify Shuffle Join, also known as Partitioned Join, where both the sub table and overall table are Hashed according to the Key of the Join and then perform a distributed Join, with the memory consumption being spread across all compute nodes in the cluster.

Doris will automatically attempt a Broadcast Join and switch to a Shuffle Join if the sub table is estimated to be too large; note that if a Broadcast Join is explicitly specified at this point, it will enforce Broadcast Join.

Use Broadcast Join (default):

```sql
mysql> select sum(table1.pv) from table1 join table2 where table1.siteid = 2;
+--------------------+
| sum(`table1`.`pv`) |
+--------------------+
|                 10 |
+--------------------+
1 row in set (0.20 sec)
```

Use Broadcast Join (explicitly specified):

```sql
mysql> select sum(table1.pv) from table1 join [broadcast] table2 where table1.siteid = 2;
+--------------------+
| sum(`table1`.`pv`) |
+--------------------+
|                 10 |
+--------------------+
1 row in set (0.20 sec)
```

Use Shuffle Join:

```sql
mysql> select sum(table1.pv) from table1 join [shuffle] table2 where table1.siteid = 2;
+--------------------+
| sum(`table1`.`pv`) |
+--------------------+
|                 10 |
+--------------------+
1 row in set (0.15 sec)
```

### Query Retry and High Availability

When deploying multiple FE nodes, you can deploy a load balancing layer on top of multiple FEs to achieve high availability of Doris.

Please refer to [Load Balancing](../admin-manual/cluster-management/load-balancing.md) for details on installation, deployment, and usage.

## Update and Delete Data

Doris supports two methods to delete imported data. One is to use the `DELETE FROM` statement and specify the target data by the `WHERE` condition. This method is widely applicable and suitable for less frequent scheduled deletion tasks.

The other method is only used in the Unique Models with a unique primary key. It imports the the primary key rows that are to be deleted, and the final physical deletion of the data is performed internally by Doris using the deletion mark. This method is suitable for real-time deletion of data.

For specific instructions on deleting and updating data, see [Data Update](../data-operate/update-delete/update.md).
