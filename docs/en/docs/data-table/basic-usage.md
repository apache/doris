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


# Use Guide

Doris uses MySQL protocol to communicate. Users can connect to Doris cluster through MySQL client or MySQL JDBC. When selecting the MySQL client version, it is recommended to use the version after 5.1, because user names of more than 16 characters cannot be supported before 5.1. This paper takes MySQL client as an example to show users the basic usage of Doris through a complete process.

## Create Users

### Root User Logon and Password Modification

Doris has built-in root and admin users, and the password is empty by default. 

>Remarks:
>
>The default root and admin users provided by Doris are admin users
>
>The >root user has all the privileges of the cluster by default. Users who have both Grant_priv and Node_priv can grant this permission to other users and have node change permissions, including operations such as adding, deleting, and going offline of FE, BE, and BROKER nodes.
>
>admin user has ADMIN_PRIV and GRANT_PRIV privileges
>
>For specific instructions on permissions, please refer to [Permission Management](../../admin-manual/privilege-ldap/user-privilege)

After starting the Doris program, you can connect to the Doris cluster through root or admin users.
Use the following command to log in to Doris:

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

>` FE_HOST` is the IP address of any FE node. ` 9030 ` is the query_port configuration in fe.conf.

After login, you can modify the root password by following commands

```sql
mysql> SET PASSWORD FOR 'root' = PASSWORD('your_password');
Query OK, 0 rows affected (0.00 sec)
```

> `your_password` is a new password for the `root` user, which can be set at will. It is recommended to set a strong password to increase security, and use the new password to log in the next time you log in.

### Creating New Users

We can create a regular user `test` with the following command:

```sql
mysql> CREATE USER 'test' IDENTIFIED BY 'test_passwd';
Query OK, 0 rows affected (0.00 sec)
```

Follow-up login can be done through the following connection commands.

```sql
[root@doris ~]# mysql -h FE_HOST -P9030 -utest -ptest_passwd
```

> By default, the newly created common user does not have any permissions. Permission grants can be referred to later permission grants.

## Data Table Creation and Data Import

### Create a database

Initially, a database can be created through root or admin users:

```sql
CREATE DATABASE example_db;
```

> All commands can use `HELP` command to see detailed grammar help. For example: `HELP CREATE DATABASE;'`.You can also refer to the official website [SHOW CREATE DATABASE](../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-DATABASE.md) command manual.
>
> If you don't know the full name of the command, you can use "help command a field" for fuzzy query. If you type `HELP CREATE`, you can match commands like `CREATE DATABASE', `CREATE TABLE', `CREATE USER', etc.

After the database is created, you can view the database information through [SHOW DATABASES](../../sql-manual/sql-reference/Show-Statements/SHOW-DATABASES).

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

> Information_schema exists to be compatible with MySQL protocol. In practice, information may not be very accurate. Therefore, information about specific databases is suggested to be obtained by directly querying the corresponding databases.

### Account Authorization

After the example_db is created, the read and write permissions of example_db can be authorized to ordinary accounts, such as test, through the root/admin account. After authorization, the example_db database can be operated by logging in with the test account.

```sql
mysql> GRANT ALL ON example_db TO test;
Query OK, 0 rows affected (0.01 sec)
```

### Formulation

Create a table using the [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) command. More detailed parameters can be seen:`HELP CREATE TABLE;`

First, we need to switch databases using the [USE](../sql-manual/sql-reference/Utility-Statements/USE.md) command:

```sql
mysql> USE example_db;
Database changed
```

Doris supports [composite partition and single partition](./data-partition)  two table building methods. The following takes the aggregation model as an example to demonstrate how to create two partitioned data tables.

#### Single partition

Create a logical table with the name table1. The number of barrels is 10.

The schema of this table is as follows:

* Siteid: Type is INT (4 bytes), default value is 10
* citycode: The type is SMALLINT (2 bytes)
* username: The type is VARCHAR, the maximum length is 32, and the default value is an empty string.
* pv: Type is BIGINT (8 bytes), default value is 0; this is an index column, Doris will aggregate the index column internally, the aggregation method of this column is SUM.

The TABLE statement is as follows:
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

#### Composite partition

Create a logical table named table2.

The schema of this table is as follows:

* event_day: Type DATE, no default
* Siteid: Type is INT (4 bytes), default value is 10
* citycode: The type is SMALLINT (2 bytes)
* username: The type is VARCHAR, the maximum length is 32, and the default value is an empty string.
* pv: Type is BIGINT (8 bytes), default value is 0; this is an index column, Doris will aggregate the index column internally, the aggregation method of this column is SUM.

We use the event_day column as the partition column to create three partitions: p201706, p201707, and p201708.

* p201706: Range [Minimum, 2017-07-01)
* p201707: Scope [2017-07-01, 2017-08-01)
* p201708: Scope [2017-08-01, 2017-09-01)

> Note that the interval is left closed and right open.

Each partition uses siteid to hash buckets, with a bucket count of 10

The TABLE statement is as follows:
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

After the table is built, you can view the information of the table in example_db:

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

> Notes:
>
> 1. By setting replication_num, the above tables are all single-copy tables. Doris recommends that users adopt the default three-copy settings to ensure high availability.
> 2. Composite partition tables can be added or deleted dynamically. See the Partition section in `HELP ALTER TABLE`.
> 3. Data import can import the specified Partition. See `HELP LOAD;`.
> 4. Schema of table can be dynamically modified, See `HELP ALTER TABLE;`.
> 5. Rollup can be added to Table to improve query performance. This section can be referred to the description of Rollup in Advanced Usage Guide.
> 6. The default value of Null property for column is true, which may result in poor scan performance.

### Import data

Doris supports a variety of data import methods. Specifically, you can refer to the [data import](../data-operate/import/load-manual.md) document. Here we use streaming import and Broker import as examples.

#### Flow-in

Streaming import transfers data to Doris via HTTP protocol. It can import local data directly without relying on other systems or components. Detailed grammar help can be found in `HELP STREAM LOAD;'

Example 1: With "table1_20170707" as Label, import table1 tables using the local file table1_data.

```bash
curl --location-trusted -u test:test_passwd -H "label:table1_20170707" -H "column_separator:," -T table1_data http://FE_HOST:8030/api/example_db/table1/_stream_load
```

> 1. FE_HOST is the IP of any FE node and 8030 is http_port in fe.conf.
> 2. You can use the IP of any BE and the webserver_port in be.conf to connect the target left and right for import. For example: `BE_HOST:8040`

The local file `table1_data` takes `,` as the separation between data, and the specific contents are as follows:

```text
1,1,Jim,2
2,1,grace,2
3,2,tom,2
4,3,bush,3
5,3,helen,3
```

Example 2: With "table2_20170707" as Label, import table2 tables using the local file table2_data.

```bash
curl --location-trusted -u test:test -H "label:table2_20170707" -H "column_separator:|" -T table2_data http://127.0.0.1:8030/api/example_db/table2/_stream_load
```

The local file `table2_data'is separated by `|'. The details are as follows:

```
2017-07-03|1|1|jim|2
2017-07-05|2|1|grace|2
2017-07-12|3|2|tom|2
2017-07-15|4|3|bush|3
2017-07-12|5|3|helen|3
```

> Notes:
>
> 1. The recommended file size for streaming import is limited to 10GB. Excessive file size will result in higher cost of retry failure.
> 2. Each batch of imported data needs to take a Label. Label is best a string related to a batch of data for easy reading and management. Doris based on Label guarantees that the same batch of data can be imported only once in a database. Label for failed tasks can be reused.
> 3. Streaming imports are synchronous commands. The successful return of the command indicates that the data has been imported, and the failure of the return indicates that the batch of data has not been imported.

#### Broker Load

Broker imports import data from external storage through deployed Broker processes. For more help, see `HELP BROKER LOAD;`

Example: Import files on HDFS into table1 table with "table1_20170708" as Label

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

Broker imports are asynchronous commands. Successful execution of the above commands only indicates successful submission of tasks. Successful imports need to be checked through `SHOW LOAD;' Such as:

```sql
SHOW LOAD WHERE LABEL = "table1_20170708";
```

In the return result, `FINISHED` in the `State` field indicates that the import was successful.

For more instructions on `SHOW LOAD`, see` HELP SHOW LOAD; `

Asynchronous import tasks can be cancelled before the end:

```sql
CANCEL LOAD WHERE LABEL = "table1_20170708";
```

## Data query

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

A `SELECT * EXCEPT` statement specifies the names of one or more columns to exclude from the result. All matching column names are omitted from the output.

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

**Note**: `SELECT * EXCEPT` does not exclude columns that do not have names.

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

## Table Structure Change

Use the [ALTER TABLE COLUMN](../../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-COLUMN) command to modify the table Schema, including the following changes.

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

Rollup can be understood as a materialized index structure for a Table. **Materialized** because its data is physically stored independently, and **Indexed** in the sense that Rollup can reorder columns to increase the hit rate of prefix indexes, and can reduce key columns to increase the aggregation of data.

Use [ALTER TABLE ROLLUP](../../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-ROLLUP) to perform various changes to Rollup.

The following examples are given

The Schema of the original table1 is as follows:

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

For table1 detailed data is siteid, citycode, username three constitute a set of key, so that the pv field aggregation; if the business side often have to see the total number of city pv needs, you can create a rollup of only citycode, pv.

```sql
ALTER TABLE table1 ADD ROLLUP rollup_city(citycode, pv);
```

After successful submission, you can check the progress of the job with the following command.

```sql
SHOW ALTER TABLE ROLLUP;
```

When the job status is ``FINISHED``, the job is completed.

You can use ``DESC table1 ALL`` to view the rollup information of the table after the rollup is created.

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

You can cancel the currently executing job with the following command:

```sql
CANCEL ALTER TABLE ROLLUP FROM table1;
```

After Rollup is created, the query does not need to specify Rollup for the query. You can still specify the original table for the query. The program will automatically determine if Rollup should be used, and whether Rollup is hit or not can be checked with the ``EXPLAIN your_sql;`` command.

For more help, see `HELP ALTER TABLE`.



## Materialized Views

Materialized views are a space-for-time data analysis acceleration technique, and Doris supports building materialized views on top of base tables. For example, a partial column-based aggregated view can be built on top of a table with a granular data model, allowing for fast querying of both granular and aggregated data.

At the same time, Doris can automatically ensure data consistency between materialized views and base tables, and automatically match the appropriate materialized view at query time, greatly reducing the cost of data maintenance for users and providing a consistent and transparent query acceleration experience for users.

For more information about materialized views, see [Materialized Views](../../advanced/materialized-view)

## Data table queries

### Memory limit

To prevent a user from having a single query that may consume too much memory because it consumes too much memory. The query is memory-controlled, and by default a query task can use no more than 2GB of memory on a single BE node.

If a user finds a `Memory limit exceeded` error while using it, the memory limit is usually exceeded.

When you encounter a memory limit exceeded, you should try to solve it by optimizing your sql statements.

If you find exactly that 2GB of memory cannot be met, you can set the memory parameters manually.

Show query memory limit:

```sql
mysql> SHOW VARIABLES LIKE "%mem_limit%";
+---------------+------------+
| Variable_name | Value      |
+---------------+------------+
| exec_mem_limit| 2147483648 |
+---------------+------------+
1 row in set (0.00 sec)
```

The unit of `exec_mem_limit` is byte, and the value of `exec_mem_limit` can be changed by `SET` command. For example, change it to 8GB.

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

>- The above change is session level and is only valid for the current connection session. Disconnecting and reconnecting will change it back to the default value.
>- If you need to change the global variable, you can set it like this: `SET GLOBAL exec_mem_limit = 8589934592;`. After setting, disconnect and log back in, the parameter will take effect permanently.

### Query Timeout

The current default query timeout is set to a maximum of 300 seconds. If a query does not complete within 300 seconds, the query will be cancelled by the Doris system. You can use this parameter to customize the timeout for your application to achieve a blocking method similar to wait(timeout).

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

Modify the timeout to 1 minute:

```sql
mysql>  SET query_timeout = 60;
Query OK, 0 rows affected (0.00 sec)
```

>- The current timeout check interval is 5 seconds, so a timeout of less than 5 seconds is not too accurate.
>- The above changes are also session level. You can change the global validity by `SET GLOBAL`.

### Broadcast/Shuffle Join

The default way to implement Join is to filter the small table conditionally, broadcast it to each node of the large table to form a memory Hash table, and then stream the data of the large table for Hash Join, but if the filtered data of the small table cannot be put into memory, the Join will not be completed, and the usual error should be the first to cause memory overrun.

In this case, it is recommended to explicitly specify Shuffle Join, also known as Partitioned Join, where both the small and large tables are Hashed according to the key of the join and then perform a distributed join, with the memory consumption being spread across all compute nodes in the cluster.

Doris will automatically attempt a Broadcast Join and switch to a Shuffle Join if the small table is estimated to be too large; note that if a Broadcast Join is explicitly specified at this point, it will enforce Broadcast Join.

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

Please refer to [Load Balancing](../admin-manual/cluster-management/load-balancing) for details on installation, deployment, and usage.

## Data update and deletion

Doris supports deleting imported data in two ways. One way is to delete data by specifying a WHERE condition with a DELETE FROM statement. This method is more general and suitable for less frequent scheduled deletion tasks.

The other deletion method is for the Unique primary key unique model only, where the primary key rows to be deleted are imported by importing the data, and the final physical deletion of the data is performed internally by Doris using the delete tag bit. This deletion method is suitable for deleting data in a real-time manner.

For specific instructions on delete and update operations, see [Data Update](../../data-operate/update-delete/update) documentation.
