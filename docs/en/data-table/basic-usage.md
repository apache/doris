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


# Guidelines for Basic Use

Doris uses MySQL protocol to communicate. Users can connect to Doris cluster through MySQL client or MySQL JDBC. When selecting the MySQL client version, it is recommended to use the version after 5.1, because user names of more than 16 characters cannot be supported before 5.1. This paper takes MySQL client as an example to show users the basic usage of Doris through a complete process.

## Create Users

### Root User Logon and Password Modification

Doris has built-in root and admin users, and the password is empty by default. After starting the Doris program, you can connect to the Doris cluster through root or admin users.
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

> All commands can use `HELP` command to see detailed grammar help. For example: `HELP CREATE DATABASE;'`.You can also refer to the official website [SHOW CREATE DATABASE](../sql-manual/sql-reference/Show-Statements/SHOW-CREATE-DATABASE.html) command manual.
>
> If you don't know the full name of the command, you can use "help command a field" for fuzzy query. If you type `HELP CREATE`, you can match commands like `CREATE DATABASE', `CREATE TABLE', `CREATE USER', etc.

After the database is created, you can view the database information through [SHOW DATABASES](../sql-manual/sql-reference/Show-Statements/SHOW-DATABASES.html#show-databases).

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

Create a table using the [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) command. More detailed parameters can be seen:`HELP CREATE TABLE;`

First, we need to switch databases using the [USE](../sql-manual/sql-reference/Utility-Statements/USE.html) command:

```sql
mysql> USE example_db;
Database changed
```

Doris supports [composite partition and single partition](data-partition.html#composite partition and single partition)  two table building methods. The following takes the aggregation model as an example to demonstrate how to create two partitioned data tables.

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

Doris supports a variety of data import methods. Specifically, you can refer to the [data import](../data-operate/import/load-manual.html) document. Here we use streaming import and Broker import as examples.

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
