---
{
    "title": "Advanced Use Guide",
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

# Advanced Use Guide

Here we introduce some of Doris's advanced features.

## Table Structural Change

Schema of the table can be modified using the [ALTER TABLE COLUMN](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-COLUMN.md) command, including the following modifications:

* Additional columns
* Delete columns
* Modify column type
* Changing column order

Examples are given below.

Schema of Table 1 is as follows:

```
+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | No   | true  | 10      |       |
| citycode | smallint(6) | No   | true  | N/A     |       |
| username | varchar(32) | No   | true  |         |       |
| pv       | bigint(20)  | No   | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
```

We added a new column of uv, type BIGINT, aggregation type SUM, default value is 0:

```sql
ALTER TABLE table1 ADD COLUMN uv BIGINT SUM DEFAULT '0' after pv;
```

After successful submission, you can view the progress of the job by following commands:

```sql
SHOW ALTER TABLE COLUMN;
```

When the job state is `FINISHED`, the job is completed. The new Schema is in force.

After ALTER TABLE is completed, you can view the latest Schema through `DESC TABLE`.

```sql
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

The following command can be used to cancel the job currently being executed:

```sql
CANCEL ALTER TABLE COLUMN FROM table1;
```

For more help, see `HELP ALTER TABLE`.

## Rollup

Rollup can be understood as a materialized index structure of Table. **materialized** because data is store as a concrete ("materialized") table independently, and **indexing** means that Rollup can adjust column order to increase the hit rate of prefix index, or reduce key column to increase data aggregation.

Use [ALTER TABLE ROLLUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-TABLE-ROLLUP.md) to perform various rollup changes.

Examples are given below.

Schema of Table 1 is as follows:

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

For table1 detailed data, siteid, citycode and username form a set of keys, which aggregate the PV field. If the business side often has the need to see the total amount of PV in the city, it can build a rollup with only citycode and pv.

```sql
ALTER TABLE table1 ADD ROLLUP rollup_city(citycode, pv);
```

After successful submission, you can view the progress of the job by following commands:

```sql
SHOW ALTER TABLE ROLLUP;
```

When the job state is `FINISHED`, the job is completed.

When Rollup is established, you can use `DESC table1 ALL` to view the Rollup information of the table.

```mysql
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

The following command can be used to cancel the job currently being executed:

```mysql
CANCEL ALTER TABLE ROLLUP FROM table1;
```

After Rollup is established, the query does not need to specify Rollup to query. Or specify the original table for query. The program automatically determines whether Rollup should be used. Whether Rollup is hit or not can be viewed by the `EXPLAIN your_sql;`command.

For more help, see `HELP ALTER TABLE`.

## Query of Data Table

### Memory Limitation

To prevent a user's query from consuming too much memory. Queries are controlled in memory. A query task uses no more than 2GB of memory by default on a single BE node.

When users use it, if they find a `Memory limit exceeded` error, they usually exceed the memory limit.

Users should try to optimize their SQL statements when they encounter memory overrun.

If it is found that 2GB memory cannot be satisfied, the memory parameters can be set manually.

Display query memory limits:

```sql
mysql> SHOW VARIABLES LIKE "%mem_limit%";
+---------------+------------+
| Variable_name | Value      |
+---------------+------------+
| exec_mem_limit| 2147483648 |
+---------------+------------+
1 row in set (0.00 sec)
```

The unit of `exec_mem_limit` is byte, and the value of `exec_mem_limit` can be changed by the `SET` command. If changed to 8GB.

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

>* The above modification is session level and is only valid within the current connection session. Disconnecting and reconnecting will change back to the default value.
>* If you need to modify the global variable, you can set it as follows: `SET GLOBAL exec_mem_limit = 8589934592;` When the setup is complete, disconnect the session and log in again, and the parameters will take effect permanently.

### Query Timeout

The current default query time is set to 300 seconds. If a query is not completed within 300 seconds, the query will be cancelled by the Doris system. Users can use this parameter to customize the timeout time of their applications and achieve a blocking mode similar to wait (timeout).

View the current timeout settings:

```sql
mysql> SHOW VARIABLES LIKE "%query_timeout%";
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| QUERY_TIMEOUT | 900   |
+---------------+-------+
1 row in set (0.00 sec)
```

Modify the timeout to 1 minute:

```sql
mysql>  SET query_timeout = 60;
Query OK, 0 rows affected (0.00 sec)
```

>* The current timeout check interval is 5 seconds, so timeouts less than 5 seconds are not very accurate.
>* The above modifications are also session level. Global validity can be modified by `SET GLOBAL`.

### Broadcast/Shuffle Join

By default, the system implements Join by conditionally filtering small tables, broadcasting them to the nodes where the large tables are located, forming a memory Hash table, and then streaming out the data of the large tables Hash Join. However, if the amount of data filtered by small tables cannot be put into memory, Join will not be able to complete at this time. The usual error should be caused by memory overrun first.

If you encounter the above situation, it is recommended to use Shuffle Join explicitly, also known as Partitioned Join. That is, small and large tables are Hash according to Join's key, and then distributed Join. This memory consumption is allocated to all computing nodes in the cluster.

Doris will try to use Broadcast Join first. If small tables are too large to broadcasting, Doris will switch to Shuffle Join automatically. Note that if you use Broadcast Join explicitly in this case, Doris will still switch to Shuffle Join automatically.

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

Shuffle Join:

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

When multiple FE nodes are deployed, users can deploy load balancing layers on top of multiple FEs to achieve high availability of Doris.

Here are some highly available solutions:

**The first**

I retry and load balancing in application layer code. For example, if a connection is found to be dead, it will automatically retry on other connections. Application-level code retry requires the application to configure multiple Doris front-end node addresses.

**Second**

If you use MySQL JDBC connector to connect Doris, you can use jdbc's automatic retry mechanism:

```
jdbc:mysql://[host1][:port1],[host2][:port2][,[host3][:port3]]...[/[database]][?propertyName1=propertyValue1[&propertyName2=propertyValue2]...]
```

**The third**

Applications can connect to and deploy MySQL Proxy on the same machine by configuring MySQL Proxy's Failover and Load Balance functions.

```
https://dev.mysql.com/doc/refman/5.6/en/proxy-users.html
```

