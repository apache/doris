---
{
"title": "Statistics",
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

# Statistics

## Introduction to statistics information

In SQL databases, the quality of the query optimizer has a significant impact on system performance. The optimizer needs to estimate the query cost according to the statistics information, especially in the equal-value query scenario, it is very important to estimate the cardinality accurately, which can help the optimizer to select the optimal query plan, thereby improving the query performance.

When executing a query, an insufficiently optimized execution plan and an optimized execution plan can result in a large difference in execution time, which can be several times greater. Therefore, it is very important for the SQL query optimizer to collect and analyze statistics so that the optimizer can accurately evaluate the cost of different execution plans and select the best one.

The Doris query optimizer uses statistics to determine the most efficient execution plan for a query. Statistics maintained by Doris include table-level statistics and column-level statistics.

Table Statistics:

| Information         | Description                                                        |
| :------------------ | :----------------------------------------------------------------- |
| `row_count`         | Number of rows in the table                                        |
| `data_size`         | Table size (in bytes)                                              |
| `update_rows`       | The number of rows updated after collecting statistics information |
| `healthy`           | The health of the table                                            |
| `update_time`       | The time of the latest update                                      |
| `last_analyze_time` | The time when the last statistics information was collected        |

> Table Health: Indicates the health of the table statistics. When it `update_rows` is greater than or equal to `row_count`, the health degree is 0; when it `update_rows` is less than `row_count`, the health degree is `100 * (1 - update_rows/ row_count)`.

Column Statistics:

| Information     | Description                           |
| :-------------- | :------------------------------------ |
| `row_count`     | Total number of rows for the column   |
| `data_size`     | Total degree of the column in bytes   |
| `avg_size_byte` | Average degree of the column in bytes |
| `ndv`           | Column num distinct value             |
| `min`           | Column Minimum                        |
| `max`           | Column Max Value                      |
| `null_count`    | Number of columns null                |

## Collect statistics

### Manual collection

The user triggers a manual collection job through a statement `ANALYZE` to collect statistics for the specified table or column based on the supplied parameters.

Column statistics collection syntax:

```SQL
ANALYZE < TABLE | DATABASE table_name | db_name >
    [ PARTITIONS (partition_name [, ...]) ]
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH INCREMENTAL ] [ WITH SAMPLE PERCENT | ROWS ] [ WITH PERIOD ]]
    [ PROPERTIES ("key" = "value", ...) ];
```

Explanation:

- Table_name: The target table for the specified. It can be a `db_name.table_name` form.
- partition_name: The specified target partitions（for hive external table only）。Must be partitions exist in `table_name`. Multiple partition names are separated by commas. e.g. (nation=US/city=Washington)
- Column_name: The specified target column. Must be `table_name` a column that exists in. Multiple column names are separated by commas.
- Sync: Synchronizes the collection of statistics. Return after collection. If not specified, it will be executed asynchronously and the job ID will be returned.
- Incremental: Incrementally gather statistics.
- Period: Collect statistics periodically. The unit is seconds, and when specified, the appropriate statistics are collected periodically.
- Sample percent | rows: Sample collection statistics. You can specify a sampling ratio or the number of rows to sample.

- Properties: used to configure statistics job. Currently, only the following configuration items are supported
  - `"sync" = "true"`: Equivalent `with sync`
  - `"incremental" = "true"`: Equivalent `with incremental`
  - `"sample.percent" = "50"`: Equivalent `with percent 50`
  - `"sample.rows" = "1000"`: Equivalent `with rows 1000`
  - `"num.buckets" = "10"`: Equivalent `with buckets 10`
  - `"period.seconds" = "300"`: Equivalent `with period 300`

Next, we will use a table `stats_test.example_tbl` as an example to explain how to collect statistics. `stats_test.example_tbl` The structure is as follows:

| Column Name     | Type        | AggregationType | Comment                 |
| --------------- | ----------- | --------------- | ----------------------- |
| user_id         | LARGEINT    |                 | User ID                 |
| imp_date        | DATEV2      |                 | Data import date        |
| city            | VARCHAR(20) |                 | User city               |
| age             | SMALLINT    |                 | User age                |
| sex             | TINYINT     |                 | User gender             |
| last_visit_date | DATETIME    | REPLACE         | User last visit time    |
| cost            | BIGINT      | SUM             | User total cost         |
| max_dwell_time  | INT         | MAX             | User maximum dwell time |
| min_dwell_time  | INT         | MIN             | User minimum dwell time |

Connect Doris:

````Bash
mysql -uroot -P9030 -h192.168.xxx.xxx```

Create a data table:

```SQL
mysql> CREATE DATABASE IF NOT EXISTS stats_test;

mysql> CREATE TABLE IF NOT EXISTS stats_test.example_tbl (
        `user_id` LARGEINT NOT NULL,        `date` DATEV2 NOT NULL,        `city` VARCHAR(20),        `age` SMALLINT,        `sex` TINYINT,        `last_visit_date` DATETIME REPLACE,        `cost` BIGINT SUM,        `max_dwell_time` INT MAX,        `min_dwell_time` INT MIN    ) ENGINE=OLAP    AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)    PARTITION BY LIST(`date`)    (        PARTITION `p_201701` VALUES IN ("2017-10-01"),        PARTITION `p_201702` VALUES IN ("2017-10-02"),        PARTITION `p_201703` VALUES IN ("2017-10-03")    )    DISTRIBUTED BY HASH(`user_id`) BUCKETS 1    PROPERTIES (        "replication_num" = "1"    );
````

Import data:

```SQL
mysql> INSERT INTO stats_test.example_tbl (`user_id`, `date`, `city`, `age`,
                                    `sex`, `last_visit_date`, `cost`,                                    `max_dwell_time`, `min_dwell_time`)    VALUES (10000, "2017-10-01", "Beijing", 20, 0, "2017-10-01 07:00:00", 15, 2, 2),        (10000, "2017-10-01", "Beijing", 20, 0, "2017-10-01 06:00:00", 20, 10, 10),        (10001, "2017-10-01", "Beijing", 30, 1, "2017-10-01 17:05:45", 2, 22, 22),        (10002, "2017-10-02", "Shanghai", 20, 1, "2017-10-02 12:59:12", 200, 5, 5),        (10003, "2017-10-02", "Guangzhou", 32, 0, "2017-10-02 11:20:00", 30, 11, 11),        (10004, "2017-10-01", "Shenzhen", 35, 0, "2017-10-01 10:00:15", 100, 3, 3),        (10004, "2017-10-03", "Shenzhen", 35, 0, "2017-10-03 10:20:22", 11, 6, 6);
```

To view data results:

```SQL
mysql> SELECT * FROM stats_test.example_tbl;
+---------+------------+-----------+------+------+---------------------+------+----------------+----------------+
| user_id | date       | city      | age  | sex  | last_visit_date     | cost | max_dwell_time | min_dwell_time |
+---------+------------+-----------+------+------+---------------------+------+----------------+----------------+
| 10004   | 2017-10-03 | Shenzhen  |   35 |    0 | 2017-10-03 10:20:22 |   11 |              6 |              6 |
| 10000   | 2017-10-01 | Beijing   |   20 |    0 | 2017-10-01 06:00:00 |   35 |             10 |              2 |
| 10001   | 2017-10-01 | Beijing   |   30 |    1 | 2017-10-01 17:05:45 |    2 |             22 |             22 |
| 10004   | 2017-10-01 | Shenzhen  |   35 |    0 | 2017-10-01 10:00:15 |  100 |              3 |              3 |
| 10002   | 2017-10-02 | Shanghai  |   20 |    1 | 2017-10-02 12:59:12 |  200 |              5 |              5 |
| 10003   | 2017-10-02 | Guangzhou |   32 |    0 | 2017-10-02 11:20:00 |   30 |             11 |             11 |
+---------+------------+-----------+------+------+---------------------+------+----------------+----------------+
```

#### Full collection

##### Collect column statistic

Column statistics mainly include the number of rows, the maximum value, the minimum value, and the number of NULL values of a column, which are collected through `ANALYZE TABLE` statements.

When executing SQL statements, the optimizer will, in most cases, only use statistics for some of the columns (for example, `WHERE` the columns that appear in the, `JOIN`, `ORDER BY`, `GROUP BY` clauses). If a table has many columns, collecting statistics for all columns can be expensive. To reduce overhead, you can collect statistics for specific columns only for use by the optimizer.

Example:

- Collect `example_tbl` statistics for all columns of a table, using the following syntax:

```SQL
mysql> ANALYZE TABLE stats_test.example_tbl;
+--------+
| job_id |
+--------+
| 51730  |
+--------+
```

- Collect `example_tbl` statistics for table `city` `age` `sex` columns, using the following syntax:

```SQL
mysql> ANALYZE TABLE stats_test.example_tbl(city, age, sex);
+--------+
| job_id |
+--------+
| 51808  |
+--------+
```

#### Incremental collection

For partitioned tables, incremental collection can be used to improve the speed of statistics collection if partitions are added or deleted after full collection.

When using incremental collection, the system automatically checks for new or deleted partitions. There are three situations:

- For newly added partitions, the statistics of the newly added partitions are collected and merged/summarized with the historical statistics.
- Refresh historical statistics for deleted partitions.
- No new/deleted partition. Do not do anything.

Incremental collection is appropriate for tables with monotonic non-decreasing columns such as time columns as partitions, or tables where historical partition data is not updated.

Notice：

- When using incremental collection, you must ensure that the statistics information of table inventory is available (that is, other historical partition data does not change). Otherwise, the statistics information will be inaccurate.

Example:

- Incrementally collect `example_tbl` statistics for a table, using the following syntax:

```SQL
-- use with incremental
mysql> ANALYZE TABLE stats_test.example_tbl WITH INCREMENTAL;
+--------+
| job_id |
+--------+
| 51910  |
+--------+

-- configure incremental
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("incremental" = "true");
+--------+
| job_id |
+--------+
| 51910  |
+--------+
```

- Incrementally collect `example_tbl` statistics for table `city` `age` `sex` columns, using the following syntax:

```SQL
mysql> ANALYZE TABLE stats_test.example_tbl(city, age, sex) WITH INCREMENTAL;
+--------+
| job_id |
+--------+
| 51988  |
+--------+
```

#### Sampling collection

When the amount of table data is large, the system may take time to collect statistics. You can use sampling collection to speed up the collection of statistics. Specify the proportion of sampling or the number of rows to be sampled according to the actual situation.

Example:

- Sampling collects `example_tbl` statistics from a table, using the following syntax:

```SQL
-- use with sample rows
mysql> ANALYZE TABLE stats_test.example_tbl WITH SAMPLE ROWS 5;
+--------+
| job_id |
+--------+
| 52120  |
+--------+

-- use with sample percent
mysql> ANALYZE TABLE stats_test.example_tbl WITH SAMPLE PERCENT 50;
+--------+
| job_id |
+--------+
| 52201  |
+--------+

-- configure sample.row
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("sample.rows" = "5");
+--------+
| job_id |
+--------+
| 52279  |
+--------+

-- configure sample.percent
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("sample.percent" = "50");
+--------+
| job_id |
+--------+
| 52282  |
+--------+
```

#### Synchronous collection

Generally, after executing `ANALYZE` the statement, the system will start an asynchronous job to collect statistics and return the statistics job ID immediately. If you want to wait for the statistics collection to finish and return, you can use synchronous collection.

Example:

- Sampling collects `example_tbl` statistics from a table, using the following syntax:

```SQL
-- use with sync
mysql> ANALYZE TABLE stats_test.example_tbl WITH SYNC;

-- configure sync
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("sync" = "true");
```

### Automatic collection

Automatic collection means that the system will automatically generate a job to collect statistics when the user specifies `PERIOD` `AUTO` keywords or performs related configuration when executing `ANALYZE` a statement.

#### Periodic collection

Periodic collection means that the corresponding statistics of a table are re-collected at a certain time interval.

Example:

- Collect `example_tbl` statistics for a table periodically (every other day), using the following syntax:

```SQL
-- use with period
mysql> ANALYZE TABLE stats_test.example_tbl WITH PERIOD 86400;
+--------+
| job_id |
+--------+
| 52409  |
+--------+

-- configure period.seconds
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("period.seconds" = "86400");
+--------+
| job_id |
+--------+
| 52535  |
+--------+
```

### Manage job

#### View statistics job

Collect information for the job by `SHOW ANALYZE` viewing the statistics.

The syntax is as follows:

```SQL
SHOW ANALYZE < table_name | job_id >
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

Explanation:

- Table_name: The table name. After it is specified, the statistics job information corresponding to the table can be viewed. It can be a `db_name.table_name` form. Return all statistics job information if not specified.
- Job_ID: The statistics job ID `ANALYZE`. The value returned when the asynchronous collection of statistics is performed. Return all statistics job information if not specified.

Currently `SHOW ANALYZE`, 11 columns are output, as follows:

| Column Name            | Description         |
| :--------------------- | :------------------ |
| `job_id`               | statistics job ID   |
| `catalog_name`         | Catalog name        |
| `db_name`              | Database name       |
| `tbl_name`             | Variable name       |
| `col_name`             | Column name         |
| `job_type`             | job type            |
| `analysis_type`        | statistics type     |
| `message`              | job information     |
| `last_exec_time_in_ms` | Last execution time |
| `state`                | job state           |
| `schedule_type`        | Scheduling method   |

> In the system, the statistics job contains multiple subtasks, each of which collects a separate column of statistics.

Example:

- View statistics job information with ID `20038`, using the following syntax:

```SQL
mysql> SHOW ANALYZE 20038 
+--------+--------------+----------------------+----------+-----------------------+----------+---------------+---------+----------------------+----------+---------------+
| job_id | catalog_name | db_name              | tbl_name | col_name              | job_type | analysis_type | message | last_exec_time_in_ms | state    | schedule_type |
+--------+--------------+----------------------+----------+-----------------------+----------+---------------+---------+----------------------+----------+---------------+
| 20038  | internal     | default_cluster:test | t3       | [col4,col2,col3,col1] | MANUAL   | FUNDAMENTALS  |         | 2023-06-01 17:22:15  | FINISHED | ONCE          |
+--------+--------------+----------------------+----------+-----------------------+----------+---------------+---------+----------------------+----------+---------------+

```

```
mysql> show analyze task status  20038 ;
+---------+----------+---------+----------------------+----------+
| task_id | col_name | message | last_exec_time_in_ms | state    |
+---------+----------+---------+----------------------+----------+
| 20039   | col4     |         | 2023-06-01 17:22:15  | FINISHED |
| 20040   | col2     |         | 2023-06-01 17:22:15  | FINISHED |
| 20041   | col3     |         | 2023-06-01 17:22:15  | FINISHED |
| 20042   | col1     |         | 2023-06-01 17:22:15  | FINISHED |
+---------+----------+---------+----------------------+----------+

```

- View all statistics job information, and return the first 3 pieces of information in descending order of the last completion time, using the following syntax:

```SQL
mysql> SHOW ANALYZE WHERE state = "FINISHED" ORDER BY last_exec_time_in_ms DESC LIMIT 3;
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
| job_id | catalog_name | db_name                    | tbl_name    | col_name        | job_type | analysis_type | message | last_exec_time_in_ms | state    | schedule_type |
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
| 68603  | internal     | default_cluster:stats_test | example_tbl | age             | MANUAL   | COLUMN        |         | 2023-05-05 17:53:27  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | sex             | MANUAL   | COLUMN        |         | 2023-05-05 17:53:26  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | last_visit_date | MANUAL   | COLUMN        |         | 2023-05-05 17:53:26  | FINISHED | ONCE          |
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
```

#### Terminate the statistics job

To `KILL ANALYZE` terminate a running statistics job.

The syntax is as follows:

```SQL
KILL ANALYZE job_id;
```

Explanation:

- Job_ID: Statistics job ID. The value returned when an asynchronous collection of statistics is performed `ANALYZE`, which can also be obtained by a `SHOW ANALYZE` statement.

Example:

- Stop the statistics job whose ID is the 52357.

```SQL
mysql> KILL ANALYZE 52357;
```

## View statistics

### Table statistics

> Temporarily unavailable.

To `SHOW TABLE STATS` view information such as the total number of rows in the table and the health of the statistics.

The syntax is as follows:

```SQL
SHOW TABLE STATS table_name [ PARTITION (partition_name) ];
```

Explanation:

- Table_name: The table to which the data is imported. It can be a `db_name.table_name` form.
- Partition_name: The specified target partition. Must be `table_name` a partition that exists in. Only one partition can be specified.

Currently `SHOW TABLE STATS`, 6 columns are output, as follows:

| Column Name       | Description                                         |
| :---------------- | :-------------------------------------------------- |
| row_count         | Number of rows                                      |
| update_rows       | Number of rows updated                              |
| data_size         | Data size. Unit: bytes                              |
| healthy           | Health                                              |
| update_time       | Update time                                         |
| last_analyze_time | Time when statistics information was last collected |

Example:

- To view `example_tbl` statistics for a table, use the following syntax:

```SQL
mysql> SHOW TABLE STATS stats_test.example_tbl;
+-----------+-------------+---------+-----------+---------------------+---------------------+
| row_count | update_rows | healthy | data_size | update_time         | last_analyze_time   |
+-----------+-------------+---------+-----------+---------------------+---------------------+
| 8         | 0           | 100     | 6999      | 2023-04-08 15:40:47 | 2023-04-08 17:43:28 |
+-----------+-------------+---------+-----------+---------------------+---------------------+
```

- To view `example_tbl` statistics for a table `p_201701` partition, use the following syntax:

```SQL
mysql> SHOW TABLE STATS stats_test.example_tbl PARTITION (p_201701);
+-----------+-------------+---------+-----------+---------------------+---------------------+
| row_count | update_rows | healthy | data_size | update_time         | last_analyze_time   |
+-----------+-------------+---------+-----------+---------------------+---------------------+
| 4         | 0           | 100     | 2805      | 2023-04-08 11:48:02 | 2023-04-08 17:43:27 |
+-----------+-------------+---------+-----------+---------------------+---------------------+
```

### View Column Statistics

`SHOW COLUMN STATS` To view information such as the number of different values and `NULL` the number of columns.

The syntax is as follows:

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ] [ PARTITION (partition_name) ];
```

Explanation:

- cached: Cached means to show statistics in current FE memory cache.
- Table_name: The target table for collecting statistics. It can be a `db_name.table_name` form.
- Column_name: Specified destination column. `table_name` Must be a column that exists in. Multiple column names are separated by commas.
- Partition_name: The specified target partition `table_name` must exist in. Only one partition can be specified.

Currently `SHOW COLUMN STATS`, 10 columns are output, as follows:

| Column Name     | Explain                               |
| :-------------- | :------------------------------------ |
| `column_name`   | Column name                           |
| `count`         | Total number of rows for the column   |
| `ndv`           | Number of distinct values             |
| `num_null`      | The number of null values             |
| `data_size`     | Total degree of the column in bytes   |
| `avg_size_byte` | Average degree of the column in bytes |
| `min`           | Column Minimum                        |
| `max`           | Column Max Value                      |

Example:

- To view `example_tbl` statistics for all columns of a table, use the following syntax:

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl;
+-----------------+-------+------+----------+-------------------+-------------------+-----------------------+-----------------------+
| column_name     | count | ndv  | num_null | data_size         | avg_size_byte     | min                   | max                   |
+-----------------+-------+------+----------+-------------------+-------------------+-----------------------+-----------------------+
| date            | 6.0   | 3.0  | 0.0      | 28.0              | 4.0               | '2017-10-01'          | '2017-10-03'          |
| cost            | 6.0   | 6.0  | 0.0      | 56.0              | 8.0               | 2                     | 200                   |
| min_dwell_time  | 6.0   | 6.0  | 0.0      | 28.0              | 4.0               | 2                     | 22                    |
| city            | 6.0   | 4.0  | 0.0      | 54.0              | 7.0               | 'Beijing'             | 'Shenzhen'            |
| user_id         | 6.0   | 5.0  | 0.0      | 112.0             | 16.0              | 10000                 | 10004                 |
| sex             | 6.0   | 2.0  | 0.0      | 7.0               | 1.0               | 0                     | 1                     |
| max_dwell_time  | 6.0   | 6.0  | 0.0      | 28.0              | 4.0               | 3                     | 22                    |
| last_visit_date | 6.0   | 6.0  | 0.0      | 112.0             | 16.0              | '2017-10-01 06:00:00' | '2017-10-03 10:20:22' |
| age             | 6.0   | 4.0  | 0.0      | 14.0              | 2.0               | 20                    | 35                    |
+-----------------+-------+------+----------+-------------------+-------------------+-----------------------+-----------------------+
```

- To view `example_tbl` statistics for a table `p_201701` partition, use the following syntax:

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl PARTITION (p_201701);
+-----------------+-------+------+----------+--------------------+-------------------+-----------------------+-----------------------+
| column_name     | count | ndv  | num_null | data_size          | avg_size_byte     | min                   | max                   |
+-----------------+-------+------+----------+--------------------+-------------------+-----------------------+-----------------------+
| date            | 3.0   | 1.0  | 0.0      | 16.0               | 4.0               | '2017-10-01'          | '2017-10-01'          |
| cost            | 3.0   | 3.0  | 0.0      | 32.0               | 8.0               | 2                     | 100                   |
| min_dwell_time  | 3.0   | 3.0  | 0.0      | 16.0               | 4.0               | 2                     | 22                    |
| city            | 3.0   | 2.0  | 0.0      | 29.0               | 7.0               | 'Beijing'             | 'Shenzhen'            |
| user_id         | 3.0   | 3.0  | 0.0      | 64.0               | 16.0              | 10000                 | 10004                 |
| sex             | 3.0   | 2.0  | 0.0      | 4.0                | 1.0               | 0                     | 1                     |
| max_dwell_time  | 3.0   | 3.0  | 0.0      | 16.0               | 4.0               | 3                     | 22                    |
| last_visit_date | 3.0   | 3.0  | 0.0      | 64.0               | 16.0              | '2017-10-01 06:00:00' | '2017-10-01 17:05:45' |
| age             | 3.0   | 3.0  | 0.0      | 8.0                | 2.0               | 20                    | 35                    |
+-----------------+-------+------+----------+--------------------+-------------------+-----------------------+-----------------------+
```

- To view `example_tbl` statistics for a table `city` `age` `sex` column, use the following syntax:

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl(city, age, sex);
+-------------+-------+------+----------+-------------------+-------------------+-----------+------------+
| column_name | count | ndv  | num_null | data_size         | avg_size_byte     | min       | max        |
+-------------+-------+------+----------+-------------------+-------------------+-----------+------------+
| city        | 6.0   | 4.0  | 0.0      | 54.0              | 7.0               | 'Beijing' | 'Shenzhen' |
| sex         | 6.0   | 2.0  | 0.0      | 7.0               | 1.0               | 0         | 1          |
| age         | 6.0   | 4.0  | 0.0      | 14.0              | 2.0               | 20        | 35         |
+-------------+-------+------+----------+-------------------+-------------------+-----------+------------+
```

- To view `example_tbl` statistics for a table `p_201701` partition `city` `age` `sex` column, use the following syntax:

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl(city, age, sex) PARTITION (p_201701);
+-------------+-------+------+----------+--------------------+-------------------+-----------+------------+
| column_name | count | ndv  | num_null | data_size          | avg_size_byte     | min       | max        |
+-------------+-------+------+----------+--------------------+-------------------+-----------+------------+
| city        | 3.0   | 2.0  | 0.0      | 29.0               | 7.0               | 'Beijing' | 'Shenzhen' |
| sex         | 3.0   | 2.0  | 0.0      | 4.0                | 1.0               | 0         | 1          |
| age         | 3.0   | 3.0  | 0.0      | 8.0                | 2.0               | 20        | 35         |
+-------------+-------+------+----------+--------------------+-------------------+-----------+------------+
```

## Modify the statistics

Users can modify the statistics information through statements `ALTER`, and modify the corresponding statistics information of the column according to the provided parameters.

```SQL
ALTER TABLE table_name MODIFY COLUMN column_name SET STATS ('stat_name' = 'stat_value', ...) [ PARTITION (partition_name) ];
```

Explanation:

- Table_name: The table to which the statistics are dropped. It can be a `db_name.table_name` form.
- Column_name: Specified target column. `table_name` Must be a column that exists in. Statistics can only be modified one column at a time.
- Stat _ name and stat _ value: The corresponding stat name and the value of the stat info. Multiple stats are comma separated. Statistics that can be modified include `row_count`, `ndv`, `num_nulls` `min_value` `max_value`, and `data_size`.
- Partition_name: specifies the target partition. Must be a partition existing in `table_name`. Multiple partitions are separated by commas.

Example:

- To modify `example_tbl` table `age` column `row_count` statistics, use the following syntax:

```SQL
mysql> ALTER TABLE stats_test.example_tbl MODIFY COLUMN age SET STATS ('row_count'='6001215');
mysql> SHOW COLUMN STATS stats_test.example_tbl(age);
+-------------+-----------+------+----------+-----------+---------------+------+------+
| column_name | count     | ndv  | num_null | data_size | avg_size_byte | min  | max  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
| age         | 6001215.0 | 0.0  | 0.0      | 0.0       | 0.0           | N/A  | N/A  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
```

- Modify `example_tbl` table `age` columns `row_count`, `num_nulls`, `data_size` statistics, using the following syntax:

```SQL
mysql> ALTER TABLE stats_test.example_tbl MODIFY COLUMN age SET STATS ('row_count'='6001215', 'num_nulls'='2023', 'data_size'='600121522');
mysql> SHOW COLUMN STATS stats_test.example_tbl(age);
+-------------+-----------+------+----------+-----------+---------------+------+------+
| column_name | count     | ndv  | num_null | data_size | avg_size_byte | min  | max  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
| age         | 6001215.0 | 0.0  | 2023.0   | 600121522 | 0.0           | N/A  | N/A  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
```

## Delete statistics

The user deletes the statistics for the specified table, partition, or column based on the supplied parameters through the delete statistics statement `DROP`.

Grammar

```SQL
DROP [ EXPIRED ] STATS [ table_name [ (column_name [, ...]) ] ];
```

Explanation:

- Table_name: The table to which you want to delete the statistics. It can be a `db_name.table_name` form.
- Column_name: The specified target column. Must be `table_name` a column that exists in. Multiple column names are separated by commas.
- Expired: statistics cleanup. Table cannot be specified. Invalid statistics and out-of-date statistics jobs information in the system will be deleted.

Example:

- Clean up statistics, using the following syntax:

```SQL
mysql> DROP EXPIRED STATS;
```

- To delete `example_tbl` statistics for a table, use the following syntax:

```SQL
mysql> DROP STATS stats_test.example_tbl;
```

- To delete `example_tbl` statistics for a table `city`, `age` `sex` column, use the following syntax:

```SQL
mysql> DROP STATS stats_test.example_tbl(city, age, sex);
```

## Delete Analyze Job

User can delete automatic/periodic Analyze jobs based on job ID.

```sql
DROP ANALYZE JOB [JOB_ID]
```

## Full auto analyze

User could use option `enable_full_auto_analyze` to determine if enable full auto analyze, if enabled Doris would analyze all databases automatically except for some internal databases (information_db and etc.) and ignore the `AUTO`/`PERIOD` jobs. By default it's `true`.

## Other ANALYZE configuration item


| conf                                                                                                                                                                                                                                                                                                           | comment                                                                                                                                                                                                                                                                                             | default value                  |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| statistics_sql_parallel_exec_instance_num                                                                                                                                                                                                                                                                      | Control the number of concurrent instances/pipeline tasks on the BE side for each statistics collection SQL.                                                                                                                                                                                        | 1                              |
| statistics_sql_mem_limit_in_bytes                                                                                                                                                                                                                                                                              | Control the amount of BE memory that each statistics SQL can occupy.                                                                                                                                                                                                                                | 2L * 1024 * 1024 * 1024 (2GiB) |
| statistics_simultaneously_running_task_num                                                                                                                                                                                                                                                                     | The number of concurrent AnalyzeTasks that can be executed.                                                                                                                                                                                                                                         | 10                             |
| analyze_task_timeout_in_minutes                         | Execution time limit for AnalyzeTask, timeout task would be cancelled                                                                                                                                                                                                                               | 2hours                         |
|stats_cache_size|The actual memory size taken by stats cache highly depends on characteristics of data, since on the different dataset and scenarios the max/min literal's average size would be highly different. Besides, JVM version etc. also has influence on it, though not much as data itself. Here I would give the mem size taken by stats cache with 100000 items.Each item's avg length of max/min literal is 32, and the avg column name length is 16, stats cache takes total 61.2777404785MiB mem. It's strongly discourage analyzing a column with a very large STRING value in the column, since it would cause FE OOM. | 100000                        |

## Q&A

### ANALYZE WITH SYNC Execution Failure: Failed to analyze following columns...

The execution time of SQL is controlled by the query_timeout session variable, which has a default value of 300 seconds. Statements like ANALYZE DATABASE/TABLE usually take a longer time to execute and can easily exceed this time limit, leading to cancellation. It is recommended to increase the value of query_timeout based on the amount of data in the ANALYZE object.

### ANALYZE Submission Error: Stats table not available...

When executing ANALYZE, statistical data is written to the internal table __internal_schema.column_statistics. FE checks the tablet status of this table before executing ANALYZE, and if any unavailable tablet is found, the task is rejected. If this error occurs, please check the status of the BE cluster.

### ANALYZE Failure for Large Tables

Due to the strict resource limitations for ANALYZE, the ANALYZE operation for large tables might experience timeouts or exceed the memory limit of BE. In such cases, it's recommended to use ANALYZE ... WITH SAMPLE.... Additionally, for scenarios involving dynamic partitioned tables, it's highly recommended to use ANALYZE ... WITH INCREMENTAL.... This statement processes only the partitions with updated data incrementally, avoiding redundant computations and improving efficiency.