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

Collecting statistics helps the optimizer understand data distribution characteristics. When performing Cost-Based Optimization (CBO), the optimizer utilizes these statistics to calculate the selectivity of predicates and estimate the cost of each execution plan. This enables the selection of more efficient plans, significantly improving query performance.

Currently, the collected column-level information includes:

| Information      | Description              |
| :--------------- | :------------------------ |
| `row_count`      | Total number of rows     |
| `data_size`      | Total data size          |
| `avg_size_byte`  | Average length of values |
| `ndv`            | Number of distinct values |
| `min`            | Minimum value            |
| `max`            | Maximum value            |
| `null_count`     | Number of null values    |

## Collecting Statistics

### Using the ANALYZE Statement

Doris supports users in triggering the collection and updating of statistics by submitting the ANALYZE statement.

Syntax:

```SQL
ANALYZE < TABLE | DATABASE table_name | db_name >
    [ PARTITIONS [(*) | (partition_name [, ...]) | WITH RECENT COUNT ] ]
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ] [ WITH SQL ] ]
    [ PROPERTIES ("key" = "value", ...) ];
```

Where:

- `table_name`: Specifies the target table. It can be in the `db_name.table_name` format.
- `partition_name`: The specified target partitions（for hive external table only）。Must be partitions exist in `table_name`. Multiple partition names are separated by commas. e.g. for single level partition: PARTITIONS(`event_date=20230706`), for multi level partition: PARTITIONS(`nation=US/city=Washington`). PARTITIONS(*) specifies all partitions, PARTITIONS WITH RECENT 30 specifies the latest 30 partitions.
- `column_name`: Specifies the target column. It must be an existing column in `table_name`, and multiple column names are separated by commas.
- `sync`: Collect statistics synchronously. Returns upon completion. If not specified, it executes asynchronously and returns a task ID.
- `sample percent | rows`: Collect statistics using sampling. You can specify either the sampling percentage or the number of sampled rows.
- `sql`: Collect statistics for external partition column with sql. By default, it uses meta data for partition columns, which is faster but may inaccurate for row count and size. Using sql could collect the accurate stats.

### Automatic Statistics Collection

Users can enable this feature by setting the FE configuration option `enable_full_auto_analyze = true`. Once enabled, statistics on qualifying tables and columns will be automatically collected during specified time intervals. Users can specify the automatic collection time period by setting the `full_auto_analyze_start_time` (default is 00:00:00) and `full_auto_analyze_end_time` (default is 02:00:00) parameters.

This feature collects statistics only for tables and columns that either have no statistics or have outdated statistics. When more than 20% of the data in a table is updated (this value can be configured using the `table_stats_health_threshold` parameter with a default of 80), Doris considers the statistics for that table to be outdated.

For tables with a large amount of data (default is 5GiB), Doris will automatically use sampling to collect statistics, reducing the impact on the system and completing the collection job as quickly as possible. Users can adjust this behavior by setting the `huge_table_lower_bound_size_in_bytes` FE parameter. If you want to collect statistics for all tables in full, you can set the `enable_auto_sample` FE parameter to false. For tables with data size greater than `huge_table_lower_bound_size_in_bytes`, Doris ensures that the collection interval is not less than 12 hours (this time can be controlled using the `huge_table_auto_analyze_interval_in_millis` FE parameter).

The default sample size for automatic sampling is 4194304(2^22) rows, but the actual sample size may be larger due to implementation reasons. If you want to sample more rows to obtain more accurate data distribution information, you can configure the `huge_table_default_sample_rows` FE parameter.

### Task Management

#### Viewing Analyze Tasks

You can use `SHOW ANALYZE` to view information about statistics collection tasks.

Syntax:

```SQL
SHOW ANALYZE < table_name | job_id >
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

- `table_name`: Specifies the table for which you want to view statistics collection tasks. It can be in the form of `db_name.table_name`. If not specified, it returns information for all statistics collection tasks.
- `job_id`: The job ID of the statistics information task returned when executing `ANALYZE`. If not specified, it returns information for all statistics collection tasks.

Output:

| Column Name           | Description    |
| :-------------------- | :------------- |
| `job_id`              | Job ID         |
| `catalog_name`        | Catalog Name   |
| `db_name`             | Database Name  |
| `tbl_name`            | Table Name     |
| `col_name`            | Column Name    |
| `job_type`            | Job Type       |
| `analysis_type`       | Analysis Type  |
| `message`             | Task Message   |
| `last_exec_time_in_ms`| Last Execution Time |
| `state`               | Task State     |
| `schedule_type`       | Schedule Type  |


You can use `SHOW ANALYZE TASK STATUS [job_id]` to check the completion status of collecting statistics for each column.

```
mysql> show analyze task status 20038;
+---------+----------+---------+----------------------+----------+
| task_id | col_name | message | last_exec_time_in_ms | state    |
+---------+----------+---------+----------------------+----------+
| 20039   | col4     |         | 2023-06-01 17:22:15  | FINISHED |
| 20040   | col2     |         | 2023-06-01 17:22:15  | FINISHED |
| 20041   | col3     |         | 2023-06-01 17:22:15  | FINISHED |
| 20042   | col1     |         | 2023-06-01 17:22:15  | FINISHED |
+---------+----------+---------+----------------------+----------+
```

#### Terminating Analyze Tasks

You can terminate running statistics collection tasks using `KILL ANALYZE`.

Syntax:

```SQL
KILL ANALYZE job_id;
```

- `job_id`: The job ID of the statistics information task. It is returned when executing `ANALYZE`, or you can obtain it using the `SHOW ANALYZE` statement.

Example:

- Terminating statistics collection task with job ID 52357.

```SQL
mysql> KILL ANALYZE 52357;
```

#### Viewing Statistics Information

#### Table Statistics Information

You can use `SHOW TABLE STATS` to view an overview of statistics collection for a table.

Syntax:

```SQL
SHOW TABLE STATS table_name;
```

- `table_name`: The name of the table for which you want to view statistics collection information. It can be in the form of `db_name.table_name`.

Output:

| Column Name      | Description                            |
| :--------------- | :------------------------------------- |
| `row_count`      | Number of rows (may not be the exact count at the time of execution) |
| `method`         | Collection method (FULL/SAMPLE)        |
| `type`           | Type of statistics data                 |
| `updated_time`   | Last update time                       |
| `columns`        | Columns for which statistics were collected |
| `trigger`        | Trigger method for statistics collection (Auto/User) |


#### Viewing Column Statistics Information

You can use `SHOW COLUMN [cached] STATS` to view information about the number of distinct values and NULLs in columns.

Syntax:

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ];
```

- `cached`: Displays statistics information from the current FE memory cache.
- `table_name`: The name of the table for which you want to view column statistics information. It can be in the form of `db_name.table_name`.
- `column_name`: The specific column(s) you want to view statistics for. It must be a column that exists in `table_name`, and multiple column names can be separated by commas.

#### Modifying Statistics Information

Users can adjust statistics information using the `ALTER` statement.

```SQL
ALTER TABLE table_name MODIFY COLUMN column_name SET STATS ('stat_name' = 'stat_value', ...) [ PARTITION (partition_name) ];
```

- `table_name`: The name of the table for which you want to modify statistics information. It can be in the form of `db_name.table_name`.
- `column_name`: The specific column for which you want to modify statistics information. It must be a column that exists in `table_name`, and you can modify statistics information for one column at a time.
- `stat_name` and `stat_value`: The corresponding statistics information name and its value. Multiple statistics can be modified, separated by commas. You can modify statistics such as `row_count`, `ndv`, `num_nulls`, `min_value`, `max_value`, and `data_size`.

#### Delete Statistics

Users can delete statistics using the `DROP` statement, which allows them to specify the table, partition, or column for which they want to delete statistics based on the provided parameters. When deleted, both column statistics and column histogram information are removed.

Syntax:

```SQL
DROP [ EXPIRED ] STATS [ table_name [ (column_name [, ...]) ] ];
```

#### Delete Analyze Job

Used to delete automatic/periodic Analyze jobs based on the job ID.

```sql
DROP ANALYZE JOB [JOB_ID]
```

### View Automatic Collection Task Execution Status

This command is used to check the completion status of automatic collection tasks after enabling automatic collection functionality.

```sql
SHOW AUTO ANALYZE [table_name]
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

Automatic collection tasks do not support viewing the completion status and failure reasons for each column individually. By default, it only retains the status of the last 20,000 completed automatic collection tasks.

## Configuration Options

| fe conf option                                                    | comment                                                                                                                                                                                                                                                                                             | default value                  |
|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| statistics_sql_parallel_exec_instance_num               | Controls the number of concurrent instances/pipeline tasks for each statistics collection SQL on the BE side.                                                                                                                                                                                                                                                           | 1                              |
| statistics_sql_mem_limit_in_bytes                       | Controls the amount of BE memory that each statistics collection SQL can use.                                                                                                                                                                                                                                                                                 | 2L * 1024 * 1024 * 1024 (2GiB) |
| statistics_simultaneously_running_task_num              | After submitting asynchronous jobs using `ANALYZE TABLE[DATABASE]`, this parameter limits the number of columns that can be analyzed simultaneously. All asynchronous tasks are collectively constrained by this parameter.                                                                                                                                                                                                                                  | 5                              |
| analyze_task_timeout_in_minutes                         | Timeout for AnalyzeTask execution.                                                                                                                                                                                                                                                                                   | 12 hours                       |
| stats_cache_size| The actual memory usage of statistics cache depends heavily on the characteristics of the data because the average size of maximum/minimum values and the number of buckets in histograms can vary greatly in different datasets and scenarios. Additionally, factors like JVM versions can also affect it. Below is the memory size occupied by statistics cache with 100,000 items. The average length of maximum/minimum values per item is 32, the average length of column names is 16, and the statistics cache occupies a total of 61.2777404785MiB of memory. It is strongly discouraged to analyze columns with very large string values as this may lead to FE memory overflow. | 100000                        |
|enable_auto_sample|Enable automatic sampling for large tables. When enabled, statistics will be automatically collected through sampling for tables larger than the `huge_table_lower_bound_size_in_bytes` threshold.| false|
|auto_analyze_job_record_count|Controls the persistence of records for automatically triggered statistics collection jobs.|20000|
|huge_table_default_sample_rows|Defines the number of sample rows for large tables when automatic sampling is enabled.|4194304|
|huge_table_lower_bound_size_in_bytes|Defines the lower size threshold for large tables. When `enable_auto_sample` is enabled, statistics will be automatically collected through sampling for tables larger than this value.|5368 709120|
|huge_table_auto_analyze_interval_in_millis|Controls the minimum time interval for automatic ANALYZE on large tables. Within this interval, tables larger than `huge_table_lower_bound_size_in_bytes` will only be analyzed once.|43200000|
|table_stats_health_threshold|Takes a value between 0-100. When the data update volume reaches (100 - table_stats_health_threshold)% since the last statistics collection operation, the statistics for the table are considered outdated.|80|

|Session Variable|Description|Default Value|
|---|---|---|
|full_auto_analyze_start_time|Start time for automatic statistics collection|00:00:00|
|full_auto_analyze_end_time|End time for automatic statistics collection|02:00:00|
|enable_full_auto_analyze|Enable automatic collection functionality|true|

ATTENTION: The session variables listed above must be set globally using SET GLOBAL.

## Usage Recommendations

Based on our testing, on tables with data size (i.e., actual storage space) below 128GiB, there is usually no need to modify the default configuration settings unless it is necessary to avoid resource contention during peak business hours by adjusting the execution time of the automatic collection feature.

Depending on the cluster configuration, automatic collection tasks typically consume around 20% of CPU resources. Therefore, users should adjust the execution time of the automatic collection feature to avoid resource contention during peak business hours, depending on their specific business needs.

Since ANALYZE is a resource-intensive operation, it is best to avoid executing such operations during peak business hours to prevent disruption to the business. Additionally, in cases of high cluster load, ANALYZE operations are more likely to fail. Furthermore, it is advisable to avoid performing full ANALYZE on the entire database or table. Typically, it is sufficient to perform ANALYZE on columns that are frequently used as predicate conditions, in JOIN conditions, as aggregation fields, or as ID fields. If a user's SQL queries involve a large number of such operations and the table has no statistics or very outdated statistics, we recommend:

* Performing ANALYZE on the columns involved in complex queries before submitting the complex query, as poorly planned complex queries can consume a significant amount of system resources and may lead to resource exhaustion or timeouts.
* If you have configured periodic data import routines for Doris, it is recommended to execute ANALYZE after the data import is complete to ensure that subsequent query planning can use the most up-to-date statistics. You can automate this setting using Doris's existing job scheduling framework.
* When significant changes occur in the table's data, such as creating a new table and completing data import, it is recommended to run ANALYZE on the corresponding table.

## Common Issues

### ANALYZE WITH SYNC Execution Failed: Failed to analyze following columns...

The SQL execution time is controlled by the `query_timeout` session variable, which has a default value of 300 seconds. Statements like `ANALYZE DATABASE/TABLE` often take longer, easily exceeding this time limit and being canceled. It is recommended to increase the value of `query_timeout` based on the data volume of the ANALYZE object.

### ANALYZE Submission Error: Stats table not available...

When ANALYZE is executed, statistics data is written to the internal table `__internal_schema.column_statistics`. FE checks the tablet status of this table before executing ANALYZE. If there are unavailable tablets, the task is rejected. Please check the BE cluster status if this error occurs.

Users can use `SHOW BACKENDS\G` to verify the BE (Backend) status. If the BE status is normal, you can use the command `ADMIN SHOW REPLICA STATUS FROM __internal_schema.[tbl_in_this_db]` to check the tablet status within this database, ensuring that the tablet status is also normal.

### Failure of ANALYZE on Large Tables

Due to resource limitations, ANALYZE on some large tables may timeout or exceed BE memory limits. In such cases, it is recommended to use `ANALYZE ... WITH SAMPLE...`. 

