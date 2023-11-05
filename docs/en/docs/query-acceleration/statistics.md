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

Collecting statistics helps the optimizer understand data distribution characteristics. When performing Cost-Based Optimization (CBO), the optimizer uses these statistics to calculate the selectivity of predicates and estimate the cost of each execution plan. This allows for the selection of more optimal plans, significantly improving query efficiency.

Currently, the following information is collected for each column:

| Information        | Description                    |
| :----------------- | :------------------------------ |
| `row_count`        | Total number of rows           |
| `data_size`        | Total data size                |
| `avg_size_byte`    | Average length of values       |
| `ndv`              | Number of distinct values      |
| `min`              | Minimum value                  |
| `max`              | Maximum value                  |
| `null_count`       | Number of null values          |

## Collecting Statistics

### Manual Collection Using ANALYZE Statement

Doris supports manual triggering of statistics collection and updates by executing the ANALYZE statement. The syntax is as follows:

```SQL
ANALYZE <TABLE | DATABASE table_name | db_name>
    [ (column_name [, ...]) ]
    [ [WITH SYNC] [WITH SAMPLE PERCENT | ROWS] [WITH SQL] ]
    [PROPERTIES ("key" = "value", ...)];
```

Where:

- `table_name`: Specifies the target table, which can be in the format `db_name.table_name`.
- `column_name`: Specifies the target column, which must be an existing column in `table_name`. You can specify multiple column names separated by commas.
- `sync`: Synchronously collects statistics information and returns after completion. If not specified, it executes asynchronously and returns a JOB ID.
- `sample percent | rows`: Collects statistics information with sampling. You can specify either a sampling percentage or the number of sampled rows.
- `sql`: Collects statistics information for external table partition columns using SQL. By default, partition column information is collected from metadata, which is efficient but may not be very accurate regarding row count and data size. You can specify using SQL to collect accurate partition column information.

Here are some examples:

Collect statistics data for a table with a 10% sampling rate:

```sql
ANALYZE TABLE lineitem WITH SAMPLE PERCENT 10;
```

Collect statistics data for a table by sampling 100,000 rows:

```sql
ANALYZE TABLE lineitem WITH SAMPLE ROWS 100000;
```

### Automatic Collection

This feature is officially supported starting from 2.0.3 and is enabled by default. The basic operational logic is as follows: after each transaction import, Doris records the number of rows updated in the table as a means to estimate the health of the statistics data for existing tables (for tables without collected statistics, the health is considered as 0). When the health of a table falls below 60 (adjustable using the `table_stats_health_threshold` parameter), Doris considers the statistics data for that table outdated and triggers statistics collection jobs for that table.

For large tables (default is 5GiB, adjustable using the `huge_table_lower_bound_size_in_bytes` FE parameter), Doris uses sampling to collect statistics. The default sampling is 4,194,304 (2^22) rows to minimize the system's overhead and complete the collection job as quickly as possible. If you need to sample more rows for more accurate data distribution information, you can configure this by increasing the `huge_table_default_sample_rows` FE parameter. Additionally, for tables with data sizes greater than `huge_table_lower_bound_size_in_bytes * 5`, Doris guarantees a collection interval of no less than 12 hours (controllable through the `huge_table_auto_analyze_interval_in_millis` FE parameter).

If you are concerned about automatic collection jobs interfering with your business operations, you can specify the time period for automatic collection by setting the `full_auto_analyze_start_time` and `full_auto_analyze_end_time` parameters to execute the jobs during low business loads. You can also completely disable this feature by setting the `enable_full_auto_analyze` parameter to `false`.

### Task Management

#### Viewing Statistics Tasks

You can use `SHOW ANALYZE` to view information about statistics collection tasks. The syntax is as follows:

```SQL
SHOW [AUTO] ANALYZE <table_name | job_id>
    [WHERE [STATE = ["PENDING" | "RUNNING" | "FINISHED" | "FAILED"]]];
```

- AUTO: Shows only historical information for automatic collection jobs. It's important to note that, by default, only the status of the last 20,000 completed automatic collection tasks is retained.
- table_name: Specifies the table for which you want to view statistics task information. It can be in the format `db_name.table_name`. If not specified, it returns information on all statistics tasks.
- job_id: The ID of the statistics task. This is the value returned when executing an `ANALYZE` asynchronous collection task. If not specified, it returns information on all statistics tasks.

Output:

| Column Name             | Description    |
| :---------------------- | :------------- |
| `job_id`                | Statistics Task ID |
| `catalog_name`          | Catalog Name   |
| `db_name`               | Database Name  |
| `tbl_name`              | Table Name     |
| `col_name`              | Column Name    |
| `job_type`              | Task Type      |
| `analysis_type`         | Statistics Type |
| `message`               | Task Information |
| `last_exec_time_in_ms`  | Last Execution Time |
| `state`                 | Task Status    |
| `schedule_type`         | Scheduling Method |

#### Viewing Column Statistics Collection

Syntax:

```sql
SHOW ANALYZE TASK STATUS [job_id]
```

Here is an example:

```sql
mysql> show analyze task status  20038;
+---------+----------+---------+----------------------+----------+
| task_id | col_name | message | last_exec_time_in_ms | state    |
+---------+----------+---------+----------------------+----------+
| 20039   | col4     |         | 2023-06-01 17:22:15  | FINISHED |
| 20040   | col2     |         | 2023-06-01 17:22:15  | FINISHED |
| 20041   | col3     |         | 2023-06-01 17:22:15  | FINISHED |
| 20042   | col1     |         | 2023-06-01 17:22:15  | FINISHED |
+---------+----------+---------+----------------------+----------+
```

#### Terminating Statistics Tasks

You can use `KILL ANALYZE` to terminate running statistics tasks. The syntax is as follows:

```SQL
KILL ANALYZE job_id;
```

- job_id: The ID of the statistics task, which is the value returned when executing an `ANALYZE` asynchronous collection task. You can also obtain this ID by using the `SHOW ANALYZE` statement.

Example:

Terminate the statistics task with ID 52357:

```SQL
mysql> KILL ANALYZE 52357;
```


```markdown
### Viewing Statistics

#### Viewing Column Statistics

Use `SHOW COLUMN STATS` to view the number of distinct values and the number of NULL values for columns.

The syntax is as follows:

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ];
```

Where:

- `cached`: Displays statistics information currently in FE memory cache.
- `table_name`: The target table for which you want to collect statistics. It can be in the format `db_name.table_name`.
- `column_name`: Specifies the target columns, which must be existing columns in `table_name`. You can specify multiple column names separated by commas.

#### Table Statistics Overview

Use `SHOW TABLE STATS` to view an overview of table statistics collection.

The syntax is as follows:

```SQL
SHOW TABLE STATS table_name;
```

Where:

- `table_name`: The name of the target table. It can be in the format `db_name.table_name`.

Output:

| Column Name      | Description                                |
| :--------------- | :----------------------------------------- |
| `updated_rows`   | Number of rows updated since the last ANALYZE |
| `query_times`    | Reserved column for recording the number of queries in future versions |
| `row_count`      | Number of rows (may not reflect the exact number of rows at the time of execution) |
| `updated_time`   | Last update time |
| `columns`        | Columns for which statistics have been collected |

### Injecting Statistics

Users can adjust statistics using the `ALTER` statement.

The syntax is as follows:

```SQL
ALTER TABLE table_name MODIFY COLUMN column_name SET STATS ('stat_name' = 'stat_value', ...) [ PARTITION (partition_name) ];
```

Where:

- `table_name`: The target table for which you want to adjust statistics. It can be in the format `db_name.table_name`.
- `column_name`: Specifies the target column, which must be an existing column in `table_name`. You can modify statistics for one column at a time.
- `stat_name` and `stat_value`: Corresponding statistic names and values, separated by commas. You can modify statistics such as `row_count`, `ndv`, `num_nulls`, `min_value`, `max_value`, and `data_size`.

#### Deleting Statistics

Users can use the `DROP` statement to delete statistics. Depending on the provided parameters, you can delete statistics for specific tables, partitions, or columns. When deleting, column statistics and column histogram information are also removed.

Syntax:

```SQL
DROP [ EXPIRED ] STATS [ table_name [ (column_name [, ...]) ] ];
```

#### Deleting Analyze Jobs

Used to delete automatic/periodic Analyze jobs based on job ID.

```SQL
DROP ANALYZE JOB [JOB_ID]
```

## Configuration Settings

| Session Variable | Description | Default Value |
| --- | --- | --- |
| full_auto_analyze_start_time | Start time for automatic statistics collection | 00:00:00 |
| full_auto_analyze_end_time | End time for automatic statistics collection | 23:59:59 |
| enable_full_auto_analyze | Enable automatic collection feature | true |
| huge_table_default_sample_rows | Defines the number of rows to sample for large tables when automatic sampling is enabled | 4194304 |
| huge_table_lower_bound_size_in_bytes | Tables larger than this size will automatically collect statistics through sampling during automatic collection | 5368709120 |
| huge_table_auto_analyze_interval_in_millis | Controls the minimum time interval for automatic ANALYZE on large tables; for tables larger than `huge_table_lower_bound_size_in_bytes * 5`, ANALYZE will only be executed once within this interval | 43200000 |
| table_stats_health_threshold | Value between 0 and 100; when the data update exceeds `(100 - table_stats_health_threshold)%` since the last statistics collection operation, the statistics for the table are considered outdated | 60 |
| analyze_timeout | Controls the synchronous ANALYZE timeout in seconds | 43200 |

The following FE configuration settings are generally not a concern:

| FE Configuration Setting | Description | Default Value |
| --- | --- | --- |
| analyze_record_limit | Controls the number of persistent records for statistics job execution | 20000 |
| stats_cache_size | The actual memory usage for statistics cache highly depends on data characteristics; with 100,000 items in the cache, each with an average length of 32 for maximum/minimum values and an average column name length of 16, the statistics cache occupies approximately 61.28 MiB of memory | 500000 |
| statistics_simultaneously_running_task_num | The number of asynchronous tasks that can be executed simultaneously | 3 |
| analyze_task_timeout_in_hours | Asynchronous task execution timeout | 12 |
| statistics_sql_mem_limit_in_bytes | Controls the maximum BE memory that each statistics SQL can occupy | 2 GiB |


## Common Issues

### ANALYZE WITH SYNC Execution Failed: Failed to analyze following columns...

The SQL execution time is controlled by the `query_timeout` session variable, which has a default value of 300 seconds. Statements like `ANALYZE DATABASE/TABLE` often take longer, easily exceeding this time limit and being canceled. It is recommended to increase the value of `query_timeout` based on the data volume of the ANALYZE object.

### ANALYZE Submission Error: Stats table not available...

When ANALYZE is executed, statistics data is written to the internal table `__internal_schema.column_statistics`. FE checks the tablet status of this table before executing ANALYZE. If there are unavailable tablets, the task is rejected. Please check the BE cluster status if this error occurs.

Users can use `SHOW BACKENDS\G` to verify the BE (Backend) status. If the BE status is normal, you can use the command `ADMIN SHOW REPLICA STATUS FROM __internal_schema.[tbl_in_this_db]` to check the tablet status within this database, ensuring that the tablet status is also normal.

### Failure of ANALYZE on Large Tables

Due to resource limitations, ANALYZE on some large tables may timeout or exceed BE memory limits. In such cases, it is recommended to use `ANALYZE ... WITH SAMPLE...`. 
