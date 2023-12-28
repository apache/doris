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


## 1. Collecting Statistics

---

### 1.1 Manual Collection Using ANALYZE Statement

Doris allows users to manually trigger the collection and update of statistics by submitting the ANALYZE statement.

Syntax:

```SQL
ANALYZE < TABLE | DATABASE table_name | db_name > 
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ] ];
```

Where:

- `table_name`: The specified target table. It can be in the format `db_name.table_name`.
- `column_name`: The specified target column. It must be an existing column in `table_name`. You can specify multiple column names separated by commas.
- `sync`: Collect statistics synchronously. Returns after collection. If not specified, it executes asynchronously and returns a JOB ID.
- `sample percent | rows`: Collect statistics with sampling. You can specify a sampling percentage or a number of sampling rows.

Here are some examples:

Collect statistics for a table with a 10% sampling rate:

```sql
ANALYZE TABLE lineitem WITH SAMPLE PERCENT 10;
```

Collect statistics for a table with a sample of 100,000 rows:

```sql
ANALYZE TABLE lineitem WITH SAMPLE ROWS 100000;
```

<br />

### 1.2 Automatic Collection

This feature has been officially supported since 2.0.3 and is enabled by default. The basic operation logic is described below. After each import transaction commit, Doris records the number of rows updated by the import transaction to estimate the health of the existing table's statistics data (for tables that have not collected statistics, their health is 0). When the health of a table is below 60 (adjustable through the `table_stats_health_threshold` parameter), Doris considers the statistics for that table outdated and triggers statistics collection jobs for that table in subsequent operations. For tables with a health value above 60, no repeated collection is performed.

The collection jobs for statistics themselves consume a certain amount of system resources. To minimize the overhead, for tables with a large amount of data (default 5 GiB, adjustable with the FE parameter `huge_table_lower_bound_size_in_bytes`), Doris automatically uses sampling to collect statistics. Automatic sampling defaults to sampling 4,194,304 (2^22) rows to reduce the system's burden and complete the collection job as quickly as possible. If you want to sample more rows to obtain a more accurate data distribution, you can increase the sampling row count by adjusting the `huge_table_default_sample_rows` parameter. In addition, for tables with data larger than `huge_table_lower_bound_size_in_bytes` * 5, Doris ensures that the collection time interval is not less than 12 hours (which can be controlled by adjusting the `huge_table_auto_analyze_interval_in_millis` parameter).

If you are concerned about automatic collection jobs interfering with your business, you can specify a time frame for the automatic collection jobs to run during low business loads by setting the `auto_analyze_start_time` and `auto_analyze_end_time` parameters according to your needs. You can also completely disable this feature by setting the `enable_auto_analyze` parameter to `false`.

External catalogs do not participate in automatic collection by default. Because external catalogs often contain massive historical data, if they participate in automatic collection, it may occupy too many resources. You can turn on and off the automatic collection of external catalogs by setting the catalog's properties.

```sql
ALTER CATALOG external_catalog SET PROPERTIES ('enable.auto.analyze'='true'); // Turn on
ALTER CATALOG external_catalog SET PROPERTIES ('enable.auto.analyze'='false'); // Turn off
```

<br />



## 2. Job Management

---

### 2.1 View Analyze Jobs

Use `SHOW ANALYZE` to view information about statistics collection jobs.

Syntax:

```SQL
SHOW [AUTO] ANALYZE < table_name | job_id >
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

- AUTO: Show historical information for automatic collection jobs only. Note that, by default, the status of only the last 20,000 completed automatic collection jobs is retained.
- table_name: Table name, specify to view statistics job information for that table. It can be in the format `db_name.table_name`. When not specified, it returns information for all statistics jobs.
- job_id: Job ID for statistics collection, obtained when executing `ANALYZE`. When not specified, this command returns information for all statistics jobs.

Output:

| Column Name           | Description      |
| :--------------------- | :--------------- |
| `job_id`               | Job ID           |
| `catalog_name`         | Catalog Name     |
| `db_name`              | Database Name    |
| `tbl_name`             | Table Name       |
| `col_name`             | Column Name List |
| `job_type`             | Job Type         |
| `analysis_type`        | Analysis Type    |
| `message`              | Job Information  |
| `last_exec_time_in_ms` | Last Execution Time |
| `state`                | Job Status       |
| `schedule_type`        | Scheduling Method |

Here's an example:

```sql
mysql> show analyze 245073\G;
*************************** 1. row ***************************
              job_id: 245073
        catalog_name: internal
             db_name: default_cluster:tpch
            tbl_name: lineitem
            col_name: [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct]
            job_type: MANUAL
       analysis_type: FUNDAMENTALS
             message: 
last_exec_time_in_ms: 2023-11-07 11:00:52
               state: FINISHED
            progress: 16 Finished  |  0 Failed  |  0 In Progress  |  16 Total
       schedule_type: ONCE
```

<br/>

### 2.2 View Column Statistics Collection Status

Each collection job can contain one or more tasks, with each task corresponding to the collection of a column. Users can use the following command to view the completion status of statistics collection for each column.

Syntax:

```sql
SHOW ANALYZE TASK STATUS [job_id]
```

Here's an example:

```
mysql> show analyze task status 20038 ;
+---------+----------+---------+----------------------+----------+
| task_id | col_name | message | last_exec_time_in_ms | state    |
+---------+----------+---------+----------------------+----------+
| 20039   | col4     |         | 2023-06-01 17:22:15  | FINISHED |
| 20040   | col2     |         | 2023-06-01 17:22:15  | FINISHED |
| 20041   | col3     |         | 2023-06-01 17:22:15  | FINISHED |
| 20042   | col1     |         | 2023-06-01 17:22:15  | FINISHED |
+---------+----------+---------+----------------------+----------+
```

<br/>

### 2.3 View Column Statistics

Use `SHOW COLUMN STATS` to view various statistics data for columns.

Syntax:

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ];
```

Where:

- cached: Show statistics information in the current FE memory cache.
- table_name: The target table for collecting statistics. It can be in the format `db_name.table_name`.
- column_name: Specifies the target column, which must be an existing column in `table_name`. You can specify multiple column names separated by commas.

Here's an example:

```sql
mysql> show column stats lineitem(l_tax)\G;
*************************** 1. row ***************************
  column_name: l_tax
        count: 6001215.0
          ndv: 9.0
     num_null: 0.0
    data_size: 4.800972E7
avg_size_byte: 8.0
          min: 0.00
          max: 0.08
       method: FULL
         type: FUNDAMENTALS
      trigger: MANUAL
  query_times: 0
 updated_time: 2023-11-07 11:00:46
```

<br/>

### 2.4 Table Collection Overview

Use `SHOW TABLE STATS` to view an overview of statistics collection for a table.

Syntax:

```SQL
SHOW TABLE STATS table_name;
```

Where:

- table_name: The target table name. It can be in the format `db_name.table_name`.

Output:

| Column Name           | Description      |
| :--------------------- | :--------------- |
| `updated_rows`        | Updated rows since the last ANALYZE |
| `query_times`         | Reserved column for recording the number of times the table was queried in future versions |
| `row_count`           | Number of rows (does not reflect the exact number of rows at the time of command execution) |
| `updated_time`        | Last update time |
| `columns`             | Columns for which statistics information has been collected |

Here's an example:

```sql
mysql> show table stats lineitem \G;
*************************** 1. row ***************************
updated_rows: 0
 query_times: 0
   row_count: 6001215
updated_time: 2023-11-07
     columns: [l_returnflag, l_receiptdate, l_tax, l_shipmode, l_suppkey, l_shipdate, l_commitdate, l_partkey, l_orderkey, l_quantity, l_linestatus, l_comment, l_extendedprice, l_linenumber, l_discount, l_shipinstruct]
     trigger: MANUAL
```

<br/>

### 2.5 Terminate Statistics Jobs

Use `KILL ANALYZE` to terminate running statistics jobs.

Syntax:

```SQL
KILL ANALYZE job_id;
```

Where:

- job_id: Job ID for statistics collection. Obtained when performing asynchronous statistics collection using the `ANALYZE` statement, and it can also be obtained through the `SHOW ANALYZE` statement.

Example:

- Terminate statistics job with ID 52357.

```SQL
mysql> KILL ANALYZE 52357;
```

<br/>


## 3. Session Variables and Configuration Options

---

### 3.1 Session Variables

|Session Variable|Description|Default Value|
|---|---|---|
|auto_analyze_start_time|Start time for automatic statistics collection|00:00:00|
|auto_analyze_end_time|End time for automatic statistics collection|23:59:59|
|enable_auto_analyze|Enable automatic collection functionality|true|
|huge_table_default_sample_rows|Sampling rows for large tables|4194304|
|huge_table_lower_bound_size_in_bytes|Tables with size greater than this value will be automatically sampled during collection of statistics|0|
|huge_table_auto_analyze_interval_in_millis|Controls the minimum time interval for automatic ANALYZE on large tables. Tables with sizes greater than `huge_table_lower_bound_size_in_bytes * 5` will be ANALYZEed only once within this time interval.|0|
|table_stats_health_threshold|Ranges from 0 to 100. If data updates since the last statistics collection exceed `(100 - table_stats_health_threshold)%`, the table's statistics are considered outdated.|60|
|analyze_timeout|Controls the timeout for synchronous ANALYZE in seconds|43200|
|auto_analyze_table_width_threshold|Controls the maximum width of table that will be auto analyzed. Table with more columns than this value will not be auto analyzed.|70|

<br/>

### 3.2 FE Configuration Options

The following FE configuration options are typically not a major concern:

|FE Configuration Option|Description|Default Value|
|---|---|---|
|analyze_record_limit|Controls the persistence of statistics job execution records|20000|
|stats_cache_size|FE-side statistics cache entries|500,000|
|statistics_simultaneously_running_task_num|Number of asynchronous jobs that can run simultaneously|3|
|statistics_sql_mem_limit_in_bytes|Controls the amount of BE memory each statistics SQL can use|2,147,483,648 bytes (2 GiB)|

<br/>

## 4. Common Issues

### 4.1 ANALYZE Submission Error: Stats table not available...

When ANALYZE is executed, statistics data is written to the internal table `__internal_schema.column_statistics`. FE checks the tablet status of this table before executing ANALYZE. If there are unavailable tablets, the task is rejected. Please check the BE cluster status if this error occurs.

Users can use `SHOW BACKENDS\G` to verify the BE (Backend) status. If the BE status is normal, you can use the command `ADMIN SHOW REPLICA STATUS FROM __internal_schema.[tbl_in_this_db]` to check the tablet status within this database, ensuring that the tablet status is also normal.

### 4.2 Failure of ANALYZE on Large Tables

Due to resource limitations, ANALYZE on some large tables may timeout or exceed BE memory limits. In such cases, it is recommended to use `ANALYZE ... WITH SAMPLE...`. 
