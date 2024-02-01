---
{
    "title": "SHOW-ANALYZE",
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

## SHOW-ANALYZE

### Name

SHOW ANALYZE

### Description

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

### Keywords

SHOW, ANALYZE