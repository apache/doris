---
{
    "title": "TASKS",
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

## `tasks`

### Name

tasks

### description

Table function, generating a temporary task table, which can view task information in a certain task type.

This function is used in the from clause.

#### syntax

`tasks("type"="")`

tasks("type"="mv") Table structure:
```sql
mysql> desc function tasks("type"="mv");
+-----------------------+------+------+-------+---------+-------+
| Field                 | Type | Null | Key   | Default | Extra |
+-----------------------+------+------+-------+---------+-------+
| TaskId                | TEXT | No   | false | NULL    | NONE  |
| JobId                 | TEXT | No   | false | NULL    | NONE  |
| JobName               | TEXT | No   | false | NULL    | NONE  |
| MvId                  | TEXT | No   | false | NULL    | NONE  |
| MvName                | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseId          | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseName        | TEXT | No   | false | NULL    | NONE  |
| Status                | TEXT | No   | false | NULL    | NONE  |
| ErrorMsg              | TEXT | No   | false | NULL    | NONE  |
| CreateTime            | TEXT | No   | false | NULL    | NONE  |
| StartTime             | TEXT | No   | false | NULL    | NONE  |
| FinishTime            | TEXT | No   | false | NULL    | NONE  |
| DurationMs            | TEXT | No   | false | NULL    | NONE  |
| TaskContext           | TEXT | No   | false | NULL    | NONE  |
| RefreshMode           | TEXT | No   | false | NULL    | NONE  |
| NeedRefreshPartitions | TEXT | No   | false | NULL    | NONE  |
| CompletedPartitions   | TEXT | No   | false | NULL    | NONE  |
| Progress              | TEXT | No   | false | NULL    | NONE  |
+-----------------------+------+------+-------+---------+-------+
18 rows in set (0.00 sec)
```

* TaskId: task id
* JobId: job id
* JobName: job Name
* MvId: Materialized View ID
* MvName: Materialized View Name
* MvDatabaseId: DB ID of the materialized view
* MvDatabaseName: Name of the database to which the materialized view belongs
* Status: task status
* ErrorMsg: Task failure information
* CreateTime: Task creation time
* StartTime: Task start running time
* FinishTime: Task End Run Time
* DurationMs: Task runtime
* TaskContext: Task running parameters
* RefreshMode: refresh mode
* NeedRefreshPartitions: The partition information that needs to be refreshed for this task
* CompletedPartitions: The partition information that has been refreshed for this task
* Progress: Task running progress

### example

1. View tasks for all materialized views

```sql
mysql> select * from tasks("type"="mv");
```

2. View all tasks with jobName `inner_mtmv_75043`

```sql
mysql> select * from tasks("type"="mv") where JobName="inner_mtmv_75043";
```

### keywords

    tasks
