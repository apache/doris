---
{
    "title": "JOBS",
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

## `jobs`

### Name

jobs

### description

Table function, generating a temporary task table, which can view job information in a certain task type.

This function is used in the from clause.

#### syntax

`jobs("type"="")`

jobs("type"="mv")Table structure:
```sql
mysql> desc function jobs("type"="mv");
+-------------------+------+------+-------+---------+-------+
| Field             | Type | Null | Key   | Default | Extra |
+-------------------+------+------+-------+---------+-------+
| Id                | TEXT | No   | false | NULL    | NONE  |
| Name              | TEXT | No   | false | NULL    | NONE  |
| MvId              | TEXT | No   | false | NULL    | NONE  |
| MvName            | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseId      | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseName    | TEXT | No   | false | NULL    | NONE  |
| ExecuteType       | TEXT | No   | false | NULL    | NONE  |
| RecurringStrategy | TEXT | No   | false | NULL    | NONE  |
| Status            | TEXT | No   | false | NULL    | NONE  |
| CreateTime        | TEXT | No   | false | NULL    | NONE  |
+-------------------+------+------+-------+---------+-------+
10 rows in set (0.00 sec)
```

* Id: job ID.
* Name: job name.
* MvId: Materialized View ID
* MvName: Materialized View Name
* MvDatabaseId: DB ID of the materialized view
* MvDatabaseName: Name of the database to which the materialized view belongs
* ExecuteType: Execution type
* RecurringStrategy: Loop strategy
* Status: Job status
* CreateTime: Task creation time

### example

1. View jobs in all materialized views

```sql
mysql> select * from jobs("type"="mv");
```

2. View job with name `inner_mtmv_75043`

```sql
mysql> select * from jobs("type"="mv") where Name="inner_mtmv_75043";
```

### keywords

    jobs
