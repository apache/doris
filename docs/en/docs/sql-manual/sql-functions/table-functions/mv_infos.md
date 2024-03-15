---
{
    "title": "MV_INFOS",
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

## `mv_infos`

### Name

mv_infos

### description

Table function, generating temporary tables for asynchronous materialized views, which can view information about asynchronous materialized views created in a certain database.

This function is used in the from clause.

#### syntax

`mv_infos("database"="")`

mv_infos() Table structure:
```sql
mysql> desc function mv_infos("database"="tpch100");
+--------------------+---------+------+-------+---------+-------+
| Field              | Type    | Null | Key   | Default | Extra |
+--------------------+---------+------+-------+---------+-------+
| Id                 | BIGINT  | No   | false | NULL    | NONE  |
| Name               | TEXT    | No   | false | NULL    | NONE  |
| JobName            | TEXT    | No   | false | NULL    | NONE  |
| State              | TEXT    | No   | false | NULL    | NONE  |
| SchemaChangeDetail | TEXT    | No   | false | NULL    | NONE  |
| RefreshState       | TEXT    | No   | false | NULL    | NONE  |
| RefreshInfo        | TEXT    | No   | false | NULL    | NONE  |
| QuerySql           | TEXT    | No   | false | NULL    | NONE  |
| EnvInfo            | TEXT    | No   | false | NULL    | NONE  |
| MvProperties       | TEXT    | No   | false | NULL    | NONE  |
| MvPartitionInfo    | TEXT    | No   | false | NULL    | NONE  |
| SyncWithBaseTables | BOOLEAN | No   | false | NULL    | NONE  |
+--------------------+---------+------+-------+---------+-------+
12 rows in set (0.01 sec)
```

* Id: Materialized View ID
* Name: Materialized View Name
* JobName: The job name corresponding to the materialized view
* State: Materialized View State
* SchemaChangeDetail: The reason why the materialized view State becomes a SchemeChange
* RefreshState: Materialized view refresh status
* RefreshInfo: Refreshing strategy information defined by materialized views
* QuerySql: Query statements defined by materialized views
* EnvInfo: Environmental information during the creation of materialized views
* MvProperties: Materialized visual attributes
* MvPartitionInfo: Partition information of materialized views
* SyncWithBaseTables：Is it synchronized with the base table data? To see which partition is not synchronized, please use [SHOW PARTITIONS](../sql-reference/Show-Statements/SHOW-PARTITIONS.md)

### example

1. View all materialized views under db1

```sql
mysql> select * from mv_infos("database"="db1");
```

2. View the materialized view named mv1 under db1

```sql
mysql> select * from mv_infos("database"="db1") where Name = "mv1";
```

3. View the status of the materialized view named mv1 under db1

```sql
mysql> select State from mv_infos("database"="db1") where Name = "mv1";
```

### keywords

    mv, infos
