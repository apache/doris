---
{
    "title": "ACTIVE_QUERIES",
    "language": "zh-CN"
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

## `active_queries`

### Name

<version since="dev">

active_queries

</version>

### description

表函数，生成active_queries临时表，可以查看当前doris集群中正在运行的 query 信息。

该函数用于from子句中。

#### syntax
`active_queries()`

active_queries()表结构：
```
mysql [(none)]>desc function active_queries();
+------------------+--------+------+-------+---------+-------+
| Field            | Type   | Null | Key   | Default | Extra |
+------------------+--------+------+-------+---------+-------+
| QueryId          | TEXT   | No   | false | NULL    | NONE  |
| StartTime        | TEXT   | No   | false | NULL    | NONE  |
| QueryTimeMs      | BIGINT | No   | false | NULL    | NONE  |
| WorkloadGroupId  | BIGINT | No   | false | NULL    | NONE  |
| Database         | TEXT   | No   | false | NULL    | NONE  |
| FrontendInstance | TEXT   | No   | false | NULL    | NONE  |
| Sql              | TEXT   | No   | false | NULL    | NONE  |
+------------------+--------+------+-------+---------+-------+
7 rows in set (0.00 sec)
```

### example
```
mysql [(none)]>select * from active_queries();
+-----------------------------------+---------------------+-------------+-----------------+----------+------------------+--------------------------------+
| QueryId                           | StartTime           | QueryTimeMs | WorkloadGroupId | Database | FrontendInstance | Sql                            |
+-----------------------------------+---------------------+-------------+-----------------+----------+------------------+--------------------------------+
| a84bf9f3ea6348e1-ac542839f8f2af5d | 2024-03-04 17:33:09 |           9 |           10002 |          | localhost        | select * from active_queries() |
+-----------------------------------+---------------------+-------------+-----------------+----------+------------------+--------------------------------+
1 row in set (0.03 sec)
```

### keywords

    active_queries
