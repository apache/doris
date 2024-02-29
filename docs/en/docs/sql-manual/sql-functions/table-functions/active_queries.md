---
{
    "title": "ACTIVE_QUERIES",
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

## `active_queries`

### Name

<version since="dev">

active_queries

</version>

### description

Table-Value-Function, generate a temporary table named active_queries. This tvf is used to view the information of running queries in doris cluster.

This function is used in FROM clauses.

#### syntax
`active_queries()`

active_queries() table schema：
```
mysql [(none)]> desc function active_queries();
+------------------------+--------+------+-------+---------+-------+
| Field                  | Type   | Null | Key   | Default | Extra |
+------------------------+--------+------+-------+---------+-------+
| BeHost                 | TEXT   | No   | false | NULL    | NONE  |
| BePort                 | BIGINT | No   | false | NULL    | NONE  |
| QueryId                | TEXT   | No   | false | NULL    | NONE  |
| StartTime              | TEXT   | No   | false | NULL    | NONE  |
| QueryTimeMs            | BIGINT | No   | false | NULL    | NONE  |
| WorkloadGroupId        | BIGINT | No   | false | NULL    | NONE  |
| QueryCpuTimeMs         | BIGINT | No   | false | NULL    | NONE  |
| ScanRows               | BIGINT | No   | false | NULL    | NONE  |
| ScanBytes              | BIGINT | No   | false | NULL    | NONE  |
| BePeakMemoryBytes      | BIGINT | No   | false | NULL    | NONE  |
| CurrentUsedMemoryBytes | BIGINT | No   | false | NULL    | NONE  |
| Database               | TEXT   | No   | false | NULL    | NONE  |
| FrontendInstance       | TEXT   | No   | false | NULL    | NONE  |
| Sql                    | TEXT   | No   | false | NULL    | NONE  |
+------------------------+--------+------+-------+---------+-------+
14 rows in set (0.00 sec)
```

### example
```
mysql [(none)]>select * from active_queries();
+------------+--------+----------------------------------+---------------------+-------------+-----------------+----------------+----------+------------+-------------------+------------------------+----------+------------------+-------+
| BeHost     | BePort | QueryId                          | StartTime           | QueryTimeMs | WorkloadGroupId | QueryCpuTimeMs | ScanRows | ScanBytes  | BePeakMemoryBytes | CurrentUsedMemoryBytes | Database | FrontendInstance | Sql   |
+------------+--------+----------------------------------+---------------------+-------------+-----------------+----------------+----------+------------+-------------------+------------------------+----------+------------------+-------+
| 127.0.0.1 |   6090 | 71fd11b7b0e438c-bc98434b97b8cb98 | 2024-01-16 16:21:15 |        7260 |           10002 |           8392 | 16082249 | 4941889536 |         360470040 |              360420915 | hits     | localhost   | SELECT xxxx |
+------------+--------+----------------------------------+---------------------+-------------+-----------------+----------------+----------+------------+-------------------+------------------------+----------+------------------+-------+
1 row in set (0.01 sec)
```

### keywords

    active_queries
