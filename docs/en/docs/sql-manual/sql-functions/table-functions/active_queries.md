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
mysql [(none)]>desc function active_queries();
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
| ShuffleSendBytes       | BIGINT | No   | false | NULL    | NONE  |
| ShuffleSendRows        | BIGINT | No   | false | NULL    | NONE  |
| Database               | TEXT   | No   | false | NULL    | NONE  |
| FrontendInstance       | TEXT   | No   | false | NULL    | NONE  |
| QueryType              | TEXT   | No   | false | NULL    | NONE  |
| Sql                    | TEXT   | No   | false | NULL    | NONE  |
+------------------------+--------+------+-------+---------+-------+
17 rows in set (0.01 sec)
```

### example
```
mysql [(none)]>select * from active_queries();
+------------+--------+-----------------------------------+---------------------+-------------+-----------------+----------------+----------+-----------+-------------------+------------------------+------------------+-----------------+----------+------------------+-----------+--------------------------------+
| BeHost     | BePort | QueryId                           | StartTime           | QueryTimeMs | WorkloadGroupId | QueryCpuTimeMs | ScanRows | ScanBytes | BePeakMemoryBytes | CurrentUsedMemoryBytes | ShuffleSendBytes | ShuffleSendRows | Database | FrontendInstance | QueryType | Sql                            |
+------------+--------+-----------------------------------+---------------------+-------------+-----------------+----------------+----------+-----------+-------------------+------------------------+------------------+-----------------+----------+------------------+-----------+--------------------------------+
| 127.0.0.1 |   6090 | dbeee76695b64a5b-b27e20242434ce64 | 2024-02-22 19:50:19 |          17 |           10002 |              0 |        0 |         0 |            471040 |                      0 |                0 |               0 |          | localhost   | QUERY     | select * from active_queries() |
+------------+--------+-----------------------------------+---------------------+-------------+-----------------+----------------+----------+-----------+-------------------+------------------------+------------------+-----------------+----------+------------------+-----------+--------------------------------+
1 rows in set (0.06 sec)
```

### Parameter Description
1 QueryType

enumeration type, UNKNOWN/QUERY/INSERT/BROKER-LOAD/STREAM-LOAD. When QueryType shows UNKNOWN, it may be one of the following two situations:
* At present, there is a situation where a query ends at FE but is not yet completed at BE. Therefore, when the query statistics of FE are released due to the end of the query, BE may still report statistical information, and the displayed statistical information is UNKNOWN
* Load types other than query/insert/broker load/stream load.

### keywords

    active_queries
