---
{
    "title": "QUERIES",
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

## `queries`

### Name

<version since="dev">

queries

</version>

### description

表函数，生成queries临时表，可以查看当前doris集群中正在运行的以及历史的 query 信息。

该函数用于from子句中。

#### syntax
`queries()`

queries()表结构：
```
mysql> desc function queries();
+------------------+--------+------+-------+---------+-------+
| Field            | Type   | Null | Key   | Default | Extra |
+------------------+--------+------+-------+---------+-------+
| QueryId          | TEXT   | No   | false | NULL    | NONE  |
| StartTime        | BIGINT | No   | false | NULL    | NONE  |
| EndTime          | BIGINT | Yes  | false | NULL    | NONE  |
| EventTime        | BIGINT | Yes  | false | NULL    | NONE  |
| Latency          | BIGINT | No   | false | NULL    | NONE  |
| State            | TEXT   | No   | false | NULL    | NONE  |
| Database         | TEXT   | Yes  | false | NULL    | NONE  |
| Sql              | TEXT   | No   | false | NULL    | NONE  |
| FrontendInstance | TEXT   | No   | false | NULL    | NONE  |
+------------------+--------+------+-------+---------+-------+
9 rows in set (0.00 sec)
```

### example
```
mysql> select* from queries();
+-----------------------------------+---------------+---------------+---------------+---------+----------+----------+------------------------+------------------+
| QueryId                           | StartTime     | EndTime       | EventTime     | Latency | State    | Database | Sql                    | FrontendInstance |
+-----------------------------------+---------------+---------------+---------------+---------+----------+----------+------------------------+------------------+
| e1293f2ed2a5427a-982301c462586043 | 1699255138730 | 1699255139823 | 1699255139823 |    1093 | FINISHED | demo     | select* from queries() | localhost        |
| 46fa3ad0e7814ebd-b1cd34940a29b1e9 | 1699255143588 |            -1 | 1699255143588 |      20 | RUNNING  | demo     | select* from queries() | localhost        |
+-----------------------------------+---------------+---------------+---------------+---------+----------+----------+------------------------+------------------+
2 rows in set (0.04 sec)
```

### keywords

    queries
