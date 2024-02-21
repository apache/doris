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

### 名称

<version since="dev">

active_queries

</version>

### 描述

表函数，生成active_queries临时表，可以查看当前doris集群中正在运行的 query 信息。

该函数用于from子句中。

#### 语法
`active_queries()`

active_queries()表结构：
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

### 样例
```
mysql [(none)]>select * from active_queries();
+------------+--------+-----------------------------------+---------------------+-------------+-----------------+----------------+----------+-----------+-------------------+------------------------+------------------+-----------------+----------+------------------+-----------+--------------------------------+
| BeHost     | BePort | QueryId                           | StartTime           | QueryTimeMs | WorkloadGroupId | QueryCpuTimeMs | ScanRows | ScanBytes | BePeakMemoryBytes | CurrentUsedMemoryBytes | ShuffleSendBytes | ShuffleSendRows | Database | FrontendInstance | QueryType | Sql                            |
+------------+--------+-----------------------------------+---------------------+-------------+-----------------+----------------+----------+-----------+-------------------+------------------------+------------------+-----------------+----------+------------------+-----------+--------------------------------+
| 127.0.0.1 |   6090 | dbeee76695b64a5b-b27e20242434ce64 | 2024-02-22 19:50:19 |          17 |           10002 |              0 |        0 |         0 |            471040 |                      0 |                0 |               0 |          | localhost   | QUERY     | select * from active_queries() |
+------------+--------+-----------------------------------+---------------------+-------------+-----------------+----------------+----------+-----------+-------------------+------------------------+------------------+-----------------+----------+------------------+-----------+--------------------------------+
1 rows in set (0.06 sec)
```

### 参数说明
1 QueryType
枚举类型，UNKNOWN/QUERY/INSERT/BROKER_LOAD/STREAM_LOAD。当QueryType展示为UNKNOWN时，可能是以下两种情况：
* 目前存在一个查询在FE结束时但是在BE还未结束的情况，因此当FE的查询统计信息由于查询结束被释放时，BE仍然有可能上报统计信息，此时展示的统计信息就是UNKNOWN
* query/insert/broker load/stream load之外的负载类型

### keywords

    active_queries
