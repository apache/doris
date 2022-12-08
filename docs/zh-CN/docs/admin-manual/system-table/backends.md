---
{
    "title": "backends",
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

## backends

### Name

<version since="1.2">

backends

</version>

### description

`backends` 是doris内置的一张系统表，存放在`information_schema`数据库下。通过`backends`系统表可以查看当前doris集群中的 `BE` 节点信息。

`backends` 表结构为：
```sql
MySQL [information_schema]> desc information_schema.backends;
+-----------------------+-------------+------+-------+---------+-------+
| Field                 | Type        | Null | Key   | Default | Extra |
+-----------------------+-------------+------+-------+---------+-------+
| BackendId             | BIGINT      | Yes  | false | NULL    |       |
| Cluster               | VARCHAR(40) | Yes  | false | NULL    |       |
| IP                    | VARCHAR(40) | Yes  | false | NULL    |       |
| HeartbeatPort         | INT         | Yes  | false | NULL    |       |
| BePort                | INT         | Yes  | false | NULL    |       |
| HttpPort              | INT         | Yes  | false | NULL    |       |
| BrpcPort              | INT         | Yes  | false | NULL    |       |
| LastStartTime         | VARCHAR(40) | Yes  | false | NULL    |       |
| LastHeartbeat         | VARCHAR(40) | Yes  | false | NULL    |       |
| Alive                 | VARCHAR(40) | Yes  | false | NULL    |       |
| SystemDecommissioned  | VARCHAR(40) | Yes  | false | NULL    |       |
| ClusterDecommissioned | VARCHAR(40) | Yes  | false | NULL    |       |
| TabletNum             | BIGINT      | Yes  | false | NULL    |       |
| DataUsedCapacity      | BIGINT      | Yes  | false | NULL    |       |
| AvailCapacity         | BIGINT      | Yes  | false | NULL    |       |
| TotalCapacity         | BIGINT      | Yes  | false | NULL    |       |
| UsedPct               | DOUBLE      | Yes  | false | NULL    |       |
| MaxDiskUsedPct        | DOUBLE      | Yes  | false | NULL    |       |
| RemoteUsedCapacity    | BIGINT      | Yes  | false | NULL    |       |
| Tag                   | VARCHAR(40) | Yes  | false | NULL    |       |
| ErrMsg                | VARCHAR(40) | Yes  | false | NULL    |       |
| Version               | VARCHAR(40) | Yes  | false | NULL    |       |
| Status                | VARCHAR(40) | Yes  | false | NULL    |       |
+-----------------------+-------------+------+-------+---------+-------+
```
`backends` 系统表展示出来的信息基本与 `show backends` 语句展示出的信息一致。但是`backends`系统表的各个字段类型更加明确，且可以利用 `backends` 系统表去做过滤、join等操作。

### Example

```sql
MySQL [information_schema]> select * from  information_schema.backends;
+-----------+-----------------+-----------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+--------------------+------------------+--------------------+--------------------------+--------+-----------------------------+-------------------------------------------------------------------------------------------------------------------------------+
| BackendId | Cluster         | IP        | HeartbeatPort | BePort | HttpPort | BrpcPort | LastStartTime       | LastHeartbeat       | Alive | SystemDecommissioned | ClusterDecommissioned | TabletNum | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct            | MaxDiskUsedPct   | RemoteUsedCapacity | Tag                      | ErrMsg | Version                     | Status                                                                                                                        |
+-----------+-----------------+-----------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+--------------------+------------------+--------------------+--------------------------+--------+-----------------------------+-------------------------------------------------------------------------------------------------------------------------------+
|     10757 | default_cluster | 127.0.0.1 |          9159 |   9169 |     8149 |     8169 | 2022-11-24 11:16:31 | 2022-11-24 12:02:57 | true  | false                | false                 |        14 |                0 |  941359747073 | 3170529116160 | 70.309064746482065 | 70.3090647465136 |                  0 | {"location" : "default"} |        | doris-0.0.0-trunk-cc9545359 | {"lastSuccessReportTabletsTime":"2022-11-24 12:02:06","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false} |
+-----------+-----------------+-----------+---------------+--------+----------+----------+---------------------+---------------------+-------+----------------------+-----------------------+-----------+------------------+---------------+---------------+--------------------+------------------+--------------------+--------------------------+--------+-----------------------------+-------------------------------------------------------------------------------------------------------------------------------+
```

### KeyWords

    backends, information_schema

### Best Practice
