---
{
    "title": "backends",
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

## `backends`

### Name

<version since="dev">

backends

</version>

### description

Table-Value-Function, generate a temporary table named `backends`. This tvf is used to view the information of BE nodes in the doris cluster.

This function is used in `FROM` clauses.

#### syntax

`backends()`

The table schema of `backends()` tvf：
```
mysql> desc function backends();
+-------------------------+--------+------+-------+---------+-------+
| Field                   | Type   | Null | Key   | Default | Extra |
+-------------------------+--------+------+-------+---------+-------+
| BackendId               | BIGINT | No   | false | NULL    | NONE  |
| Cluster                 | TEXT   | No   | false | NULL    | NONE  |
| Host                    | TEXT   | No   | false | NULL    | NONE  |
| HeartbeatPort           | INT    | No   | false | NULL    | NONE  |
| BePort                  | INT    | No   | false | NULL    | NONE  |
| HttpPort                | INT    | No   | false | NULL    | NONE  |
| BrpcPort                | INT    | No   | false | NULL    | NONE  |
| LastStartTime           | TEXT   | No   | false | NULL    | NONE  |
| LastHeartbeat           | TEXT   | No   | false | NULL    | NONE  |
| Alive                   | TEXT   | No   | false | NULL    | NONE  |
| SystemDecommissioned    | TEXT   | No   | false | NULL    | NONE  |
| ClusterDecommissioned   | TEXT   | No   | false | NULL    | NONE  |
| TabletNum               | BIGINT | No   | false | NULL    | NONE  |
| DataUsedCapacity        | BIGINT | No   | false | NULL    | NONE  |
| AvailCapacity           | BIGINT | No   | false | NULL    | NONE  |
| TotalCapacity           | BIGINT | No   | false | NULL    | NONE  |
| UsedPct                 | DOUBLE | No   | false | NULL    | NONE  |
| MaxDiskUsedPct          | DOUBLE | No   | false | NULL    | NONE  |
| RemoteUsedCapacity      | BIGINT | No   | false | NULL    | NONE  |
| Tag                     | TEXT   | No   | false | NULL    | NONE  |
| ErrMsg                  | TEXT   | No   | false | NULL    | NONE  |
| Version                 | TEXT   | No   | false | NULL    | NONE  |
| Status                  | TEXT   | No   | false | NULL    | NONE  |
| HeartbeatFailureCounter | INT    | No   | false | NULL    | NONE  |
| NodeRole                | TEXT   | No   | false | NULL    | NONE  |
+-------------------------+--------+------+-------+---------+-------+
25 rows in set (0.04 sec)
```

The information displayed by the `backends` tvf is basically consistent with the information displayed by the `show backends` statement. However, the types of each field in the `backends` tvf are more specific, and you can use the `backends` tvf to perform operations such as filtering and joining.

### example
```
mysql> select * from backends()\G
*************************** 1. row ***************************
              BackendId: 10022
                Cluster: default_cluster
                   Host: 10.16.10.14
          HeartbeatPort: 9159
                 BePort: 9169
               HttpPort: 8149
               BrpcPort: 8169
          LastStartTime: 2023-03-24 14:37:00
          LastHeartbeat: 2023-03-27 20:25:35
                  Alive: true
   SystemDecommissioned: false
  ClusterDecommissioned: false
              TabletNum: 21
       DataUsedCapacity: 0
          AvailCapacity: 787460558849
          TotalCapacity: 3169589592064
                UsedPct: 75.155756416520319
         MaxDiskUsedPct: 75.155756416551881
     RemoteUsedCapacity: 0
                    Tag: {"location" : "default"}
                 ErrMsg:
                Version: doris-0.0.0-trunk-8de51f96f3
                 Status: {"lastSuccessReportTabletsTime":"2023-03-27 20:24:55","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false}
HeartbeatFailureCounter: 0
               NodeRole: mix
1 row in set (0.03 sec)
```

### keywords

    backends