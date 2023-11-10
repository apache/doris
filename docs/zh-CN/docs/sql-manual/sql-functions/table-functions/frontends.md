---
{
    "title": "FRONTENDS",
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

## `frontends`

### Name

<version since="dev">

frontends

</version>

### description

表函数，生成frontends临时表，可以查看当前doris集群中的 FE 节点信息。

该函数用于from子句中。

#### syntax
`frontends()`

frontends()表结构：
```
mysql> desc function frontends();
+-------------------+------+------+-------+---------+-------+
| Field             | Type | Null | Key   | Default | Extra |
+-------------------+------+------+-------+---------+-------+
| Name              | TEXT | No   | false | NULL    | NONE  |
| Host              | TEXT | No   | false | NULL    | NONE  |
| EditLogPort       | TEXT | No   | false | NULL    | NONE  |
| HttpPort          | TEXT | No   | false | NULL    | NONE  |
| QueryPort         | TEXT | No   | false | NULL    | NONE  |
| RpcPort           | TEXT | No   | false | NULL    | NONE  |
| ArrowFlightSqlPort| TEXT | No   | false | NULL    | NONE  |
| Role              | TEXT | No   | false | NULL    | NONE  |
| IsMaster          | TEXT | No   | false | NULL    | NONE  |
| ClusterId         | TEXT | No   | false | NULL    | NONE  |
| Join              | TEXT | No   | false | NULL    | NONE  |
| Alive             | TEXT | No   | false | NULL    | NONE  |
| ReplayedJournalId | TEXT | No   | false | NULL    | NONE  |
| LastHeartbeat     | TEXT | No   | false | NULL    | NONE  |
| IsHelper          | TEXT | No   | false | NULL    | NONE  |
| ErrMsg            | TEXT | No   | false | NULL    | NONE  |
| Version           | TEXT | No   | false | NULL    | NONE  |
| CurrentConnected  | TEXT | No   | false | NULL    | NONE  |
+-------------------+------+------+-------+---------+-------+
17 rows in set (0.022 sec)
```

`frontends()` tvf展示出来的信息基本与 `show frontends` 语句展示出的信息一致,但是 `frontends()` tvf的各个字段类型更加明确，且可以利用tvf生成的表去做过滤、join等操作。

对 `frontends()` tvf信息展示进行了鉴权，与 `show frontends` 行为保持一致，要求用户具有 ADMIN/OPERATOR 权限。

### example
```
mysql> select * from frontends()\G
*************************** 1. row ***************************
             Name: fe_5fa8bf19_fd6b_45cb_89c5_25a5ebc45582
               IP: 10.xx.xx.14
      EditLogPort: 9013
         HttpPort: 8034
        QueryPort: 9033
          RpcPort: 9023
ArrowFlightSqlPort: 9040
             Role: FOLLOWER
         IsMaster: true
        ClusterId: 1258341841
             Join: true
            Alive: true
ReplayedJournalId: 186
    LastHeartbeat: 2023-06-15 16:53:12
         IsHelper: true
           ErrMsg: 
          Version: doris-0.0.0-trunk-4b18cde0c7
 CurrentConnected: Yes
1 row in set (0.060 sec)
```

### keywords

    frontends