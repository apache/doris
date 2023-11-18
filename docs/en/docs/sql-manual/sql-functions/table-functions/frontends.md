---
{
    "title": "FRONTENDS",
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

## `frontends`

### Name

<version since="dev">

frontends

</version>

### description

Table-Value-Function, generate a temporary table named `frontends`. This tvf is used to view the information of BE nodes in the doris cluster.

This function is used in `FROM` clauses.

#### syntax

`frontends()`

The table schema of `frontends()` tvf：
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

The information displayed by the `frontends` tvf is basically consistent with the information displayed by the `show frontends` statement. However, the types of each field in the `frontends` tvf are more specific, and you can use the `frontends` tvf to perform operations such as filtering and joining.

The information displayed by the `frontends` tvf is authenticated, which is consistent with the behavior of `show frontends`, user must have ADMIN/OPERATOR privelege.

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