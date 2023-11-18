---
{
    "title": "WORKLOAD_GROUPS",
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

## `workload_groups`

### Name

<version since="dev">

workload_groups

</version>

### description

Table-Value-Function, generate a temporary table named `workload_groups`. This tvf is used to view information about workload groups for which current user has permission.

This function is used in `FROM` clauses.

#### syntax

`workload_groups()`

The table schema of `workload_groups()` tvf:
```
mysql> desc function workload_groups();
+-------+-------------+------+-------+---------+-------+
| Field | Type        | Null | Key   | Default | Extra |
+-------+-------------+------+-------+---------+-------+
| Id    | BIGINT      | No   | false | NULL    | NONE  |
| Name  | STRING      | No   | false | NULL    | NONE  |
| Item  | STRING      | No   | false | NULL    | NONE  |
| Value | STRING      | No   | false | NULL    | NONE  |
+-------+-------------+------+-------+---------+-------+
```

### example
```
mysql> select * from workload_groups()\G
+-------+--------+--------------+-------+
| Id    | Name   | Item         | Value |
+-------+--------+--------------+-------+
| 11001 | normal | memory_limit | 100%  |
| 11001 | normal | cpu_share    | 10    |
+-------+--------+--------------+-------+
```

### keywords

    workload_groups