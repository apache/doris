---
{
    "title": "resource_groups",
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

## `resource_groups`

### Name

<version since="dev">

resource_groups

</version>

### description

Table-Value-Function, generate a temporary table named `resource_groups`. This tvf is used to view informations about current resource groups.

This function is used in `FROM` clauses.

grammar:

```
resource_groups();
```

The table schema of `resource_groups()` tvf:
```
mysql> desc function resource_groups();
+-------+-------------+------+-------+---------+-------+
| Field | Type        | Null | Key   | Default | Extra |
+-------+-------------+------+-------+---------+-------+
| Id    | BIGINT      | No   | false | NULL    | NONE  |
| Name  | VARCHAR(64) | No   | false | NULL    | NONE  |
| Item  | VARCHAR(64) | No   | false | NULL    | NONE  |
| Value | INT         | No   | false | NULL    | NONE  |
+-------+-------------+------+-------+---------+-------+
```

### example
```
mysql> select * from resource_groups()\G
+-------+------------+-----------+-------+
| Id    | Name       | Item      | Value |
+-------+------------+-----------+-------+
| 10076 | group_name | cpu_share |     1 |
| 10077 | group_test | cpu_share |    10 |
+-------+------------+-----------+-------+
```

### keywords

    resource_groups