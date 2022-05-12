---
{
    "title": "SHOW-ROW-POLICY",
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

## SHOW-POLICY

### Name

SHOW ROW POLICY

### Description

查看当前 DB 下的行安全策略

语法：

```sql
SHOW ROW POLICY [FOR user]
```

### Example

1. 查看所有安全策略。

    ```sql
    mysql> SHOW ROW POLICY;
    +-------------------+----------------------+-----------+------+-------------+-------------------+------+-------------------------------------------------------------------------------------------------------------------------------------------+
    | PolicyName        | DbName               | TableName | Type | FilterType  | WherePredicate    | User | OriginStmt                                                                                                                                |
    +-------------------+----------------------+-----------+------+-------------+-------------------+------+-------------------------------------------------------------------------------------------------------------------------------------------+
    | test_row_policy_1 | default_cluster:test | table1    | ROW  | RESTRICTIVE | `id` IN (1, 2)    | root | /* ApplicationName=DataGrip 2021.3.4 */ CREATE ROW POLICY test_row_policy_1 ON test.table1 AS RESTRICTIVE TO root USING (id in (1, 2));
    |
    | test_row_policy_2 | default_cluster:test | table1    | ROW  | RESTRICTIVE | `col1` = 'col1_1' | root | /* ApplicationName=DataGrip 2021.3.4 */ CREATE ROW POLICY test_row_policy_2 ON test.table1 AS RESTRICTIVE TO root USING (col1='col1_1');
    |
    +-------------------+----------------------+-----------+------+-------------+-------------------+------+-------------------------------------------------------------------------------------------------------------------------------------------+
    2 rows in set (0.00 sec)
    ```
    
2. 指定用户名查询

    ```sql
    mysql> SHOW ROW POLICY FOR test;
    +-------------------+----------------------+-----------+------+------------+-------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------+
    | PolicyName        | DbName               | TableName | Type | FilterType | WherePredicate    | User                 | OriginStmt                                                                                                                               |
    +-------------------+----------------------+-----------+------+------------+-------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------+
    | test_row_policy_3 | default_cluster:test | table1    | ROW  | PERMISSIVE | `col1` = 'col1_2' | default_cluster:test | /* ApplicationName=DataGrip 2021.3.4 */ CREATE ROW POLICY test_row_policy_3 ON test.table1 AS PERMISSIVE TO test USING (col1='col1_2');
    |
    +-------------------+----------------------+-----------+------+------------+-------------------+----------------------+------------------------------------------------------------------------------------------------------------------------------------------+
    1 row in set (0.01 sec)
    ```
    

### Keywords

    SHOW, POLICY

### Best Practice

