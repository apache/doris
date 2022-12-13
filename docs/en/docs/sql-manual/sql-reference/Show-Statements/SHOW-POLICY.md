---
{
    "title": "SHOW-POLICY",
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

## SHOW-POLICY

### Name

SHOW ROW POLICY

### Description

View the row security policy under the current DB

```sql
SHOW ROW POLICY [FOR user]
```

### Example

1. view all security policies.

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
    
2. specify user name query

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

3. demonstrate data migration strategies
    ```sql
    mysql> SHOW STORAGE POLICY;
    +---------------------+---------+-----------------------+---------------------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | PolicyName          | Type    | StorageResource       | CooldownDatetime    | CooldownTtl | properties                                                                                                                                                                                                                                                                                                          |
    +---------------------+---------+-----------------------+---------------------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | showPolicy_1_policy | STORAGE | showPolicy_1_resource | 2022-06-08 00:00:00 | -1          | {
    "AWS_SECRET_KEY": "******",
    "AWS_REGION": "bj",
    "AWS_ACCESS_KEY": "bbba",
    "AWS_MAX_CONNECTIONS": "50",
    "AWS_CONNECTION_TIMEOUT_MS": "1000",
    "type": "s3",
    "AWS_ROOT_PATH": "path/to/rootaaaa",
    "AWS_BUCKET": "test-bucket",
    "AWS_ENDPOINT": "bj.s3.comaaaa",
    "AWS_REQUEST_TIMEOUT_MS": "3000"
    } |
    +---------------------+---------+-----------------------+---------------------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    1 row in set (0.00 sec)
    ```
        

### Keywords

    SHOW, POLICY

### Best Practice

