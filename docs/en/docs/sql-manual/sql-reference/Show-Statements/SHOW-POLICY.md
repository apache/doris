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
SHOW ROW POLICY [FOR user| ROLE role]
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

3. specify role name query
    
    ```sql
    mysql> SHOW ROW POLICY for role role1;
    +------------+--------+-----------+------+-------------+----------------+------+-------+----------------------------------------------------------------------------------+
    | PolicyName | DbName | TableName | Type | FilterType  | WherePredicate | User | Role  | OriginStmt                                                                       |
    +------------+--------+-----------+------+-------------+----------------+------+-------+----------------------------------------------------------------------------------+
    | zdtest1    | zd     | user      | ROW  | RESTRICTIVE | `user_id` = 1  | NULL | role1 | create row policy zdtest1 on user as restrictive to role role1 using (user_id=1) |
    +------------+--------+-----------+------+-------------+----------------+------+-------+----------------------------------------------------------------------------------+
    1 row in set (0.01 sec)
   ```

4. demonstrate data migration strategies
    ```sql
    mysql> SHOW STORAGE POLICY;
    +---------------------+---------+-----------------------+---------------------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | PolicyName          | Type    | StorageResource       | CooldownDatetime    | CooldownTtl | properties                                                                                                                                                                                                                                                                                                          |
    +---------------------+---------+-----------------------+---------------------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | showPolicy_1_policy | STORAGE | showPolicy_1_resource | 2022-06-08 00:00:00 | -1          | {
    "type": "s3",
    "s3.endpoint": "bj.s3.comaaaa",
    "s3.region": "bj",
    "s3.access_key": "bbba",
    "s3.secret_key": "******",
    "s3.root.path": "path/to/rootaaaa",
    "s3.bucket": "test-bucket",
    "s3.connection.request.timeout": "3000"
    "3.connection.maximum": "50",
    "s3.connection.timeout": "1000",
    } |
    +---------------------+---------+-----------------------+---------------------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    1 row in set (0.00 sec)
    ```
        

### Keywords

    SHOW, POLICY

### Best Practice

