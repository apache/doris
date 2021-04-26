---
{
    "title": "ADMIN SHOW CONFIG",
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

# ADMIN SHOW CONFIG
## Description

This statement is used to show the configuration of the current cluster (currently only supporting the display of FE configuration items)

Grammar:

ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];

Explain:

The implications of the results are as follows:
1. Key: Configuration item name
2. Value: Configuration item value
3. Type: Configuration item type
4. IsMutable: 是否可以通过 ADMIN SET CONFIG 命令设置
5. MasterOnly: 是否仅适用于 Master FE
6. Comment: Configuration Item Description

## example

1. View the configuration of the current FE node

ADMIN SHOW FRONTEND CONFIG;

2. Search for a configuration of the current Fe node with like predicate

mysql> ADMIN SHOW FRONTEND CONFIG LIKE '%check_java_version%';
+--------------------+-------+---------+-----------+------------+---------+
| Key                | Value | Type    | IsMutable | MasterOnly | Comment |
+--------------------+-------+---------+-----------+------------+---------+
| check_java_version | true  | boolean | false     | false      |         |
+--------------------+-------+---------+-----------+------------+---------+
1 row in set (0.00 sec)

## keyword
ADMIN,SHOW,CONFIG
