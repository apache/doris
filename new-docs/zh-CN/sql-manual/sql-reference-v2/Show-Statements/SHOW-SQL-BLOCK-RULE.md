---
{
    "title": "SHOW-SQL-BLOCK-RULE",
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

## SHOW-SQL-BLOCK-RULE

### Name

SHOW SQL  BLOCK RULE

### Description

查看已配置的SQL阻止规则，不指定规则名则为查看所有规则。

语法：

```sql
SHOW SQL_BLOCK_RULE [FOR RULE_NAME];
```

### Example

1. 查看所有规则。

    ```sql
    mysql> SHOW SQL_BLOCK_RULE;
    +------------+------------------------+---------+--------------+-----------+-------------+--------+--------+
    | Name       | Sql                    | SqlHash | PartitionNum | TabletNum | Cardinality | Global | Enable |
    +------------+------------------------+---------+--------------+-----------+-------------+--------+--------+
    | test_rule  | select * from order_analysis | NULL    | 0            | 0         | 0           | true   | true   |
    | test_rule2 | NULL                   | NULL    | 30           | 0         | 10000000000 | false  | true   |
    +------------+------------------------+---------+--------------+-----------+-------------+--------+--------+
    2 rows in set (0.01 sec)
    ```
    
2. 制定规则名查询

    ```sql
    mysql> SHOW SQL_BLOCK_RULE FOR test_rule2;
    +------------+------+---------+--------------+-----------+-------------+--------+--------+
    | Name       | Sql  | SqlHash | PartitionNum | TabletNum | Cardinality | Global | Enable |
    +------------+------+---------+--------------+-----------+-------------+--------+--------+
    | test_rule2 | NULL | NULL    | 30           | 0         | 10000000000 | false  | true   |
    +------------+------+---------+--------------+-----------+-------------+--------+--------+
    1 row in set (0.00 sec)
    
    ```
    

### Keywords

    SHOW, SQL_BLOCK_RULE

### Best Practice

