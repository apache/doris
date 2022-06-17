---
{
    "title": "SHOW-TABLETS",
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

## SHOW-TABLETS

### Name

SHOW TABLETS

### Description

该语句用于列出指定table的所有tablets（仅管理员使用）

语法：

```sql
SHOW TABLETS FROM [database.]table [PARTITIONS(p1,p2)]
[WHERE where_condition]
[ORDER BY col_name]
[LIMIT [offset,] row_count]
```
1. **Syntax Description:**

where_condition 可以为下列条件之一:
```
    Version = version
    state = "NORMAL|ROLLUP|CLONE|DECOMMISSION"
    BackendId = backend_id
```
或者通过`AND`组合的复合条件.

### Example

1. 列出指定table所有的tablets

    ```sql
    SHOW TABLETS FROM example_db.table_name;
    ````

2. 列出指定partitions的所有tablets

    ```sql
    SHOW TABLETS FROM example_db.table_name PARTITIONS(p1, p2);
    ````

3. 列出某个backend上状态为DECOMMISSION的tablets

    ```sql
    SHOW TABLETS FROM example_db.table_name WHERE state="DECOMMISSION" AND BackendId=11003;
    ````

### Keywords

    SHOW, TABLETS

### Best Practice

