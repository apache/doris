---
{
    "title": "CREATE-VIEW",
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

## CREATE-VIEW

### Name

CREATE VIEW

### Description

该语句用于创建一个逻辑视图
语法：

```sql
CREATE VIEW [IF NOT EXISTS]
 [db_name.]view_name
 (column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt
```


说明：

- 视图为逻辑视图，没有物理存储。所有在视图上的查询相当于在视图对应的子查询上进行。
- query_stmt 为任意支持的 SQL

### Example

1. 在 example_db 上创建视图 example_view

    ```sql
    CREATE VIEW example_db.example_view (k1, k2, k3, v1)
    AS
    SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
    WHERE k1 = 20160112 GROUP BY k1,k2,k3;
    ```
    
2. 创建一个包含 comment 的 view

    ```sql
    CREATE VIEW example_db.example_view
    (
        k1 COMMENT "first key",
        k2 COMMENT "second key",
        k3 COMMENT "third key",
        v1 COMMENT "first value"
    )
    COMMENT "my first view"
    AS
    SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
    WHERE k1 = 20160112 GROUP BY k1,k2,k3;
    ```

### Keywords

    CREATE, VIEW

### Best Practice

