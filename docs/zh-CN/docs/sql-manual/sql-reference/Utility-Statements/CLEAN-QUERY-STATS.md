---
{
    "title": "CLEAN-QUERY-STATS",
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

## CLEAN-QUERY-STATS

### Name

<version since="dev">
CLEAN QUERY STATS
</version>

### Description

该语句用请空查询统计信息

语法：

```sql
CLEAN [ALL| DATABASE | TABLE] QUERY STATS [[FOR db_name]|[FROM|IN] table_name]];
```

说明：

1. 如果指定 ALL，则清空所有查询统计信息，包括数据库和表的统计信息，需要admin 权限
2. 如果指定 DATABASE，则清空指定数据库的查询统计信息，需要对应database 的alter 权限
3. 如果指定 TABLE，则清空指定表的查询统计信息，需要对应表的alter 权限

### Example

1. 清空所有统计信息

    ```sql
    clean all query stats;
    ```

2. 清空指定数据库的统计信息

    ```sql
    clean database query stats for test_query_db;
    ```
3. 清空指定表的统计信息

    ```sql
    clean table query stats from test_query_db.baseall;
    ```

### Keywords

    CLEAN, QUERY, STATS

### Best Practice

