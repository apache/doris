---
{
    "title": "ALTER-VIEW",
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

## ALTER-VIEW

### Name

ALTER VIEW

### Description

该语句用于修改一个view的定义

语法：

```sql
ALTER VIEW
[db_name.]view_name
(column1[ COMMENT "col comment"][, column2, ...])
AS query_stmt
```

说明：

- 视图都是逻辑上的，其中的数据不会存储在物理介质上，在查询时视图将作为语句中的子查询，因此，修改视图的定义等价于修改query_stmt。
- query_stmt 为任意支持的 SQL 

### Example

1、修改example_db上的视图example_view

```sql
ALTER VIEW example_db.example_view
(
	c1 COMMENT "column 1",
	c2 COMMENT "column 2",
	c3 COMMENT "column 3"
)
AS SELECT k1, k2, SUM(v1) FROM example_table 
GROUP BY k1, k2
```

### Keywords

```text
ALTER, VIEW
```

### Best Practice

