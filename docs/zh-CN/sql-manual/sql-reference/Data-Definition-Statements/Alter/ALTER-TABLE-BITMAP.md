---
{
    "title": "ALTER-TABLE-BITMAP",
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

## ALTER-TABLE-BITMAP

### Name

ALTER  TABLE  BITMAP

### Description

该语句用于对已有 table 进行 bitmap index 操作。

语法：

```sql
ALTER TABLE [database.]table alter_clause;
```

bitmap index 的 alter_clause 支持如下几种修改方式

1. 创建bitmap 索引

语法：

```sql
ADD INDEX [IF NOT EXISTS] index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
```

注意：

- 目前仅支持bitmap 索引
- BITMAP 索引仅在单列上创建

2. 删除索引

语法：

```sql
DROP INDEX [IF EXISTS] index_name；
```

### Example

1. 在table1 上为siteid 创建bitmap 索引

```sql
ALTER TABLE table1 ADD INDEX [IF NOT EXISTS] index_name (siteid) [USING BITMAP] COMMENT 'balabala';
```

2. 删除table1 上的siteid列的bitmap 索引

```sql
ALTER TABLE table1 DROP INDEX [IF EXISTS] index_name;
```

### Keywords

```text
ALTER, TABLE, BITMAP
```

### Best Practice

