---
{
    "title": "ALTER-TABLE-RENAME",
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

## ALTER-TABLE-RENAME

### Name

ALTER TABLE RENAME

### Description

该语句用于对已有 table 属性的某些名称进行重命名操作。这个操作是同步的，命令返回表示执行完毕。

语法：

```sql
ALTER TABLE [database.]table alter_clause;
```

rename 的 alter_clause 支持对以下名称进行修改

1. 修改表名

语法：

```sql
RENAME new_table_name;
```

2. 修改 rollup index 名称

 语法：

```sql
RENAME ROLLUP old_rollup_name new_rollup_name;
```

3. 修改 partition 名称

语法：

```sql
RENAME PARTITION old_partition_name new_partition_name;    
```

4.  修改 column 名称

<version since="1.2">
  
修改 column 名称

</version>

语法：

```sql
RENAME COLUMN old_column_name new_column_name;    
```

注意：
- 建表时需要在property中设置light_schema_change=true


### Example

1. 将名为 table1 的表修改为 table2

```sql
ALTER TABLE table1 RENAME table2;
```

2. 将表 example_table 中名为 rollup1 的 rollup index 修改为 rollup2

```sql
ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
```

3. 将表 example_table 中名为 p1 的 partition 修改为 p2

```sql
ALTER TABLE example_table RENAME PARTITION p1 p2;
```

4. 将表 example_table 中名为 c1 的 column 修改为 c2

```sql
ALTER TABLE example_table RENAME COLUMN c1 c2;
```

### Keywords

```text
ALTER, TABLE, RENAME, ALTER TABLE
```

### Best Practice

