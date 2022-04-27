---
{
    "title": "ALTER-TABLE-ROLLUP",
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

## ALTER-TABLE-ROLLUP

### Name

ALTER TABLE ROLLUP

### Description

该语句用于对已有 table 进行 rollup 进行修改操作。rollup 是异步操作，任务提交成功则返回，之后可使用[SHOW ALTER](../../Show-Statements/SHOW-ALTER.html) 命令查看进度。

语法：

```sql
ALTER TABLE [database.]table alter_clause;
```

rollup 的 alter_clause 支持如下几种创建方式

1. 创建 rollup index

语法：

```sql
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)]
```

2. 批量创建 rollup index

语法：

```sql
ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
                    [FROM from_index_name]
                    [PROPERTIES ("key"="value", ...)],...]
```

注意：

- 如果没有指定 from_index_name，则默认从 base index 创建
- rollup 表中的列必须是 from_index 中已有的列
- 在 properties 中，可以指定存储格式。具体请参阅 [CREATE TABLE](../Create/CREATE-TABLE.html#create-table)

3. 删除 rollup index

 语法：

```sql
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)]
```

4. 批量删除 rollup index

语法：

```sql
DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...]
```

注意：

- 不能删除 base index

### Example

1. 创建 index: example_rollup_index，基于 base index（k1,k2,k3,v1,v2）。列式存储。

```sql
ALTER TABLE example_db.my_table
ADD ROLLUP example_rollup_index(k1, k3, v1, v2);
```

2. 创建 index: example_rollup_index2，基于 example_rollup_index（k1,k3,v1,v2）

```sql
ALTER TABLE example_db.my_table
ADD ROLLUP example_rollup_index2 (k1, v1)
FROM example_rollup_index;
```

3. 创建 index: example_rollup_index3, 基于 base index (k1,k2,k3,v1), 自定义 rollup 超时时间一小时。

```sql
ALTER TABLE example_db.my_table
ADD ROLLUP example_rollup_index(k1, k3, v1)
PROPERTIES("timeout" = "3600");
```

4. 删除 index: example_rollup_index2

```sql
ALTER TABLE example_db.my_table
DROP ROLLUP example_rollup_index2;
```

### Keywords

```text
ALTER, TABLE, ROLLUP
```

### Best Practice

