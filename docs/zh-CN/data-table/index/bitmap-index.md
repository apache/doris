---
{
    "title": "Bitmap 索引",
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

# Bitmap 索引

用户可以通过创建bitmap index 加速查询 本文档主要介绍如何创建 index 作业，以及创建 index 的一些注意事项和常见问题。

## 名词解释

- bitmap index：位图索引，是一种快速数据结构，能够加快查询速度

## 原理介绍

创建和删除本质上是一个 schema change 的作业，具体细节可以参照 [Schema Change](../../advanced/alter-table/schema-change.html)。

## 语法

### 创建索引

在table1 上为siteid 创建bitmap 索引

```sql
CREATE INDEX [IF NOT EXISTS] index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
```

### 查看索引

展示指定 table_name 的下索引

```sql
SHOW INDEX FROM example_db.table_name;
```

### 删除索引

展示指定 table_name 的下索引

```sql
DROP INDEX [IF EXISTS] index_name ON [db_name.]table_name;
```

## 注意事项

- 目前索引仅支持 bitmap 类型的索引。
- bitmap 索引仅在单列上创建。
- bitmap 索引能够应用在 `Duplicate` 数据模型的所有列和 `Aggregate`, `Uniq` 模型的key列上。
- bitmap 索引支持的数据类型如下:
  - `TINYINT`
  - `SMALLINT`
  - `INT`
  - `UNSIGNEDINT`
  - `BIGINT`
  - `CHAR`
  - `VARCHAR`
  - `DATE`
  - `DATETIME`
  - `LARGEINT`
  - `DECIMAL`
  - `BOOL`
- bitmap索引仅在 Segment V2 下生效。当创建 index 时，表的存储格式将默认转换为 V2 格式。

## 更多帮助

关于 bitmap索引 使用的更多详细语法及最佳实践，请参阅 [CREARE INDEX](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-INDEX.md) / [SHOW INDEX](../../sql-manual/sql-reference/Show-Statements/SHOW-INDEX.html) / [DROP INDEX](../../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-INDEX.html) 命令手册，你也可以在 MySql 客户端命令行下输入 `HELP CREATE INDEX` /  `HELP SHOW INDEX` / `HELP DROP INDEX`。
