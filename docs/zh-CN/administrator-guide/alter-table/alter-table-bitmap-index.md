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
用户可以通过创建bitmap index 加速查询
本文档主要介绍如何创建 index 作业，以及创建 index 的一些注意事项和常见问题。

## 名词解释
* bitmap index：位图索引，是一种快速数据结构，能够加快查询速度

## 原理介绍
创建和删除本质上是一个 schema change 的作业，具体细节可以参照 [Schema Change](alter-table-schema-change)。

## 语法
index 创建和修改相关语法有两种形式，一种集成与 alter table 语句中，另一种是使用单独的 
create/drop index 语法
1. 创建索引

    创建索引的的语法可以参见 [CREATE INDEX](../../sql-reference/sql-statements/Data%20Definition/CREATE%20INDEX.html) 
    或 [ALTER TABLE](../../sql-reference/sql-statements/Data%20Definition/ALTER%20TABLE.html) 中bitmap 索引相关的操作,
    也可以通过在创建表时指定bitmap 索引，参见[CREATE TABLE](../../sql-reference/sql-statements/Data%20Definition/CREATE%20TABLE.html)

2. 查看索引

    参照[SHOW INDEX](../../sql-reference/sql-statements/Administration/SHOW%20INDEX.html)

3. 删除索引

    参照[DROP INDEX](../../sql-reference/sql-statements/Data%20Definition/DROP%20INDEX.html)
    或者 [ALTER TABLE](../../sql-reference/sql-statements/Data%20Definition/ALTER%20TABLE.html) 中bitmap 索引相关的操作

## 创建作业
参照 schema change 文档 [Schema Change](alter-table-schema-change.html)

## 查看作业
参照 schema change 文档 [Schema Change](alter-table-schema-change.html)

## 取消作业
参照 schema change 文档 [Schema Change](alter-table-schema-change.html)

## 注意事项
* 目前索引仅支持 bitmap 类型的索引。 
* bitmap 索引仅在单列上创建。
* bitmap 索引能够应用在 `Duplicate` 数据模型的所有列和 `Aggregate`, `Uniq` 模型的key列上。
* bitmap 索引支持的数据类型如下:
    * `TINYINT`
    * `SMALLINT`
    * `INT`
    * `UNSIGNEDINT`
    * `BIGINT`
    * `CHAR`
    * `VARCHAR`
    * `DATE`
    * `DATETIME`
    * `LARGEINT`
    * `DECIMAL`
    * `BOOL`

* bitmap索引仅在 Segment V2 下生效。当创建 index 时，表的存储格式将默认转换为 V2 格式。
