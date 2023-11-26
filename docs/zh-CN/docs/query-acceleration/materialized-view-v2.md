---
{
    "title": "新物化视图",
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

# 新物化视图

物化视图是将预先计算（根据定义好的 SELECT 语句）好的数据集，存储在 Doris 中的一个特殊的表。

新物化视图的出现主要是为了打破老物化视图的限制，例如只可基于单一基表创建，且仅支持有限的聚合算子。

新物化视图底层是一张OLAP表，可以选择直接使用SELECT语句直接查询这张表，将来的版本也可以利用已经创建好的物化视图对基于基表的SELECT语句进行查询改写。

## 适用场景

- 查询加速。
- 简单的ETL。
- 湖仓加速。

## 使用物化视图

### 创建物化视图

物化视图支持多种刷新策略，

具体的语法可查看[CREATE MULTI TABLE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-MULTI-TABLE-MATERIALIZED-VIEW.md)

### 删除物化视图
物化视图有专门的删除语法，不能通过drop table来删除，

具体的语法可查看[DROP MULTI TABLE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-MULTI-TABLE-MATERIALIZED-VIEW.md)

### 修改物化视图

修改物化视图的名字，物化视图的刷新方式及物化视图特有的property可通过[ALTER MULTI TABLE MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-MULTI-TABLE-MATERIALIZED-VIEW.md)来修改

table相关的属性，例如副本数，仍通过`ALTER TABLE`相关的语法来修改

### 查看已创建的物化视图

物化视图独有的特性可以通过[mtmvs()](../sql-manual/sql-functions/table-functions/mtmvs.md)查看

和table相关的属性，仍通过[SHOW TABLES](../sql-manual/sql-reference/Show-Statements/SHOW-TABLES.md)来查看

### 手动刷新物化视图

物化视图有多种刷新方式，无论哪种方式，都可以随时进行手动刷新，

具体的语法可查看[REFRESH MATERIALIZED VIEW](../sql-manual/sql-reference/Utility-Statements/REFRESH-MATERIALIZED-VIEW.md)

### 查看物化视图刷新数据的job

每个物化视图底层都会默认创建一个job，用来定义物化视图的刷新逻辑，

具体的语法可查看[SHOW MTMV JOB](../sql-manual/sql-reference/Show-Statements/SHOW-MTMV-JOB.md)

备注：jobName 可通过[mtmvs()](../sql-manual/sql-functions/table-functions/mtmvs.md)查看

### 查看物化视图刷新数据的task

每个job可以有一个或多个task，用来记录物化视图的刷新记录及状态等信息

具体的语法可查看[SHOW MTMV TASK](../sql-manual/sql-reference/Show-Statements/SHOW-MTMV-TASK.md)

注意：每个物化视图目前最多只保存100条记录，超过之后，会自动删除最老的数据