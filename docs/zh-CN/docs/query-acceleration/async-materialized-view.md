---
{
    "title": "异步物化视图",
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

# 物化视图

在数据仓库环境中，应用程序通常会对大型表执行复杂的查询。

一个例子是SELECT语句对包含数十亿行的表执行多表连接和聚合。就系统资源和计算结果所花费的时间而言，处理这些查询可能代价高昂。
物化视图提供了一种解决这些问题的方法。
物化视图包含基于对一个或多个基表的SQL查询而预先计算的结果集。
您可以发出SELECT语句来查询物化视图，与查询数据库中的其他表或视图的方式相同。Doris从物化视图返回预先计算的结果，不需要访问基表。
从用户的角度来看，与从基表检索相同的数据相比，查询结果的返回速度要快得多。

## 适用场景

- BI报表。

  物化视图对于加速可预测和重复的查询特别有用。应用程序不需要对大型表(如聚合或多个连接)执行资源密集型查询，而是可以查询物化视图并检索预先计算的结果集。
  例如，考虑使用一组查询来填充仪表板的场景。这个用例对于物化视图来说是理想的，因为查询是可预测的，并且是反复重复的。    

- 简单的ETL。

  可以利用物化视图对数据进行分层。

- Ad Hoc
  
  如果查询结果只包含基表中的少数行，或者查询结果需要大量的逻辑处理，例如半结构数据或花费大量时间的聚合。这时如果基表的数据变更不频繁，可以用物化视图来加速查询。

- 湖仓加速。

  查询是在外部catalog上进行的(例如存在hive catalog)，与查询本地数据库表相比，这可能会有较低的性能，这时可以通过建立物化视图，把外部数据存在Doris，来加速查询。

## 使用物化视图

### 创建物化视图

物化视图支持多种刷新策略，

具体的语法可查看[CREATE ASYNC MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-ASYNC-MATERIALIZED-VIEW.md)

### 删除物化视图
物化视图有专门的删除语法，不能通过drop table来删除，

具体的语法可查看[DROP ASYNC MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-ASYNC-MATERIALIZED-VIEW.md)

### 修改物化视图

修改物化视图的名字，物化视图的刷新方式及物化视图特有的property可通过[ALTER ASYNC MATERIALIZED VIEW](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-ASYNC-MATERIALIZED-VIEW.md)来修改

table相关的属性，例如副本数，仍通过`ALTER TABLE`相关的语法来修改

### 查看已创建的物化视图

物化视图独有的特性可以通过[mv_infos()](../sql-manual/sql-functions/table-functions/mv_infos.md)查看

和table相关的属性，仍通过[SHOW TABLES](../sql-manual/sql-reference/Show-Statements/SHOW-TABLES.md)来查看

### 手动刷新物化视图

物化视图有多种刷新方式，无论哪种方式，都可以随时进行手动刷新，

具体的语法可查看[REFRESH MATERIALIZED VIEW](../sql-manual/sql-reference/Utility-Statements/REFRESH-MATERIALIZED-VIEW.md)

### 查看物化视图刷新数据的job

每个物化视图底层都会默认创建一个job，用来定义物化视图的刷新逻辑，

具体的语法可查看[jobs("type"="mv")](../sql-manual/sql-functions/table-functions/jobs.md)

### 暂停物化视图job调度

可以暂停物化视图的定时调度

具体的语法可查看[PAUSE MATERIALIZED VIEW](../sql-manual/sql-reference/Utility-Statements/PAUSE-MATERIALIZED-VIEW.md)

### 恢复物化视图job调度

可以恢复物化视图的定时调度

具体的语法可查看[RESUME MATERIALIZED VIEW](../sql-manual/sql-reference/Utility-Statements/RESUME-MATERIALIZED-VIEW.md)

### 查看物化视图刷新数据的task

每个job可以有一个或多个task，用来记录物化视图的刷新记录及状态等信息

具体的语法可查看[tasks("type"="mv")](../sql-manual/sql-functions/table-functions/tasks.md)

注意：每个物化视图目前最多只保存100条记录，超过之后，会自动删除最老的数据