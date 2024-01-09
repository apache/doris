---
{
    "title": "TASKS",
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

## `tasks`

### Name

tasks

### description

表函数，生成任务临时表，可以查看某个任务类型中的task信息。

该函数用于 from 子句中。

#### syntax

`tasks("type"="")`

tasks("type"="mv")表结构：
```sql
mysql> desc function tasks("type"="mv");
+-----------------------+------+------+-------+---------+-------+
| Field                 | Type | Null | Key   | Default | Extra |
+-----------------------+------+------+-------+---------+-------+
| TaskId                | TEXT | No   | false | NULL    | NONE  |
| JobId                 | TEXT | No   | false | NULL    | NONE  |
| JobName               | TEXT | No   | false | NULL    | NONE  |
| MvId                  | TEXT | No   | false | NULL    | NONE  |
| MvName                | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseId          | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseName        | TEXT | No   | false | NULL    | NONE  |
| Status                | TEXT | No   | false | NULL    | NONE  |
| ErrorMsg              | TEXT | No   | false | NULL    | NONE  |
| CreateTime            | TEXT | No   | false | NULL    | NONE  |
| StartTime             | TEXT | No   | false | NULL    | NONE  |
| FinishTime            | TEXT | No   | false | NULL    | NONE  |
| DurationMs            | TEXT | No   | false | NULL    | NONE  |
| TaskContext           | TEXT | No   | false | NULL    | NONE  |
| RefreshMode           | TEXT | No   | false | NULL    | NONE  |
| NeedRefreshPartitions | TEXT | No   | false | NULL    | NONE  |
| CompletedPartitions   | TEXT | No   | false | NULL    | NONE  |
| Progress              | TEXT | No   | false | NULL    | NONE  |
+-----------------------+------+------+-------+---------+-------+
18 rows in set (0.00 sec)
```

* TaskId：task id
* JobId：job id
* JobName：job名称
* MvId：物化视图id
* MvName：物化视图名称
* MvDatabaseId：物化视图所属db id
* MvDatabaseName：物化视图所属db名称
* Status：task状态
* ErrorMsg：task失败信息
* CreateTime：task创建时间
* StartTime：task开始运行时间
* FinishTime：task结束运行时间
* DurationMs：task运行时间
* TaskContext：task运行参数
* RefreshMode：刷新模式
* NeedRefreshPartitions：本次task需要刷新的分区信息
* CompletedPartitions：本次task刷新完成的分区信息
* Progress：task运行进度

### example

1. 查看所有物化视图的task

```sql
mysql> select * from tasks("type"="mv");
```

2. 查看jobName为`inner_mtmv_75043`的所有task

```sql
mysql> select * from tasks("type"="mv") where JobName="inner_mtmv_75043";
```

### keywords

    tasks
