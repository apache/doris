---
{
    "title": "JOBS",
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

## `jobs`

### Name

jobs

### description

表函数，生成任务临时表，可以查看某个任务类型中的job信息。

该函数用于 from 子句中。

#### syntax

`jobs("type"="")`

jobs("type"="mv")表结构：
```sql
mysql> desc function jobs("type"="mv");
+-------------------+------+------+-------+---------+-------+
| Field             | Type | Null | Key   | Default | Extra |
+-------------------+------+------+-------+---------+-------+
| Id                | TEXT | No   | false | NULL    | NONE  |
| Name              | TEXT | No   | false | NULL    | NONE  |
| MvId              | TEXT | No   | false | NULL    | NONE  |
| MvName            | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseId      | TEXT | No   | false | NULL    | NONE  |
| MvDatabaseName    | TEXT | No   | false | NULL    | NONE  |
| ExecuteType       | TEXT | No   | false | NULL    | NONE  |
| RecurringStrategy | TEXT | No   | false | NULL    | NONE  |
| Status            | TEXT | No   | false | NULL    | NONE  |
| CreateTime        | TEXT | No   | false | NULL    | NONE  |
+-------------------+------+------+-------+---------+-------+
10 rows in set (0.00 sec)
```

* Id：job id.
* Name：job名称.
* MvId：物化视图id
* MvName：物化视图名称
* MvDatabaseId：物化视图所属db id
* MvDatabaseName：物化视图所属db名称
* ExecuteType：执行类型
* RecurringStrategy：循环策略
* Status：job状态
* CreateTime：task创建时间

### example

1. 查看所有物化视图的job

```sql
mysql> select * from jobs("type"="mv");
```

2. 查看name为`inner_mtmv_75043`的job

```sql
mysql> select * from jobs("type"="mv") where Name="inner_mtmv_75043";
```

### keywords

    jobs
