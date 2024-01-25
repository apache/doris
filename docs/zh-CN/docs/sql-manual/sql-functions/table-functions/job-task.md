---
{
    "title": "Tasks",
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

<version since="dev">

tasks

</version>

### description

表函数，生成 tasks 临时表，可以查看当前 doris 集群中的 job 产生的 tasks 信息。

该函数用于 from 子句中。

#### syntax

`tasks("type"="insert");`
**参数说明**

| 参数名  | 说明     | 类型     | 是否必填 |
|:-----|:-------|:-------|:-----|
| type | 作业类型   | string | 是    |

type 支持的类型：

- insert：insert into 类型的任务。

##### Insert tasks
`tasks("type"="insert");`表结构：
```
mysql> desc  function tasks("type"="insert");
+---------------+------+------+-------+---------+-------+
| Field         | Type | Null | Key   | Default | Extra |
+---------------+------+------+-------+---------+-------+
| TaskId        | TEXT | No   | false | NULL    | NONE  |
| JobId         | TEXT | No   | false | NULL    | NONE  |
| Label         | TEXT | No   | false | NULL    | NONE  |
| Status        | TEXT | No   | false | NULL    | NONE  |
| EtlInfo       | TEXT | No   | false | NULL    | NONE  |
| TaskInfo      | TEXT | No   | false | NULL    | NONE  |
| ErrorMsg      | TEXT | No   | false | NULL    | NONE  |
| CreateTimeMs  | TEXT | No   | false | NULL    | NONE  |
| FinishTimeMs  | TEXT | No   | false | NULL    | NONE  |
| TrackingUrl   | TEXT | No   | false | NULL    | NONE  |
| LoadStatistic | TEXT | No   | false | NULL    | NONE  |
| User          | TEXT | No   | false | NULL    | NONE  |
+---------------+------+------+-------+---------+-------+
12 rows in set (0.01 sec)
```

### example
```
mysql>  select * from tasks("type"="insert") limit 1 \G
*************************** 1. row ***************************
       TaskId: 667704038678903
        JobId: 10069
        Label: 10069_667704038678903
       Status: FINISHED
      EtlInfo: \N
     TaskInfo: cluster:N/A; timeout(s):14400; max_filter_ratio:0.0; priority:NORMAL
     ErrorMsg: \N
 CreateTimeMs: 2023-12-08 16:46:57
 FinishTimeMs: 2023-12-08 16:46:57
  TrackingUrl: 
LoadStatistic: {"Unfinished backends":{},"ScannedRows":0,"TaskNumber":0,"LoadBytes":0,"All backends":{},"FileNumber":0,"FileSize":0}
         User: root
1 row in set (0.05 sec)

```

### keywords

    tasks, insert