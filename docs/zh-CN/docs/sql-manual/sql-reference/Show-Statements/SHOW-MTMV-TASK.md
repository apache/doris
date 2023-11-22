---
{
    "title": "SHOW-MTMV-TASK",
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

## SHOW-MTMV-JOB-TASK

### Name

SHOW MTMV JOB TASK

### Description

该语句用于展示多表物化视图task列表。

语法：

```sql
SHOW MTMV JOB TASKS FOR jobName
```

说明：

jobName： task所属的job，每个job可能会有多个task

返回结果说明：

```sql
mysql> show mtmv job tasks for mtmv_14023\G
*************************** 1. row ***************************
       JobId: 14045
      TaskId: 61241503345280
      Status: SUCCESS
  CreateTime: 2023-11-22 21:01:29
   StartTime: 2023-11-22 21:01:29
  FinishTime: 2023-11-22 21:01:29
  DurationMS: 115
  ExecuteSql: INSERT OVERWRITE TABLE internal.zd.mv3 SELECT k1+1 as dd,k3 FROM user
    1 row in set (0.01 sec)
```

* JobId：JOB唯一ID.
* TaskId：TASK唯一ID.
* Status： task状态
* CreateTime： 创建时间
* StartTime： 运行开始时间
* FinishTime： 运行结束时间
* DurationMs： 耗时(ms)
* ExecuteSql： 任务执行的sql语句

### Example

1. 展示job `mtmv_14023` 的所有TASK

    ```sql
    show mtmv job tasks for mtmv_14023;
    ```
   
### Keywords

    SHOW, MTMV, TASKS, JOB

### Best Practice

