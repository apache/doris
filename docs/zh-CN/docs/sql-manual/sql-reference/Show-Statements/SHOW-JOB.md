---
{
"title": "SHOW-JOB",
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

## SHOW-JOB

### Name

SHOW JOB

### Description

该语句用于展示 JOB 作业运行状态

语法：

```sql
SHOW JOBS|JOB FOR job_name;
```

SHOW JOBS 用于展示当前 DB 下所有作业的运行状态，SHOW JOB FOR job_name 用于展示指定作业的运行状态。

结果说明：

```
                       Id: JobId
                       Db: 数据库名称
                     Name: Job名称
                  Definer: 创建用户
                 TimeZone: 时区
              ExecuteType: RECURRING 表示循环调度，即使用 every 语句指定的调度时间，ONCE_TIME 表示一次性任务。
                ExecuteAT: ONCE_TIME 任务的执行开始时间
          ExecuteInterval: 周期调度任务的间隔
          ExecuteInterval: 周期调度任务的时间间隔单位
                   STARTS: 周期调度任务设置的开始时间
                     ENDS: 周期调度任务设置的结束时间
                   Status: Job 状态
    LastExecuteFinishTime: 上一次执行完成时间
                 ErrorMsg: 错误信息
                  Comment: 备注


```

* State

        有以下5种State：
        * RUNNING：运行中
        * PAUSED：暂停
        * STOPPED：结束（用户手动触发）
        * FINISHED: 完成

### Example

1. 展示当前 DB 下的所有 JOB。

    ```sql
    SHOW JOBS;
    ```

2. 展示名称为 test1 的 JOB

    ```sql
    SHOW JOB FOR test1;
    ```
   
### Keywords

    SHOW, JOB

### Best Practice
