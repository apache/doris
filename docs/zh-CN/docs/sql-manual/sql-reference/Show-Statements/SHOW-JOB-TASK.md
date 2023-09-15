---
{
"title": "SHOW-JOB-TASK",
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

## SHOW-JOB-TASK

### Name

SHOW JOB TASK

### Description

该语句用于展示 JOB 子任务的执行结果列表, 默认会保留最新的 20 条记录。

语法：

```sql
SHOW JOB TASKS FOR job_name;
```



结果说明：

```
                          JobId: JobId
                          TaskId: TaskId
                       StartTime: 开始执行时间
                         EndTime: 结束时间
                          Status: 状态
                          Result: 执行结果
                          ErrMsg: 错误信息
```

* State

        有以下 2 种 State：
        * SUCCESS
        * FAIL

### Example

1. 展示名称为 test1 的 JOB 的任务执行列表

    ```sql
    SHOW JOB TASKS FOR test1;
    ```
   
### Keywords

    SHOW, JOB, TASK

### Best Practice
