---
{
    "title": "SHOW-MTMV-JOB",
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

## SHOW-MTMV-JOBS

### Name

SHOW MTMV JOBS

### Description

该语句用于展示多表物化视图job列表。

语法：

```sql
SHOW MTMV JOBS
SHOW MTMV JOB FOR jobName
```

说明：

jobName: job的name

返回结果说明：

```sql
mysql> show mtmv jobs;
+-------+------------+-------------+-----------------------------------------+-----------+---------------------+---------+
| JobId | JobName    | ExecuteType | RecurringStrategy                       | JobStatus | CreateTime          | Comment |
+-------+------------+-------------+-----------------------------------------+-----------+---------------------+---------+
| 14091 | mtmv_14069 | MANUAL      | MANUAL TRIGGER                          | RUNNING   | 2023-11-22 21:02:22 | mv5     |
| 14045 | mtmv_14023 | RECURRING   | EVERY 1 WEEK STARTS 2023-11-22 21:01:29 | RUNNING   | 2023-11-22 21:01:29 | mv3     |
+-------+------------+-------------+-----------------------------------------+-----------+---------------------+---------+
2 rows in set (0.00 sec)
```

* JobId：JOB唯一ID.
* JobName：JOB名称.
* ExecuteType：JOB触发类型，RECURRING和MANUAL.
* RecurringStrategy： 调度策略
* JobStatus： job状态
* CreateTime： 创建时间
* Comment： 注释


### Example

1. 展示所有JOB

    ```sql
    SHOW MTMV JOBS;
    ```

2. 展示名称为`mtmv_14069`的job

    ```sql
    SHOW MTMV JOB FOR mtmv_14069;
    ```
   
### Keywords

    SHOW, MTMV, JOBS

### Best Practice

