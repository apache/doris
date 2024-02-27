---
{
"title": "PAUSE-JOB",
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

## PAUSE-JOB

### Name

PAUSE JOB

### Description

用户暂停一个正在 RUNNING 状态的 JOB ，正在运行的 TASK 会被中断，JOB 状态变更为 PAUSED。 被停止的 JOB 可以通过 RESUME 操作恢复运行。

使用此命令需要 ADMIN 权限。

```sql
PAUSE JOB WHERE jobname= 'jobname';
```

### Example

1. 暂停名称为 example 的 JOB。

```sql
   PAUSE JOB where jobname='example';
```

### Keywords

    PAUSE, JOB

### Best Practice

