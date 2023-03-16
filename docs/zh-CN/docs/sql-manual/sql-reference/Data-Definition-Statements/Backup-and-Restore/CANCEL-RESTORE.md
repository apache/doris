---
{
    "title": "CANCEL-RESTORE",
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

## CANCEL-RESTORE

### Name

CANCEL  RESTORE

### Description

该语句用于取消一个正在进行的 RESTORE 任务。

语法：

```sql
CANCEL RESTORE FROM db_name;
```

注意：

- 当取消处于 COMMIT 或之后阶段的恢复左右时，可能导致被恢复的表无法访问。此时只能通过再次执行恢复作业进行数据恢复。 

### Example

1. 取消 example_db 下的 RESTORE 任务。

```sql
CANCEL RESTORE FROM example_db;
```

### Keywords

    CANCEL, RESTORE

### Best Practice

