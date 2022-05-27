---
{
    "title": "PAUSE-SYNC-JOB",
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

## PAUSE-SYNC-JOB

### Name

PAUSE SYNC JOB

### Description

通过 `job_name` 暂停一个数据库内正在运行的常驻数据同步作业，被暂停的作业将停止同步数据，保持消费的最新位置，直到被用户恢复。

语法：

```sql
PAUSE SYNC JOB [db.]job_name
```

### Example

1. 暂停名称为 `job_name` 的数据同步作业。

   ```sql
   PAUSE SYNC JOB `job_name`;
   ```

### Keywords

    PAUSE, SYNC, JOB

### Best Practice

