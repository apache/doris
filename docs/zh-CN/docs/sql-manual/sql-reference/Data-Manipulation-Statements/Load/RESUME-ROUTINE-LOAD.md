---
{
    "title": "RESUME-ROUTINE-LOAD",
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

## RESUME-ROUTINE-LOAD

### Name

RESUME ROUTINE LOAD

### Description

用于重启一个被暂停的 Routine Load 作业。重启的作业，将继续从之前已消费的 offset 继续消费。

```sql
RESUME [ALL] ROUTINE LOAD FOR job_name
```

### Example

1. 重启名称为 test1 的例行导入作业。

   ```sql
   RESUME ROUTINE LOAD FOR test1;
   ```

2. 重启所有例行导入作业。

   ```sql
   RESUME ALL ROUTINE LOAD;
   ```

### Keywords

    RESUME, ROUTINE, LOAD

### Best Practice

