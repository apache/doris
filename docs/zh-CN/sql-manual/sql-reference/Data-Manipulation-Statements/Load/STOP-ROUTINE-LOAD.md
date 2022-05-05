---
{
    "title": "STOP-ROUTINE-LOAD",
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

## STOP-ROUTINE-LOAD

### Name

STOP ROUTINE LOAD

### Description

用户停止一个 Routine Load 作业。被停止的作业无法再重新运行。

```sql
STOP ROUTINE LOAD FOR job_name;
```

### Example

1. 停止名称为 test1 的例行导入作业。

   ```sql
   STOP ROUTINE LOAD FOR test1;
   ```

### Keywords

    STOP, ROUTINE, LOAD

### Best Practice

