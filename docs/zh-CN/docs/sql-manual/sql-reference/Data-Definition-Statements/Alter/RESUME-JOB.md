---
{
"title": "RESUME-JOB",
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

## RESUME-JOB

### Name

RESUME JOB

### Description

用于重启一个 PAUSE 状态的 JOB 作业。重启的作业，将继续按照周期执行。STOP 状态的 JOB 无法被恢复。

```sql
RESUME JOB FOR job_name;
```

### Example

1. 重启名称为 test1 的 JOB。

   ```sql
   RESUME JOB FOR test1;
   ```

### Keywords

       RESUME, JOB

### Best Practice

