---
{
    "title": "STOP-SYNC-JOB",
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

## STOP-SYNC-JOB

### Name

STOP SYNC JOB

### Description

通过 `job_name` 停止一个数据库内非停止状态的常驻数据同步作业。

语法:

```sql
STOP SYNC JOB [db.]job_name
```

### Example

1. 停止名称为 `job_name` 的数据同步作业

	```sql
	STOP SYNC JOB `job_name`;
	```

### Keywords

    STOP, SYNC, JOB

### Best Practice

