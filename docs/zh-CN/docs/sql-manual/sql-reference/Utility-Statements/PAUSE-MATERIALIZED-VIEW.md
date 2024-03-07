---
{
    "title": "PAUSE-MATERIALIZED-VIEW",
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

## PAUSE-MATERIALIZED-VIEW

### Name

PAUSE MATERIALIZED VIEW

### Description

该语句用于暂停物化视图的定时调度

语法：

```sql
PAUSE MATERIALIZED VIEW JOB ON mvName=multipartIdentifier
```

### Example

1. 暂停物化视图mv1的定时调度

    ```sql
    PAUSE MATERIALIZED VIEW mv1;
    ```
   
### Keywords

    PAUSE, MATERIALIZED, VIEW

### Best Practice

