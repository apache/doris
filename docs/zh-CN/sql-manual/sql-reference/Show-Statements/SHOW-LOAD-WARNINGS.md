---
{
    "title": "SHOW-LOAD-WARNINGS",
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

## SHOW-LOAD-WARNINGS

### Name

SHOW LOAD WARNINGS

### Description

如果导入任务失败且错误信息为 `ETL_QUALITY_UNSATISFIED`，则说明存在导入质量问题, 如果想看到这些有质量问题的导入任务，改语句就是完成这个操作的。

语法：

```sql
SHOW LOAD WARNINGS
[FROM db_name]
[
   WHERE
   [LABEL [ = "your_label" ]]
   [LOAD_JOB_ID = ["job id"]]
]
```

1) 如果不指定 db_name，使用当前默认db
1) 如果使用 LABEL = ，则精确匹配指定的 label
1) 如果指定了 LOAD_JOB_ID，则精确匹配指定的 JOB ID

### Example

1. 展示指定 db 的导入任务中存在质量问题的数据，指定 label 为 "load_demo_20210112" 

   ```sql
   SHOW LOAD WARNINGS FROM demo WHERE LABEL = "load_demo_20210112" 
   ```

### Keywords

    SHOW, LOAD, WARNINGS

### Best Practice

