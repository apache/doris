---
{
    "title": "SHOW-CREATE-LOAD",
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

## SHOW-CREATE-LOAD

### Name

SHOW CREATE LOAD

### Description

该语句用于展示导入作业的创建语句.

语法：

```sql
SHOW CREATE LOAD for load_name;
```

说明：
          1.  `load_name`: 例行导入作业名称

### Example

1. 展示默认db下指定导入作业的创建语句

   ```sql
   SHOW CREATE LOAD for test_load
   ```

### Keywords

    SHOW, CREATE, LOAD

### Best Practice

