---
{
    "title": "SHOW-CREATE-REPOSITORY",
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

## SHOW-CREATE-REPOSITORY

### Name

SHOW CREATE REPOSITORY

### Description

该语句用于展示仓库的创建语句.

语法：

```sql
SHOW CREATE REPOSITORY for repository_name;
```

说明：
- `repository_name`: 仓库名称

### Example

1. 展示指定仓库的创建语句

   ```sql
   SHOW CREATE REPOSITORY for test_repository
   ```

### Keywords

    SHOW, CREATE, REPOSITORY

### Best Practice

