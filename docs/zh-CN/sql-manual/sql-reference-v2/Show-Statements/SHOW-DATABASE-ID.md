---
{
    "title": "SHOW-DATABASE-ID",
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

## SHOW-DATABASE-ID

### Name

SHOW DATABASE ID

### Description

该语句用于根据 database id 查找对应的 database name（仅管理员使用）

语法：

```sql
SHOW DATABASE [database_id]
```

### Example

1. 根据 database id 查找对应的 database name
   
    ```sql
    SHOW DATABASE 1001;
    ```

### Keywords

    SHOW, DATABASE, ID

### Best Practice

