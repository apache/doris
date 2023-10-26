---
{
"title": "ALTER-WORKLOAD -GROUP",
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

## ALTER-WORKLOAD -GROUP

### Name

ALTER WORKLOAD GROUP 

<version since="2.0"></version>

### Description

该语句用于修改资源组。

语法：

```sql
ALTER WORKLOAD GROUP  "rg_name"
PROPERTIES (
    property_list
);
```

注意：

* 修改 memory_limit 属性时不可使所有 memory_limit 值的总和超过100%；
* 支持修改部分属性，例如只修改cpu_share的话，properties里只填cpu_share即可。

### Example

1. 修改名为 g1 的资源组：

    ```sql
    alter workload group g1
    properties (
        "cpu_share"="30",
        "memory_limit"="30%"
    );
    ```

### Keywords

```sql
ALTER, WORKLOAD , GROUP
```

### Best Practice
