---
{
    "title": "SHOW-WORKLOAD-GROUPS",
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

## SHOW-WORKLOAD-GROUPS

### Name

SHOW WORKLOAD GROUPS

<version since="2.0"></version>

### Description

该语句用于展示当前用户具有usage_priv权限的资源组。

语法：

```sql
SHOW WORKLOAD GROUPS;
```

说明：

该语句仅做资源组简单展示，更复杂的展示可参考 tvf workload_groups().

### Example

1. 展示所有资源组：
    
    ```sql
    mysql> show workload groups;
    +----------+--------+--------------------------+---------+
    | Id       | Name   | Item                     | Value   |
    +----------+--------+--------------------------+---------+
    | 10343386 | normal | cpu_share                | 10      |
    | 10343386 | normal | memory_limit             | 30%     |
    | 10343386 | normal | enable_memory_overcommit | true    |
    | 10352416 | g1     | memory_limit             | 20%     |
    | 10352416 | g1     | cpu_share                | 10      |
    +----------+--------+--------------------------+---------+
    ```

### Keywords

    SHOW, WORKLOAD, GROUPS, GROUP

### Best Practice
