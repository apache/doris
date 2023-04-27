---
{
    "title": "ADMIN-REPAIR-TABLE",
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

## ADMIN-REPAIR-TABLE

### Name

ADMIN REPAIR TABLE

### Description

语句用于尝试优先修复指定的表或分区

语法：

```sql
ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)]
```

说明：

1. 该语句仅表示让系统尝试以高优先级修复指定表或分区的分片副本，并不保证能够修复成功。用户可以通过 ADMIN SHOW REPLICA STATUS 命令查看修复情况。
2. 默认的 timeout 是 14400 秒(4小时)。超时意味着系统将不再以高优先级修复指定表或分区的分片副本。需要重新使用该命令设置

### Example

1. 尝试修复指定表

        ADMIN REPAIR TABLE tbl1;

2. 尝试修复指定分区

        ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);

### Keywords

    ADMIN, REPAIR, TABLE

### Best Practice

