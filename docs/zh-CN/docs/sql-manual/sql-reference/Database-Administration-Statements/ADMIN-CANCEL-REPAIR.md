---
{
    "title": "ADMIN-CANCEL-REPAIR",
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

## ADMIN-CANCEL-REPAIR

### Name

ADMIN CANCEL REPAIR

### Description

该语句用于取消以高优先级修复指定表或分区

语法：

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];
```

说明：

1. 该语句仅表示系统不再以高优先级修复指定表或分区的分片副本。系统仍会以默认调度方式修复副本。

### Example

 1. 取消高优先级修复

       ```sql
        ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
       ```

### Keywords

    ADMIN, CANCEL, REPAIR

### Best Practice

