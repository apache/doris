---
{
    "title": "ADMIN-SET-TABLE-STATUS",
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

## ADMIN-SET-TABLE-STATUS

### Name

ADMIN SET TABLE STATUS

### Description

该语句用于设置指定表的状态，仅支持OLAP表。

该命令目前仅用于手动将 OLAP 表状态设置为指定状态，从而使得某些由于表状态被阻碍的任务能够继续运行。

语法：

```sql
ADMIN SET TABLE table_name STATUS
        PROPERTIES ("key" = "value", ...);
```

目前支持以下属性：

1. "state"：必需。指定一个目标状态，将会修改 OLAP 表的状态至此状态。

> 当前可修改的目标状态包括：
> 
> 1. NORMAL
> 2. ROLLUP
> 3. SCHEMA_CHANGE
> 4. BACKUP
> 5. RESTORE
> 6. WAITING_STABLE
> 
> 如果表的状态已经是指定的状态，则会被忽略。

**注意：此命令一般只用于紧急故障修复，请谨慎操作。**

### Example

1. 设置表 tbl1 的状态为 NORMAL。

```sql
admin set table tbl1 status properties("state" = "NORMAL");
```

2. 设置表 tbl2 的状态为 SCHEMA_CHANGE。

```sql
admin set table test_set_table_status status properties("state" = "SCHEMA_CHANGE");
```

### Keywords

    ADMIN, SET, TABLE, STATUS

### Best Practice



