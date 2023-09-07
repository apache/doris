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

This statement is used to set the state of the specified table. Only supports OLAP tables.

This command is currently only used to manually set the OLAP table state to the specified state, allowing some jobs that are stuck by the table state to continue running.

grammar:

```sql
ADMIN SET TABLE table_name STATUS
        PROPERTIES ("key" = "value", ...);
```

The following properties are currently supported:

1. "state"：Required. Specifying a target state then the state of the OLAP table will change to this state.

> The current target states include:
> 
> 1. NORMAL
> 2. ROLLUP
> 3. SCHEMA_CHANGE
> 4. BACKUP
> 5. RESTORE
> 6. WAITING_STABLE
> 
> If the current state of the table is already the specified state, it will be ignored.

**Note: This command is generally only used for emergency fault repair, please proceed with caution.**

### Example

1. Set the state of table tbl1 to NORMAL.

```sql
admin set table tbl1 status properties("state" = "NORMAL");
```

2. Set the state of table tbl2 to SCHEMA_CHANGE

```sql
admin set table test_set_table_status status properties("state" = "SCHEMA_CHANGE");
```

### Keywords

    ADMIN, SET, TABLE, STATUS

### Best Practice