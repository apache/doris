---
{
    "title": "ALTER-TABLE-REPLACE",
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

## ALTER-TABLE-REPLACE

### Name

ALTER TABLE REPLACE

### Description

该语句用于对已有 table 的 Schema 的进行属性的修改操作。语法基本类似于 [ALTER TABLE CULUMN](ALTER-TABLE-COLUMN.html)。

```sql
ALTER TABLE [database.]table MODIFY NEW_COLUMN_INFO REPLACE OLD_COLUMN_INFO ;
```

### Example

1. 修改 base index 的 val1 列最大长度。原 val1 为 (val1 VARCHAR(32) REPLACE DEFAULT "abc")

```sql
ALTER TABLE example_db.my_table
MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
```

### Keywords

```text
ALTER, TABLE, REPLACE
```

### Best Practice

