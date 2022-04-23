---
{
    "title": "ALTER-TABLE-BITMAP",
    "language": "en"
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

## ALTER-TABLE-BITMAP

### Name

ALTER TABLE BITMAP

### Description

This statement is used to perform a bitmap index operation on an existing table.

grammar:

```sql
ALTER TABLE [database.]table alter_clause;
```

The alter_clause of bitmap index supports the following modification methods

1. Create a bitmap index

 Syntax:

```sql
ADD INDEX [IF NOT EXISTS] index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
```

Notice:

- Currently only supports bitmap indexes
- BITMAP indexes are only created on a single column

2. Delete the index

Syntax:

```sql
DROP INDEX [IF EXISTS] index_name;
```

### Example

1. Create a bitmap index for siteid on table1

```sql
ALTER TABLE table1 ADD INDEX [IF NOT EXISTS] index_name (siteid) [USING BITMAP] COMMENT 'balabala';
```

2. Delete the bitmap index of the siteid column on table1

```sql
ALTER TABLE table1 DROP INDEX [IF EXISTS] index_name;
```

### Keywords

```text
ALTER, TABLE, BITMAP
```

### Best Practice
