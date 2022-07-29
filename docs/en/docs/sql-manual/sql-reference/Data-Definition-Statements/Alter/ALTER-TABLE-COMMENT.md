---
{
    "title": "ALTER-TABLE-COMMENT",
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

## ALTER-TABLE-COMMENT

### Name

ALTER TABLE COMMENT

### Description

This statement is used to modify the comment of an existing table. The operation is synchronous, and the command returns to indicate completion.

grammar：

```sql
ALTER TABLE [database.]table alter_clause;
```

1. Modify table comment

grammar：

```sql
MODIFY COMMENT "new table comment";
```

2. Modify column comment

grammar：

```sql
MODIFY COLUMN col1 COMMENT "new column comment";
```

### Example

1. Change the table1's comment to table1_comment

```sql
ALTER TABLE table1 MODIFY COMMENT "table1_comment";
```

2. Change the table1's col1 comment to table1_comment

```sql
ALTER TABLE table1 MODIFY COLUMN col1 COMMENT "table1_col1_comment";
```

### Keywords

```text
ALTER, TABLE, COMMENT, ALTER TABLE
```

### Best Practice

