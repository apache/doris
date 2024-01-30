---
{
    "title": "ALTER-TABLE-RENAME",
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

## ALTER-TABLE-RENAME

### Name

ALTER TABLE RENAME

### Description

This statement is used to rename certain names of existing table properties. This operation is synchronous, and the return of the command indicates the completion of the execution.

grammar:

```sql
ALTER TABLE [database.]table alter_clause;
```

The alter_clause of rename supports modification of the following names

1. Modify the table name

grammar:

```sql
RENAME new_table_name;
```

2. Modify the rollup index name

 grammar:

```sql
RENAME ROLLUP old_rollup_name new_rollup_name;
```

3. Modify the partition name

grammar:

```sql
RENAME PARTITION old_partition_name new_partition_name;
```

4. Modify the column name

<version since="1.2">

Modify the column name
 
</version>

grammar:

```sql
RENAME COLUMN old_column_name new_column_name;
```

Notice:
- When creating a table, you need to set 'light_schema_change=true' in the property.


### Example

1. Modify the table named table1 to table2

```sql
ALTER TABLE table1 RENAME table2;
```

2. Modify the rollup index named rollup1 in the table example_table to rollup2

```sql
ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
```

3. Modify the partition named p1 in the table example_table to p2

```sql
ALTER TABLE example_table RENAME PARTITION p1 p2;
```

4. Modify the column named c1 in the table example_table to c2

```sql
ALTER TABLE example_table RENAME COLUMN c1 c2;
```

### Keywords

```text
ALTER, TABLE, RENAME, ALTER TABLE
```

### Best Practice

