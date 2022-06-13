---
{
    "title": "CANCEL-ALTER-TABLE",
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

## CANCEL-ALTER-TABLE

### Name

CANCEL ALTER TABLE

### Description

This statement is used to undo an ALTER operation.

1. Undo the ALTER TABLE COLUMN operation

grammar:

```sql
CANCEL ALTER TABLE COLUMN
FROM db_name.table_name
```

2. Undo the ALTER TABLE ROLLUP operation

grammar:

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name
```

3. Batch cancel rollup operations based on job id

grammar:

```sql
CANCEL ALTER TABLE ROLLUP
FROM db_name.table_name (jobid,...)
```

Notice:

- This command is an asynchronous operation. You need to use `show alter table rollup` to check the task status to confirm whether the execution is successful or not.

4. Undo the ALTER CLUSTER operation

grammar:

```
(To be implemented...)
```

### Example

1. Undo the ALTER COLUMN operation on my_table.

   [CANCEL ALTER TABLE COLUMN]

```sql
CANCEL ALTER TABLE COLUMN
FROM example_db.my_table;
```

1. Undo the ADD ROLLUP operation under my_table.

   [CANCEL ALTER TABLE ROLLUP]

```sql
CANCEL ALTER TABLE ROLLUP
FROM example_db.my_table;
```

1. Undo the ADD ROLLUP operation under my_table according to the job id.

   [CANCEL ALTER TABLE ROLLUP]

```sql
CANCEL ALTER TABLE ROLLUP
FROM example_db.my_table(12801,12802);
```

### Keywords

    CANCEL, ALTER, TABLE

### Best Practice

