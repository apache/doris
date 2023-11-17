---
{
    "title": "TRUNCATE-TABLE",
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

## TRUNCATE-TABLE

### Name

TRUNCATE TABLE

### Description

This statement is used to clear the data of the specified table and partition
grammar:

```sql
TRUNCATE TABLE [db.]tbl[ PARTITION(p1, p2, ...)];
````

illustrate:

- The statement clears the data, but leaves the table or partition.
- Unlike DELETE, this statement can only clear the specified table or partition as a whole, and cannot add filter conditions.
- Unlike DELETE, using this method to clear data will not affect query performance.
- The data deleted by this operation cannot be recovered.
- When using this command, the table status needs to be NORMAL, that is, operations such as SCHEMA CHANGE are not allowed.
- This command may cause the ongoing load to fail

### Example

1. Clear the table tbl under example_db

     ```sql
     TRUNCATE TABLE example_db.tbl;
     ````

2. Empty p1 and p2 partitions of table tbl

     ```sql
     TRUNCATE TABLE tbl PARTITION(p1, p2);
     ````

### Keywords

     TRUNCATE, TABLE

### Best Practice
