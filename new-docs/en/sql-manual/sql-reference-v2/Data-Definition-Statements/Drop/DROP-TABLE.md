---
{
    "title": "DROP-TABLE",
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

## DROP-TABLE

### Name

DROP TABLE

### Description

This statement is used to drop a table.
grammar:

```sql
DROP TABLE [IF EXISTS] [db_name.]table_name [FORCE];
````


illustrate:

- After executing DROP TABLE for a period of time, the dropped table can be recovered through the RECOVER statement. See [RECOVER](../../Data-Definition-Statements/Backup-and-Restore/RECOVER.html) statement for details
- If you execute DROP TABLE FORCE, the system will not check whether there are unfinished transactions in the table, the table will be deleted directly and cannot be recovered, this operation is generally not recommended

### Example

1. Delete a table
   
     ```sql
     DROP TABLE my_table;
     ````
    
2. If it exists, delete the table of the specified database
   
     ```sql
     DROP TABLE IF EXISTS example_db.my_table;
     ````

### Keywords

     DROP, TABLE

### Best Practice
