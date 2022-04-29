---
{
    "title": "DROP-DATABASE",
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

## DROP-DATABASE

### Name

DOPR DATABASE

### Description

This statement is used to delete the database (database)
grammar:    

```sql
DROP DATABASE [IF EXISTS] db_name [FORCE];
````

illustrate:

- During the execution of DROP DATABASE, the deleted database can be recovered through the RECOVER statement. See the [RECOVER](../../Data-Definition-Statements/Backup-and-Restore/RECOVER.html) statement for details
- If you execute DROP DATABASE FORCE, the system will not check the database for unfinished transactions, the database will be deleted directly and cannot be recovered, this operation is generally not recommended

### Example

1. Delete the database db_test
   
     ```sql
     DROP DATABASE db_test;
     ````

### Keywords

     DROP, DATABASE

### Best Practice
