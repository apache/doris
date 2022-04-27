---
{
    "title": "SHOW-ALTER",
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

## SHOW-ALTER

### Name

SHOW ALTER

### Description

This statement is used to display the execution of various modification tasks currently in progress

```sql
SHOW ALTER [CLUSTER | TABLE [COLUMN | ROLLUP] [FROM db_name]];
````

TABLE COLUMN: show ALTER tasks that modify columns
                      Support syntax [WHERE TableName|CreateTime|FinishTime|State] [ORDER BY] [LIMIT]
        TABLE ROLLUP: Shows the task of creating or deleting a ROLLUP index
        If db_name is not specified, the current default db is used
        CLUSTER: Displays tasks related to cluster operations (only for administrators! To be implemented...)

### Example

1. Display the task execution of all modified columns of the default db

   ```sql
    SHOW ALTER TABLE COLUMN;
   ````

2. Display the task execution status of the last modified column of a table

   ```sql
   SHOW ALTER TABLE COLUMN WHERE TableName = "table1" ORDER BY CreateTime DESC LIMIT 1;
   ````

3. Display the task execution of creating or deleting ROLLUP index for the specified db

   ```sql
   SHOW ALTER TABLE ROLLUP FROM example_db;
   ````

4. Show tasks related to cluster operations (only for administrators! To be implemented...)

   ````
   SHOW ALTER CLUSTER;
   ````

### Keywords

    SHOW, ALTER

### Best Practice

