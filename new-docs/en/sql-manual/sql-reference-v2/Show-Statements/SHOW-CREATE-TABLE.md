---
{
    "title": "SHOW-CREATE-TABLE",
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

## SHOW-CREATE-TABLE

### Name

SHOW CREATE TABLE

### Description

This statement is used to display the creation statement of the data table.

grammar:

```sql
SHOW CREATE TABLE [DBNAME.]TABLE_NAME
````

illustrate:

1. `DBNAMNE` : database name
2. `TABLE_NAME` : table name

### Example

1. View the table creation statement of a table

    ```sql
    SHOW CREATE TABLE demo.tb1
    ````

### Keywords

    SHOW, CREATE, TABLE

### Best Practice

