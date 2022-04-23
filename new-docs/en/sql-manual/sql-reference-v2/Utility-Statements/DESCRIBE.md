---
{
    "title": "DESCRIBE",
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

## DESCRIBE

### Name

DESCRIBE

### Description

This statement is used to display the schema information of the specified table

grammar:

```sql
DESC[RIBE] [db_name.]table_name [ALL];
````

illustrate:

1. If ALL is specified, the schemas of all indexes (rollup) of the table will be displayed

### Example

1. Display the Base table schema

    ```sql
    DESC table_name;
    ````

2. Display the schema of all indexes of the table

    ```sql
    DESC db1.table_name ALL;
    ````

### Keywords

    DESCRIBE

### Best Practice

