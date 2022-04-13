---
{
    "title": "SHOW-TABLE-STATUS",
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

## SHOW-TABLE-STATUS

### Name

SHOW TABLE STATUS

### Description

This statement is used to view some information about the Table.

grammar:

```sql
SHOW TABLE STATUS
[FROM db] [LIKE "pattern"]
````

illustrate:

1. This statement is mainly used to be compatible with MySQL syntax, currently only a small amount of information such as Comment is displayed

### Example

  1. View the information of all tables under the current database

     ```sql
     SHOW TABLE STATUS;
     ````

  2. View the information of the table whose name contains example under the specified database

     ```sql
     SHOW TABLE STATUS FROM db LIKE "%example%";
     ````

### Keywords

    SHOW, TABLE, STATUS

### Best Practice

