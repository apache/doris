---
{
    "title": "SHOW-DATABASES",
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

## SHOW-DATABASES

### Name

SHOW DATABASES

### Description

This statement is used to display the currently visible db

grammar:

```sql
SHOW DATABASES [FROM catalog] [filter expr];
````

illustrate:
1. `SHOW DATABASES` will get all database names from current catalog.
2. `SHOW DATABASES FROM catalog` will all database names from the catalog named 'catalog'.
3. `SHOW DATABASES filter_expr` will get filtered database names from current catalog.
4. `SHOW DATABASES FROM catalog filter_expr` is not support yet.

### Example
1. Display all the database names from current catalog.

   ```sql
   SHOW DATABASES;
   ````

   ````
  +--------------------+
  | Database           |
  +--------------------+
  | test               |
  | information_schema |
  +--------------------+
   ````

2. Display all database names from the catalog named 'hms_catalog'.

   ```sql
   SHOW DATABASES from hms_catalog;
   ````

   ````
  +---------------+
  | Database      |
  +---------------+
  | default       |
  | tpch          |
  +---------------+
   ````

3. Display the filtered database names from current catalog with the expr 'like'.

   ```sql
   SHOW DATABASES like 'infor%';
   ````

   ````
  +--------------------+
  | Database           |
  +--------------------+
  | information_schema |
  +--------------------+
   ````

### Keywords

    SHOW, DATABASES

### Best Practice

