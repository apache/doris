---
{ 
    "title": "SHOW-VIEWS",
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

## SHOW-VIEWS

### Name

SHOW VIEWS

### Description

This statement is used to display all logical views under the current db

grammar:

```sql
SHOW [FULL] VIEWS [LIKE]
````

illustrate:

1. LIKE: Fuzzy query can be performed according to the table name

### Example

  1. Desplay all views under DB

    ```sql
    MySQL [test]> show views;
    +----------------+
    | Tables_in_test |
    +----------------+
    | t1_view        |
    | t2_view        |
    +----------------+
    2 rows in set (0.00 sec)
    ```

2. Fuzzy query by view name

    ```sql
    MySQL [test]> show views like '%t1%';
    +----------------+
    | Tables_in_test |
    +----------------+
    | t1_view        |
    +----------------+
    1 row in set (0.01 sec)
    ```

### Keywords

    SHOW, VIEWS

### Best Practice
