---
{
    "title": "SHOW-TABLES",
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

## SHOW-TABLES

### Name

SHOW TABLES

### Description

This statement is used to display all tables under the current db

grammar:

```sql
SHOW [FULL] TABLES [LIKE]
````

illustrate:

1. LIKE: Fuzzy query can be performed according to the table name

### Example

  1. View all tables under DB

     ```sql
     mysql> show tables;
     +---------------------------------+
     | Tables_in_demo                  |
     +---------------------------------+
     | ads_client_biz_aggr_di_20220419 |
     | cmy1                            |
     | cmy2                            |
     | intern_theme                    |
     | left_table                      |
     +---------------------------------+
     5 rows in set (0.00 sec)
     ````

2. Fuzzy query by table name

    ```sql
    mysql> show tables like '%cm%';
    +----------------+
    | Tables_in_demo |
    +----------------+
    | cmy1           |
    | cmy2           |
    +----------------+
    2 rows in set (0.00 sec)
    ````

### Keywords

    SHOW, TABLES

### Best Practice

