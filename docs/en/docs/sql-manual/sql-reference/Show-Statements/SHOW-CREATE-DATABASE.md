---
{
    "title": "SHOW-CREATE-DATABASE",
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

## SHOW-CREATE-DATABASE

### Name

SHOW CREATE DATABASE

### Description

This statement checks the creation of the doris database, support database from both internal catalog and hms catalog

grammar:

```sql
SHOW CREATE DATABASE db_name;
````

illustrate:

- `db_name`: The name of the database
- if specific a database from hms catalog, will return same with this stmt in hive

### Example

1. View the creation of the test database in doris internal catalog

    ```sql
    mysql> SHOW CREATE DATABASE test;
    +----------+----------------------------+
    | Database | Create Database |
    +----------+----------------------------+
    | test | CREATE DATABASE `test` |
    +----------+----------------------------+
    1 row in set (0.00 sec)
    ````

2. view a database named `hdfs_text` from a hms catalog

    ```sql
    mysql> show create database hdfs_text;                                                                                     
    +-----------+------------------------------------------------------------------------------------+                         
    | Database  | Create Database                                                                    |                         
    +-----------+------------------------------------------------------------------------------------+                         
    | hdfs_text | CREATE DATABASE `hdfs_text` LOCATION 'hdfs://HDFS1009138/hive/warehouse/hdfs_text' |                         
    +-----------+------------------------------------------------------------------------------------+                         
    1 row in set (0.01 sec)  
    ```
   
### Keywords

     SHOW, CREATE, DATABASE

### Best Practice