---
{
    "title": "SHOW-DYNAMIC-PARTITION",
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

## SHOW-DYNAMIC-PARTITION

### Name

SHOW DYNAMIC

### Description

This statement is used to display the status of all dynamic partition tables under the current db

grammar:

```sql
SHOW DYNAMIC PARTITION TABLES [FROM db_name];
````

### Example

  1. Display all dynamic partition table status of database database

     ```sql
     SHOW DYNAMIC PARTITION TABLES FROM database;
     ````

### Keywords

    SHOW, DYNAMIC, PARTITION

### Best Practice

