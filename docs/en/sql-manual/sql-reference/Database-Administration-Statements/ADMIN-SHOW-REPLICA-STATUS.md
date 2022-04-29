---
{
    "title": "ADMIN-SHOW-REPLICA-STATUS",
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

## ADMIN-SHOW-REPLICA-STATUS

### Name

ADMIN SHOW REPLICA STATUS

### Description

This statement is used to display replica status information for a table or partition.

grammar:

```sql
 ADMIN SHOW REPLICA STATUS FROM [db_name.]tbl_name [PARTITION (p1, ...)]
[where_clause];
````

illustrate

1. where_clause:
       WHERE STATUS [!]= "replica_status"

2. replica_status:
       OK: replica is healthy
       DEAD: The Backend where the replica is located is unavailable
       VERSION_ERROR: replica data version is missing
       SCHEMA_ERROR: The schema hash of the replica is incorrect
       MISSING: replica does not exist

### Example

1. View the status of all replicas of the table

   ```sql
   ADMIN SHOW REPLICA STATUS FROM db1.tbl1;
   ````

2. View a copy of a table with a partition status of VERSION_ERROR

   ```sql
   ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)
   WHERE STATUS = "VERSION_ERROR";
   ````

3. View all unhealthy replicas of the table

   ```sql
   ADMIN SHOW REPLICA STATUS FROM tbl1
   WHERE STATUS != "OK";
   ````

### Keywords

    ADMIN, SHOW, REPLICA, STATUS

### Best Practice

