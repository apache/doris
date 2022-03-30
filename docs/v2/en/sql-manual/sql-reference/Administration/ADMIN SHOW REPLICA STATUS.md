---
{
    "title": "ADMIN SHOW REPLICA STATUS",
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

# ADMIN SHOW REPLICA STATUS
## Description

This statement is used to display copy status information for a table or partition

Grammar:

ADMIN SHOW REPLICA STATUS FROM [dbu name.]tbl name [PARTITION (p1,...)]
[where_clause];

where_clause:
WHERE STATUS [!]= "replica_status"

Reply status:
OK: Replica 22788;'20581;' 29366;'24577;
DEAD: The Backend of replica is not available
VERSION_ERROR: The replica data version is missing
SCHEMA ERROR: replica schema hash
MISSING: replica does not exist

## example

1. View the status of all copies of the table

ADMIN SHOW REPLICA STATUS FROM db1.tbl1;

2. View a copy of a partition state of the table as VERSION_ERROR

ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)


3. Check all unhealthy copies of the table

ADMIN SHOW REPLICA STATUS FROM tbl1
WHERE STATUS != "OK";

## keyword
ADMIN,SHOW,REPLICA,STATUS
