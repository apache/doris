---
{
    "title": "ADMIN-REPAIR-TABLE",
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

## ADMIN-REPAIR-TABLE

### Name

ADMIN REPAIR TABLE

### Description

statement used to attempt to preferentially repair the specified table or partition

grammar:

```sql
ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)]
````

illustrate:

1. This statement only means to let the system try to repair the shard copy of the specified table or partition with high priority, and does not guarantee that the repair can be successful. Users can view the repair status through the ADMIN SHOW REPLICA STATUS command.
2. The default timeout is 14400 seconds (4 hours). A timeout means that the system will no longer repair shard copies of the specified table or partition with high priority. Need to re-use this command to set

### Example

1. Attempt to repair the specified table

        ADMIN REPAIR TABLE tbl1;

2. Try to repair the specified partition

        ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);

### Keywords

    ADMIN, REPAIR, TABLE

### Best Practice

