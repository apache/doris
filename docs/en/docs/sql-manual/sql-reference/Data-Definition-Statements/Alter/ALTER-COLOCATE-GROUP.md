---
{
"title": "ALTER-COLOCATE-GROUP",
"language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

## ALTER-COLOCATE-GROUP

### Name

ALTER COLOCATE GROUP

<version since="dev"></version>

### Description

This statement is used to modify the colocation group.

Syntax:

```sql
ALTER COLOCATE GROUP [database.]group
SET (
    property_list
);
```

NOTE:

1. If the colocate group is global, that is, its name starts with `__global__`, then it does not belong to any database;	

2. property_list is a colocation group attribute, currently only supports modifying `replication_num` and `replication_allocation`. After modifying these two attributes of the colocation group, at the same time, change the attribute `default.replication_allocation`, the attribute `dynamic.replication_allocation` of the table of the group, and the `replication_allocation` of the existing partition to be the same as it.

### Example

1. Modify the number of copies of a global group

     ```sql
     # Set "colocate_with" = "__global__foo" when creating the table
     
     ALTER COLOCATE GROUP __global__foo
     SET (
         "replication_num"="1"
     );   
     ```

2. Modify the number of copies of a non-global group

  ```sql
     # Set "colocate_with" = "bar" when creating the table, and the Database is "example_db"
     
     ALTER COLOCATE GROUP example_db.bar
     SET (
         "replication_num"="1"
     );
     ```

### Keywords

```sql
ALTER, COLOCATE, GROUP
```

### Best Practice
