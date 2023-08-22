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
ALTER COLOCATE GROUP "full_group_name"
SET (
    property_list
);
```

NOTE:

1. `full_group_name` is the full name of the colocation group, which can be divided into two cases:
 - If the group is global, that is, its name starts with `__global__`, then `full_group_name` is equal to `group_name`;
 - If the group is not global, that is, its name does not start with `__global__`, then it belongs to a certain Database, `full_group_name` is equal to `dbId` + `_` + `group_name`
 
2. `full_group_name` can also be viewed through the command `show proc '/proc/colocation_group'`;
	

3. property_list is a colocation group attribute, currently only supports modifying `replication_num` and `replication_allocation`. After modifying these two attributes of the colocation group, at the same time, change the attribute `default.replication_allocation`, the attribute `dynamic.replication_allocation` of the table of the group, and the `replication_allocation` of the existing partition to be the same as it.

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
     # Set "colocate_with" = "bar" when creating the table, and the dbId of the Database where the table is located is 10231
     
     ALTER COLOCATE GROUP 10231_bar
     SET (
         "replication_num"="1"
     );
     ```

### Keywords

```sql
ALTER, COLOCATE, GROUP
```

### Best Practice
