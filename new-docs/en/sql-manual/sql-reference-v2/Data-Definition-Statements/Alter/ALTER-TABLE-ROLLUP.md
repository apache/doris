---
{
    "title": "ALTER-TABLE-ROLLUP",
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

## ALTER-TABLE-ROLLUP

### Name

ALTER TABLE ROLLUP

### Description

```text
This statement is used to perform a rollup modification operation on an existing table. rollup is an asynchronous operation, and the task is returned when the task is submitted successfully. After that, you can use the SHOW ALTER command to view the progress.

grammar:
    ALTER TABLE [database.]table alter_clause;

The alter_clause of rollup supports the following creation methods
1. Create a rollup index
    grammar:
        ADD ROLLUP rollup_name (column_name1, column_name2, ...)
        [FROM from_index_name]
        [PROPERTIES ("key"="value", ...)]

        properties: Supports setting the timeout period, the default timeout period is 1 day.
    example:
        ADD ROLLUP r1(col1,col2) from r0
1.2 Create rollup index in batches
    grammar:
        ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
                    [FROM from_index_name]
                    [PROPERTIES ("key"="value", ...)],...]
    example:
        ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0
1.3 Note:
        1) If from_index_name is not specified, it will be created from base index by default
        2) The column in the rollup table must be an existing column in from_index
        3) In properties, you can specify the storage format. For details, please refer to CREATE TABLE
        
2. Delete the rollup index
    grammar:
        DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)]
    example:
        DROP ROLLUP r1
2.1 Batch delete rollup index
    Syntax: DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...]
    Example: DROP ROLLUP r1,r2
2.2 Note:
        1) Cannot delete base index
```

### Example

```text
1. Create index: example_rollup_index, based on base index (k1,k2,k3,v1,v2). Columnar storage.
     ALTER TABLE example_db.my_table
     ADD ROLLUP example_rollup_index(k1, k3, v1, v2);
    
2. Create index: example_rollup_index2, based on example_rollup_index (k1,k3,v1,v2)
     ALTER TABLE example_db.my_table
     ADD ROLLUP example_rollup_index2 (k1, v1)
     FROM example_rollup_index;

3. Create index: example_rollup_index3, based on base index (k1,k2,k3,v1), with a custom rollup timeout of one hour.
     ALTER TABLE example_db.my_table
     ADD ROLLUP example_rollup_index(k1, k3, v1)
     PROPERTIES("timeout" = "3600");

4. Delete index: example_rollup_index2
     ALTER TABLE example_db.my_table
     DROP ROLLUP example_rollup_index2;
```

### Keywords

    ALTER, TABLE, ROLLUP

### Best Practice

