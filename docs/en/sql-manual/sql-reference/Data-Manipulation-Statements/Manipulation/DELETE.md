---
{
    "title": "DELETE",
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

## DELETE

### Name

DELETE

### Description

This statement is used to conditionally delete data in the specified table (base index) partition.

This operation will also delete the data of the rollup index related to this base index.

grammar:

````SQL
DELETE FROM table_name [PARTITION partition_name | PARTITIONS (p1, p2)]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
````

illustrate:

1. The optional types of op include: =, >, <, >=, <=, !=, in, not in
2. Only conditions on the key column can be specified.
3. When the selected key column does not exist in a rollup, delete cannot be performed.
4. Conditions can only have an "and" relationship. If you want to achieve an "or" relationship, you need to write the conditions in two DELETE statements.
5. If it is a partitioned table, you can specify a partition. If not specified, and the session variable delete_without_partition is true, it will be applied to all partitions. If it is a single-partition table, it can be left unspecified.

Notice:

1. This statement may reduce query efficiency for a period of time after execution.
2. The degree of impact depends on the number of delete conditions specified in the statement.
3. The more conditions you specify, the greater the impact.

### Example

1. Delete the data row whose k1 column value is 3 in my_table partition p1

   ```sql
   DELETE FROM my_table PARTITION p1
       WHERE k1 = 3;
   ````

2. Delete the data rows where the value of column k1 is greater than or equal to 3 and the value of column k2 is "abc" in my_table partition p1

   ```sql
   DELETE FROM my_table PARTITION p1
   WHERE k1 >= 3 AND k2 = "abc";
   ````

3. Delete the data rows where the value of column k1 is greater than or equal to 3 and the value of column k2 is "abc" in my_table partition p1, p2

   ```sql
   DELETE FROM my_table PARTITIONS (p1, p2)
   WHERE k1 >= 3 AND k2 = "abc";
   ````

### Keywords

    DELETE

### Best Practice

