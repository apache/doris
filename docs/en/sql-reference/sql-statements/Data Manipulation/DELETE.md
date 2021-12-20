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

# DELETE
## Description

    This statement is used to conditionally delete data in the specified table (base index) partition.
    This action deletes the rollup index data associated with this base index at the same time.
    Grammar:
        DELETE FROM table_name [PARTITION partition_name]
        WHERE
        column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
    
    Explain:
        1) Optional types of OP include: =, >, <, >=, <=, <=, <=, !=, in, not in
        2) Conditions on key columns can only be specified.
        2) When the selected key column does not exist in a rollup, delete cannot be performed.
        3) The relationship between conditions can only be "and".
        If you want to achieve the "or" relationship, you need to divide the conditions into two DELETE statements.
        4) If it is a partitioned table, you can specify the partition. If not specified, and the session variable delete_without_partition is true, it will be applied to all partitions. If it is a single partition table, you do not need to specify it.
    
    Notice:
        This statement may reduce query efficiency for a period of time after execution.
        The degree of impact depends on the number of deletion conditions specified in the statement.
        The more conditions specified, the greater the impact.

## example

    1. Delete rows whose K1 column value is 3 in my_table partition p 1
    DELETE FROM my_table PARTITION p1
    WHERE k1 = 3;
    
    2. Delete rows whose K1 column value is greater than or equal to 3 and whose K2 column value is "abc" in my_table partition P1
    DELETE FROM my_table PARTITION p1
    WHERE k1 >= 3 AND k2 = "abc";

    2. Delete rows whose K1 column value is greater than or equal to 3 and whose K2 column value is "abc" in my_table partition P1,P2
    DELETE FROM my_table PARTITIONS (p1, p2)
    WHERE k1 >= 3 AND k2 = "abc";

## keyword
    DELETE

