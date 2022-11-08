---
{
    "title": "DELETE",
    "language": "zh-CN"
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

该语句用于按条件删除指定 table（base index） partition 中的数据。

该操作会同时删除和此 base index 相关的 rollup index 的数据。

语法：

```SQL
DELETE FROM table_name [PARTITION partition_name | PARTITIONS (p1, p2)]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
```
说明：

1. op 的可选类型包括：=, >, <, >=, <=, !=, in, not in
2. 使用聚合类的表模型（AGGREGATE、UNIQUE）只能指定 key 列上的条件。
3. 当选定的 key 列不存在于某个 rollup 中时，无法进行 delete。
4. 条件之间只能是“与”的关系。若希望达成“或”的关系，需要将条件分写在两个 DELETE 语句中。
5. 如果为分区表，需要指定分区，如果不指定，doris 会从条件中推断出分区。两种情况下，doris 无法从条件中推断出分区: 1) 条件中不包含分区列；2) 分区列的 op 为 not in。当分区表未指定分区，或者无法从条件中推断分区的时候，需要设置会话变量 delete_without_partition 为 true，此时 delete 会应用到所有分区。

注意：
1. 该语句可能会降低执行后一段时间内的查询效率。
2. 影响程度取决于语句中指定的删除条件的数量。
3. 指定的条件越多，影响越大。

### Example

1. 删除 my_table partition p1 中 k1 列值为 3 的数据行
    
    ```sql
    DELETE FROM my_table PARTITION p1
        WHERE k1 = 3;
    ```
    
1. 删除 my_table partition p1 中 k1 列值大于等于 3 且 k2 列值为 "abc" 的数据行
    
    ```sql
    DELETE FROM my_table PARTITION p1
    WHERE k1 >= 3 AND k2 = "abc";
    ```
    
1. 删除 my_table partition p1, p2 中 k1 列值大于等于 3 且 k2 列值为 "abc" 的数据行
    
    ```sql
    DELETE FROM my_table PARTITIONS (p1, p2)
    WHERE k1 >= 3 AND k2 = "abc";
    ```

### Keywords

    DELETE

### Best Practice

