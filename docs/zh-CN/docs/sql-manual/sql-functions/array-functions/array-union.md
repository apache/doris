---
{
    "title": "ARRAY_UNION",
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

## array_union

<version since="1.2.0">

array_union

</version>

### description

#### Syntax

`ARRAY<T> array_union(ARRAY<T> array1, ARRAY<T> array2)`

返回一个数组，包含array1和array2的并集中的所有元素，不包含重复项，如果输入参数为NULL，则返回NULL

### notice

`仅支持向量化引擎中使用`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select k1,k2,k3,array_union(k2,k3) from array_type_table;
+------+-----------------+--------------+-------------------------+
| k1   | k2              | k3           | array_union(`k2`, `k3`) |
+------+-----------------+--------------+-------------------------+
|    1 | [1, 2, 3]       | [2, 4, 5]    | [1, 2, 3, 4, 5]         |
|    2 | [2, 3]          | [1, 5]       | [2, 3, 1, 5]            |
|    3 | [1, 1, 1]       | [2, 2, 2]    | [1, 2]                  |
+------+-----------------+--------------+-------------------------+

mysql> select k1,k2,k3,array_union(k2,k3) from array_type_table_nullable;
+------+-----------------+--------------+-------------------------+
| k1   | k2              | k3           | array_union(`k2`, `k3`) |
+------+-----------------+--------------+-------------------------+
|    1 | [1, NULL, 3]    | [1, 3, 5]    | [1, NULL, 3, 5]         |
|    2 | [NULL, NULL, 2] | [2, NULL, 4] | [NULL, 2, 4]            |
|    3 | NULL            | [1, 2, 3]    | NULL                    |
+------+-----------------+--------------+-------------------------+

mysql> select k1,k2,k3,array_union(k2,k3) from array_type_table_varchar;
+------+----------------------------+----------------------------------+---------------------------------------------------+
| k1   | k2                         | k3                               | array_union(`k2`, `k3`)                           |
+------+----------------------------+----------------------------------+---------------------------------------------------+
|    1 | ['hello', 'world', 'c++']  | ['I', 'am', 'c++']               | ['hello', 'world', 'c++', 'I', 'am']              |
|    2 | ['a1', 'equals', 'b1']     | ['a2', 'equals', 'b2']           | ['a1', 'equals', 'b1', 'a2', 'b2']                |
|    3 | ['hasnull', NULL, 'value'] | ['nohasnull', 'nonull', 'value'] | ['hasnull', NULL, 'value', 'nohasnull', 'nonull'] |
|    4 | ['hasnull', NULL, 'value'] | ['hasnull', NULL, 'value']       | ['hasnull', NULL, 'value']                        |
+------+----------------------------+----------------------------------+---------------------------------------------------+

mysql> select k1,k2,k3,array_union(k2,k3) from array_type_table_decimal;
+------+------------------+-------------------+----------------------------+
| k1   | k2               | k3                | array_union(`k2`, `k3`)    |
+------+------------------+-------------------+----------------------------+
|    1 | [1.1, 2.1, 3.44] | [2.1, 3.4, 5.4]   | [1.1, 2.1, 3.44, 3.4, 5.4] |
|    2 | [NULL, 2, 5]     | [NULL, NULL, 5.4] | [NULL, 2, 5, 5.4]          |
|    4 | [1, NULL, 2, 5]  | [1, 3.1, 5.4]     | [1, NULL, 2, 5, 3.1, 5.4]  |
+------+------------------+-------------------+----------------------------+

```

### keywords

ARRAY,UNION,ARRAY_UNION