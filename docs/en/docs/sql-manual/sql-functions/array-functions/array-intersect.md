---
{
    "title": "ARRAY_INTERSECT",
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

## array_intersect

<version since="1.2.0">

array_intersect

</version>


### description

#### Syntax

`ARRAY<T> array_intersect(ARRAY<T> array1, ARRAY<T> array2)`

Returns an array of the elements in the intersection of array1 and array2, without duplicates. If the input parameter is null, null is returned.

### notice

`Only supported in vectorized engine`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select k1,k2,k3,array_intersect(k2,k3) from array_type_table;
+------+-----------------+--------------+-----------------------------+
| k1   | k2              | k3           | array_intersect(`k2`, `k3`) |
+------+-----------------+--------------+-----------------------------+
|    1 | [1, 2, 3]       | [2, 4, 5]    | [2]                         |
|    2 | [2, 3]          | [1, 5]       | []                          |
|    3 | [1, 1, 1]       | [2, 2, 2]    | []                          |
+------+-----------------+--------------+-----------------------------+

mysql> select k1,k2,k3,array_intersect(k2,k3) from array_type_table_nullable;
+------+-----------------+--------------+-----------------------------+
| k1   | k2              | k3           | array_intersect(`k2`, `k3`) |
+------+-----------------+--------------+-----------------------------+
|    1 | [1, NULL, 3]    | [1, 3, 5]    | [1, 3]                      |
|    2 | [NULL, NULL, 2] | [2, NULL, 4] | [NULL, 2]                   |
|    3 | NULL            | [1, 2, 3]    | NULL                        |
+------+-----------------+--------------+-----------------------------+

mysql> select k1,k2,k3,array_intersect(k2,k3) from array_type_table_varchar;
+------+----------------------------+----------------------------------+-----------------------------+
| k1   | k2                         | k3                               | array_intersect(`k2`, `k3`) |
+------+----------------------------+----------------------------------+-----------------------------+
|    1 | ['hello', 'world', 'c++']  | ['I', 'am', 'c++']               | ['c++']                     |
|    2 | ['a1', 'equals', 'b1']     | ['a2', 'equals', 'b2']           | ['equals']                  |
|    3 | ['hasnull', NULL, 'value'] | ['nohasnull', 'nonull', 'value'] | [NULL, 'value']             |
|    3 | ['hasnull', NULL, 'value'] | ['hasnull', NULL, 'value']       | ['hasnull', 'value']        |
+------+----------------------------+----------------------------------+-----------------------------+

mysql> select k1,k2,k3,array_intersect(k2,k3) from array_type_table_decimal;
+------+------------------+-------------------+-----------------------------+
| k1   | k2               | k3                | array_intersect(`k2`, `k3`) |
+------+------------------+-------------------+-----------------------------+
|    1 | [1.1, 2.1, 3.44] | [2.1, 3.4, 5.4]   | [2.1]                       |
|    2 | [NULL, 2, 5]     | [NULL, NULL, 5.4] | [NULL]                      |
|    3 | [1, NULL, 2, 5]  | [1, 3.1, 5.4]     | [1]                         |
+------+------------------+-------------------+-----------------------------+

```

### keywords

ARRAY,INTERSECT,ARRAY_INTERSECT

