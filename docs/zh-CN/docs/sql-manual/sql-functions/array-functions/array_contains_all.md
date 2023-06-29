---
{
    "title": "array_contains_all",
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

## array_contains_all

<version since="1.2.5">

array_contains_all

</version>

### description

#### Syntax

```sql
BOOLEAN array_contains_all(ARRAY<T> array1, ARRAY<T> array2)
```

检查 `array2` 是否为 `array1` 的一个子集。注意，输入数组会被看作集合，因此数组内元素的顺序、元素的数量对结果没有影响。

输入数组内可以包含不同类型的元素，只要这些元素之间存在一个公共祖先类型。

### notice

`只支持在向量化引擎中使用`

### example

```sql
mysql> select array_contains_all([], []);
+--------------------------------------+
| array_contains_all(ARRAY(), ARRAY()) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+

mysql> select array_contains_all([1], []);
+---------------------------------------+
| array_contains_all(ARRAY(1), ARRAY()) |
+---------------------------------------+
|                                     1 |
+---------------------------------------+

mysql> select array_contains_all([1, NULL], [NULL]);
+-------------------------------------------------+
| array_contains_all(ARRAY(1, NULL), ARRAY(NULL)) |
+-------------------------------------------------+
|                                               1 |
+-------------------------------------------------+

mysql> select array_contains_all([1, NULL], [NULL, NULL]);
+-------------------------------------------------------+
| array_contains_all(ARRAY(1, NULL), ARRAY(NULL, NULL)) |
+-------------------------------------------------------+
|                                                     1 |
+-------------------------------------------------------+

mysql> select array_contains_all([1, 1], [1, 2]);
+----------------------------------------------+
| array_contains_all(ARRAY(1, 1), ARRAY(1, 2)) |
+----------------------------------------------+
|                                            0 |
+----------------------------------------------+

mysql> select array_contains_all([1, 2, 3], [1, 2, 2]);
+----------------------------------------------------+
| array_contains_all(ARRAY(1, 2, 3), ARRAY(1, 2, 2)) |
+----------------------------------------------------+
|                                                  1 |
+----------------------------------------------------+

mysql> select array_contains_all([1.0, 2, 3, 4], [1, 3]);
+------------------------------------------------------------+
| array_contains_all(ARRAY(1.0, 2.0, 3.0, 4.0), ARRAY(1, 3)) |
+------------------------------------------------------------+
|                                                          1 |
+------------------------------------------------------------+

mysql> select array_contains_all(['a', 'b'], ['a']);
+-------------------------------------------------+
| array_contains_all(ARRAY('a', 'b'), ARRAY('a')) |
+-------------------------------------------------+
|                                               1 |
+-------------------------------------------------+

mysql> select array_contains_all(['a', 1, 2.0], ['a', 1.0, 2]);
+--------------------------------------------------------------------+
| array_contains_all(ARRAY('a', '1', '2.0'), ARRAY('a', '1.0', '2')) |
+--------------------------------------------------------------------+
|                                                                  0 |
+--------------------------------------------------------------------+
```

### keywords

ARRAY,CONTAINS_ALL,ARRAY_CONTAINS_ALL
