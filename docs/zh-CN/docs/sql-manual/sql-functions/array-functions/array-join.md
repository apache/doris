---
{
    "title": "ARRAY_JOIN",
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

## array_join

<version since="1.2.0">

array_join

</version>

### description

#### Syntax

`VARCHAR array_join(ARRAY<T> arr, VARCHAR sep[, VARCHAR null_replace])`

根据分隔符(sep)和替换NULL的字符串(null_replace), 将数组中的所有元素组合成一个新的字符串。
若sep为NULL，则返回值为NULL。
若null_replace为NULL，则返回值也为NULL。
若sep为空字符串，则不应用任何分隔符。
若null_replace为空字符串或者不指定，则直接丢弃数组中的NULL元素。

### notice

`仅支持向量化引擎中使用`

### example

```

mysql> set enable_vectorized_engine=true;

mysql> select k1, k2, array_join(k2, '_', 'null') from array_test order by k1;
+------+-----------------------------+------------------------------------+
| k1   | k2                          | array_join(`k2`, '_', 'null')      |
+------+-----------------------------+------------------------------------+
|  1   | [1, 2, 3, 4, 5]             | 1_2_3_4_5                          |
|  2   | [6, 7, 8]                   | 6_7_8                              |
|  3   | []                          |                                    |
|  4   | NULL                        | NULL                               |
|  5   | [1, 2, 3, 4, 5, 4, 3, 2, 1] | 1_2_3_4_5_4_3_2_1                  |
|  6   | [1, 2, 3, NULL]             | 1_2_3_null                         |
|  7   | [4, 5, 6, NULL, NULL]       | 4_5_6_null_null                    |
+------+-----------------------------+------------------------------------+

mysql> select k1, k2, array_join(k2, '_', 'null') from array_test01 order by k1;
+------+-----------------------------------+------------------------------------+
| k1   | k2                                | array_join(`k2`, '_', 'null')      |
+------+-----------------------------------+------------------------------------+
|  1   | ['a', 'b', 'c', 'd']              | a_b_c_d                            |
|  2   | ['e', 'f', 'g', 'h']              | e_f_g_h                            |
|  3   | [NULL, 'a', NULL, 'b', NULL, 'c'] | null_a_null_b_null_c               |
|  4   | ['d', 'e', NULL, ' ']             | d_e_null_                          |
|  5   | [' ', NULL, 'f', 'g']             |  _null_f_g                         |
+------+-----------------------------------+------------------------------------+

mysql> select k1, k2, array_join(k2, '_') from array_test order by k1;
+------+-----------------------------+----------------------------+
| k1   | k2                          | array_join(`k2`, '_')      |
+------+-----------------------------+----------------------------+
|  1   | [1, 2, 3, 4, 5]             | 1_2_3_4_5                  |
|  2   | [6, 7, 8]                   | 6_7_8                      |
|  3   | []                          |                            |
|  4   | NULL                        | NULL                       |
|  5   | [1, 2, 3, 4, 5, 4, 3, 2, 1] | 1_2_3_4_5_4_3_2_1          |
|  6   | [1, 2, 3, NULL]             | 1_2_3                      |
|  7   | [4, 5, 6, NULL, NULL]       | 4_5_6                      |
+------+-----------------------------+----------------------------+

mysql> select k1, k2, array_join(k2, '_') from array_test01 order by k1;
+------+-----------------------------------+----------------------------+
| k1   | k2                                | array_join(`k2`, '_')      |
+------+-----------------------------------+----------------------------+
|  1   | ['a', 'b', 'c', 'd']              | a_b_c_d                    |
|  2   | ['e', 'f', 'g', 'h']              | e_f_g_h                    |
|  3   | [NULL, 'a', NULL, 'b', NULL, 'c'] | a_b_c                      |
|  4   | ['d', 'e', NULL, ' ']             | d_e_                       |
|  5   | [' ', NULL, 'f', 'g']             |  _f_g                      |
+------+-----------------------------------+----------------------------+
```

### keywords

ARRAY, JOIN, ARRAY_JOIN
