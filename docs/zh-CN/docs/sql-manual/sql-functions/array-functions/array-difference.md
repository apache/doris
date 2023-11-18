---
{
    "title": "ARRAY_DIFFERENCE",
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

## array_difference

<version since="1.2.0">

array_difference

</version>

### description

#### Syntax

`ARRAY<T> array_difference(ARRAY<T> arr)`

计算相邻数组元素之间的差异。返回一个数组，其中第一个元素将为0，第二个元素是a[1]-a[0]之间的差值。
注意若 NULL 值存在，返回结果为NULL

### notice

`仅支持向量化引擎中使用`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select *,array_difference(k2) from array_type_table;
+------+-----------------------------+---------------------------------+
| k1   | k2                          | array_difference(`k2`)          |
+------+-----------------------------+---------------------------------+
|    0 | []                          | []                              |
|    1 | [NULL]                      | [NULL]                          |
|    2 | [1, 2, 3]                   | [0, 1, 1]                       |
|    3 | [1, NULL, 3]                | [0, NULL, NULL]                 |
|    4 | [0, 1, 2, 3, NULL, 4, 6]    | [0, 1, 1, 1, NULL, NULL, 2]     |
|    5 | [1, 2, 3, 4, 5, 4, 3, 2, 1] | [0, 1, 1, 1, 1, -1, -1, -1, -1] |
|    6 | [6, 7, 8]                   | [0, 1, 1]                       |
+------+-----------------------------+---------------------------------+

```

### keywords

ARRAY, DIFFERENCE, ARRAY_DIFFERENCE
