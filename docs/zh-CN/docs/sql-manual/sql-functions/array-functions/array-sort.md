---
{
    "title": "ARRAY_SORT",
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

## array_sort

<version since="1.2.0">

array_sort

</version>

### description

#### Syntax

`ARRAY<T> array_sort(ARRAY<T> arr)`

返回按升序排列后的数组，如果输入数组为NULL，则返回NULL。
如果数组元素包含NULL, 则输出的排序数组会将NULL放在最前面。

### notice

`仅支持向量化引擎中使用`

### example

```
mysql> set enable_vectorized_engine=true;
mysql> select k1, k2, array_sort(k2) from array_test;
+------+-----------------------------+-----------------------------+
| k1   | k2                          | array_sort(`k2`)            |
+------+-----------------------------+-----------------------------+
|  1   | [1, 2, 3, 4, 5]             | [1, 2, 3, 4, 5]             |
|  2   | [6, 7, 8]                   | [6, 7, 8]                   |
|  3   | []                          | []                          |
|  4   | NULL                        | NULL                        |
|  5   | [1, 2, 3, 4, 5, 4, 3, 2, 1] | [1, 1, 2, 2, 3, 3, 4, 4, 5] |
|  6   | [1, 2, 3, NULL]             | [NULL, 1, 2, 3]             |
|  7   | [1, 2, 3, NULL, NULL]       | [NULL, NULL, 1, 2, 3]       |
|  8   | [1, 1, 2, NULL, NULL]       | [NULL, NULL, 1, 1, 2]       |
|  9   | [1, NULL, 1, 2, NULL, NULL] | [NULL, NULL, NULL, 1, 1, 2] |
+------+-----------------------------+-----------------------------+

mysql> select k1, k2, array_sort(k2) from array_test01;
+------+------------------------------------------+------------------------------------------+
| k1   | k2                                       | array_sort(`k2`)                         |
+------+------------------------------------------+------------------------------------------+
|  1   | ['a', 'b', 'c', 'd', 'e']                | ['a', 'b', 'c', 'd', 'e']                |
|  2   | ['f', 'g', 'h']                          | ['f', 'g', 'h']                          |
|  3   | ['']                                     | ['']                                     |
|  3   | [NULL]                                   | [NULL]                                   |
|  5   | ['a', 'b', 'c', 'd', 'e', 'a', 'b', 'c'] | ['a', 'a', 'b', 'b', 'c', 'c', 'd', 'e'] |
|  6   | NULL                                     | NULL                                     |
|  7   | ['a', 'b', NULL]                         | [NULL, 'a', 'b']                         |
|  8   | ['a', 'b', NULL, NULL]                   | [NULL, NULL, 'a', 'b']                   |
+------+------------------------------------------+------------------------------------------+
```

### keywords

ARRAY, SORT, ARRAY_SORT
