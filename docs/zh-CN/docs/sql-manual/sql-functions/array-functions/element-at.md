---
{
    "title": "ELEMENT_AT",
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

## element_at

<version since="1.2.0">

element_at

</version>

### description

#### Syntax

```sql
T element_at(ARRAY<T> arr, BIGINT position)
T arr[position]
```

返回数组中位置为 `position` 的元素。如果该位置上元素不存在，返回NULL。`position` 从1开始，并且支持负数。

### notice

`仅支持向量化引擎中使用`

### example

`position` 为正数使用范例:

```
mysql> set enable_vectorized_engine=true;

mysql> SELECT id,c_array,element_at(c_array, 5) FROM `array_test`;
+------+-----------------+--------------------------+
| id   | c_array         | element_at(`c_array`, 5) |
+------+-----------------+--------------------------+
|    1 | [1, 2, 3, 4, 5] |                        5 |
|    2 | [6, 7, 8]       |                     NULL |
|    3 | []              |                     NULL |
|    4 | NULL            |                     NULL |
+------+-----------------+--------------------------+
```

`position` 为负数使用范例:

```
mysql> set enable_vectorized_engine=true;

mysql> SELECT id,c_array,c_array[-2] FROM `array_test`;
+------+-----------------+----------------------------------+
| id   | c_array         | %element_extract%(`c_array`, -2) |
+------+-----------------+----------------------------------+
|    1 | [1, 2, 3, 4, 5] |                                4 |
|    2 | [6, 7, 8]       |                                7 |
|    3 | []              |                             NULL |
|    4 | NULL            |                             NULL |
+------+-----------------+----------------------------------+
```

### keywords

ELEMENT_AT, SUBSCRIPT
