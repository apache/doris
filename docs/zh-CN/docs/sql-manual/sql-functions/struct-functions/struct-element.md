---
{
    "title": "STRUCT_ELEMENT",
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

## struct_element

<version since="2.0">

struct_element

</version>

### description

返回struct数据列内的某一field

#### Syntax

```
struct_element(struct, n/s)
```

#### Arguments

```
struct - 输入的struct列，如果是null，则返回null
n - field的位置，起始位置从1开始，仅支持常量
s - field的名字，仅支持常量
```

#### Returned value

返回指定的field列，类型为任意类型

### notice

`只支持在向量化引擎中使用。`

### example

```
mysql> select struct_element(named_struct('f1', 1, 'f2', 'a'), 'f2');
+--------------------------------------------------------+
| struct_element(named_struct('f1', 1, 'f2', 'a'), 'f2') |
+--------------------------------------------------------+
| a                                                      |
+--------------------------------------------------------+
1 row in set (0.03 sec)

mysql> select struct_element(named_struct('f1', 1, 'f2', 'a'), 1);
+-----------------------------------------------------+
| struct_element(named_struct('f1', 1, 'f2', 'a'), 1) |
+-----------------------------------------------------+
|                                                   1 |
+-----------------------------------------------------+
1 row in set (0.02 sec)

mysql> select struct_col, struct_element(struct_col, 'f1') from test_struct;
+-------------------------------------------------+-------------------------------------+
| struct_col                                      | struct_element(`struct_col `, 'f1') |
+-------------------------------------------------+-------------------------------------+
| {1, 2, 3, 4, 5}                                 |                                   1 |
| {1, 1000, 10000000, 100000000000, 100000000000} |                                   1 |
| {5, 4, 3, 2, 1}                                 |                                   5 |
| NULL                                            |                                NULL |
| {1, NULL, 3, NULL, 5}                           |                                   1 |
+-------------------------------------------------+-------------------------------------+
9 rows in set (0.01 sec)
```

### keywords

STRUCT, ELEMENT, STRUCT_ELEMENT