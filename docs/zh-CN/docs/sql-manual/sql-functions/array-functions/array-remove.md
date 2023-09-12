---
{
    "title": "ARRAY_REMOVE",
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

## array_remove

<version since="1.2.0">

array_remove

</version>

### description

#### Syntax

`ARRAY<T> array_remove(ARRAY<T> arr, T val)`

返回移除所有的指定元素后的数组，如果输入参数为NULL，则返回NULL

### notice

`仅支持向量化引擎中使用`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select array_remove(['test', NULL, 'value'], 'value');
+-----------------------------------------------------+
| array_remove(ARRAY('test', NULL, 'value'), 'value') |
+-----------------------------------------------------+
| [test, NULL]                                        |
+-----------------------------------------------------+

mysql> select k1, k2, array_remove(k2, 1) from array_type_table_1;
+------+--------------------+-----------------------+
| k1   | k2                 | array_remove(`k2`, 1) |
+------+--------------------+-----------------------+
|    1 | [1, 2, 3]          | [2, 3]                |
|    2 | [1, 3]             | [3]                   |
|    3 | NULL               | NULL                  |
|    4 | [1, 3]             | [3]                   |
|    5 | [NULL, 1, NULL, 2] | [NULL, NULL, 2]       |
+------+--------------------+-----------------------+

mysql> select k1, k2, array_remove(k2, k1) from array_type_table_1;
+------+--------------------+--------------------------+
| k1   | k2                 | array_remove(`k2`, `k1`) |
+------+--------------------+--------------------------+
|    1 | [1, 2, 3]          | [2, 3]                   |
|    2 | [1, 3]             | [1, 3]                   |
|    3 | NULL               | NULL                     |
|    4 | [1, 3]             | [1, 3]                   |
|    5 | [NULL, 1, NULL, 2] | [NULL, 1, NULL, 2]       |
+------+--------------------+--------------------------+

mysql> select k1, k2, array_remove(k2, date('2022-10-10')) from array_type_table_date;
+------+--------------------------+-------------------------------------------------+
| k1   | k2                       | array_remove(`k2`, date('2022-10-10 00:00:00')) |
+------+--------------------------+-------------------------------------------------+
|    1 | [2021-10-10, 2022-10-10] | [2021-10-10]                                    |
|    2 | [NULL, 2022-05-14]       | [NULL, 2022-05-14]                              |
+------+--------------------------+-------------------------------------------------+

mysql> select k1, k2, array_remove(k2, k1) from array_type_table_nullable;
+------+-----------+--------------------------+
| k1   | k2        | array_remove(`k2`, `k1`) |
+------+-----------+--------------------------+
| NULL | [1, 2, 3] | NULL                     |
|    1 | NULL      | NULL                     |
| NULL | [NULL, 1] | NULL                     |
|    1 | [NULL, 1] | [NULL]                   |
+------+-----------+--------------------------+

```

### keywords

ARRAY,REMOVE,ARRAY_REMOVE
