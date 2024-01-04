---
{
    "title": "ARRAY_CONCAT",
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

## array_concat

<version since="2.0.0">

array_concat

</version>

### description

将输入的所有数组拼接为一个数组

#### Syntax

`Array<T> array_concat(Array<T>, ...)`

#### Returned value

拼接好的数组

类型: Array.

### notice

`只支持在向量化引擎中使用`

### example

```
mysql> select array_concat([1, 2], [7, 8], [5, 6]);
+-----------------------------------------------------+
| array_concat(ARRAY(1, 2), ARRAY(7, 8), ARRAY(5, 6)) |
+-----------------------------------------------------+
| [1, 2, 7, 8, 5, 6]                                  |
+-----------------------------------------------------+
1 row in set (0.02 sec)

mysql> select col2, col3, array_concat(col2, col3) from array_test;
+--------------+-----------+------------------------------+
| col2         | col3      | array_concat(`col2`, `col3`) |
+--------------+-----------+------------------------------+
| [1, 2, 3]    | [3, 4, 5] | [1, 2, 3, 3, 4, 5]           |
| [1, NULL, 2] | [NULL]    | [1, NULL, 2, NULL]           |
| [1, 2, 3]    | NULL      | NULL                         |
| []           | []        | []                           |
+--------------+-----------+------------------------------+
```


### keywords

ARRAY,CONCAT,ARRAY_CONCAT