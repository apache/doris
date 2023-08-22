---
{
    "title": "ARRAY_PUSHFRONT",
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

## array_pushfront

<version since="2.0">

array_pushfront

</version>

### description

#### Syntax

`Array<T> array_pushfront(Array<T> arr, T value)`
将value添加到数组的开头.

#### Returned value

返回添加value后的数组

类型: Array.

### notice

`只支持在向量化引擎中使用`

### example

```
mysql> select array_pushfront([1, 2], 3);
+---------------------------------+
| array_pushfront(ARRAY(1, 2), 3) |
+---------------------------------+
| [3, 1, 2]                       |
+---------------------------------+

mysql> select col3, array_pushfront(col3, 6) from array_test;
+-----------+----------------------------+
| col3      | array_pushfront(`col3`, 6) |
+-----------+----------------------------+
| [3, 4, 5] | [6, 3, 4, 5]               |
| [NULL]    | [6, NULL]                  |
| NULL      | NULL                       |
| []        | [6]                        |
+-----------+----------------------------+

mysql> select col1, col3, array_pushfront(col3, col1) from array_test;
+------+-----------+---------------------------------+
| col1 | col3      | array_pushfront(`col3`, `col1`) |
+------+-----------+---------------------------------+
|    0 | [3, 4, 5] | [0, 3, 4, 5]                    |
|    1 | [NULL]    | [1, NULL]                       |
|    2 | NULL      | NULL                            |
|    3 | []        | [3]                             |
+------+-----------+---------------------------------+
```

### keywords

ARRAY,PUSHFRONT,ARRAY_PUSHFRONT