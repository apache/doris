---
{
    "title": "ARRAY_POSITION",
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

## array_position

<version since="1.2.0">

array_position

</version>

### description

#### Syntax

`BIGINT array_position(ARRAY<T> arr, T value)`

Returns a position/index of first occurrence of the `value` in the given array.

```
position - value position in array (starts with 1);
0        - if value does not present in the array;
NULL     - when array is NULL.
```

### notice

`Only supported in vectorized engine`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> SELECT id,c_array,array_position(c_array, 5) FROM `array_test`;
+------+-----------------+------------------------------+
| id   | c_array         | array_position(`c_array`, 5) |
+------+-----------------+------------------------------+
|    1 | [1, 2, 3, 4, 5] |                            5 |
|    2 | [6, 7, 8]       |                            0 |
|    3 | []              |                            0 |
|    4 | NULL            |                         NULL |
+------+-----------------+------------------------------+

mysql> select array_position([1, null], null);
+--------------------------------------+
| array_position(ARRAY(1, NULL), NULL) |
+--------------------------------------+
|                                    2 |
+--------------------------------------+
1 row in set (0.01 sec)
```

### keywords

ARRAY,POSITION,ARRAY_POSITION

