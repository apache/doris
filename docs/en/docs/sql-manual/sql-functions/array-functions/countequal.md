---
{
    "title": "COUNTEQUAL",
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

## countequal

<version since="1.2.0">

countequal

</version>

### description

#### Syntax

`BIGINT countequal(ARRAY<T> arr, T value)`

Returns a number of the `value` in the given array.

```
num      - how many the value number in array;
0        - if value does not present in the array;
NULL     - when array is NULL.
```

### notice

`Only supported in vectorized engine`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select *, countEqual(c_array,5) from array_test;
+------+-----------------+--------------------------+
| id   | c_array         | countequal(`c_array`, 5) |
+------+-----------------+--------------------------+
|    1 | [1, 2, 3, 4, 5] |                        1 |
|    2 | [6, 7, 8]       |                        0 |
|    3 | []              |                        0 |
|    4 | NULL            |                     NULL |
+------+-----------------+--------------------------+

mysql> select *,countEqual(c_array, 1),countEqual(c_array, 5),countEqual(c_array, NULL) from array_test;
+------+-----------------------+--------------------------+--------------------------+-----------------------------+
| id   | c_array               | countequal(`c_array`, 1) | countequal(`c_array`, 5) | countequal(`c_array`, NULL) |
+------+-----------------------+--------------------------+--------------------------+-----------------------------+
|    1 | [1, 2, 3, 4, 5]       |                        1 |                        1 |                           0 |
|    2 | [6, 7, 8]             |                        0 |                        0 |                           0 |
|    3 | []                    |                        0 |                        0 |                           0 |
|    4 | NULL                  |                     NULL |                     NULL |                        NULL |
|    5 | [66, 77]              |                        0 |                        0 |                           0 |
|    5 | [66, 77]              |                        0 |                        0 |                           0 |
|    6 | NULL                  |                     NULL |                     NULL |                        NULL |
|    7 | [NULL, NULL, NULL]    |                        0 |                        0 |                           3 |
|    8 | [1, 2, 3, 4, 5, 5, 5] |                        1 |                        3 |                           0 |
+------+-----------------------+--------------------------+--------------------------+-----------------------------+
```

### keywords

ARRAY,COUNTEQUAL,

