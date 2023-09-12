---
{
    "title": "ELEMENT_AT",
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

Returns an element of an array located at the input position. If there is no element at the position, return NULL.

`position` is 1-based and support negative number.

### notice

`Only supported in vectorized engine`

### example

positive `position` example:

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

negative `position` example:

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

