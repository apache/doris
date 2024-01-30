---
{
    "title": "ARRAY_SLICE",
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

## array_slice

<version since="1.2.0">

array_slice

</version>

### description

#### Syntax

`ARRAY<T> array_slice(ARRAY<T> arr, BIGINT off, BIGINT len)`

Returns a slice of the array.

```
A positive off indicates an indent on the left
A negative off indicates an indent on the right.
An empty array is returned when the off is not within the actual range of the array.
A negative len will be treated as 0.
```

### notice

`Only supported in vectorized engine`

### example


```
mysql> set enable_vectorized_engine=true;

mysql> select k2, k2[2:2] from array_type_table_nullable;
+-----------------+-------------------------+
| k2              | array_slice(`k2`, 2, 2) |
+-----------------+-------------------------+
| [1, 2, 3]       | [2, 3]                  |
| [1, NULL, 3]    | [NULL, 3]               |
| [2, 3]          | [3]                     |
| NULL            | NULL                    |
+-----------------+-------------------------+

mysql> select k2, array_slice(k2, 2, 2) from array_type_table_nullable;
+-----------------+-------------------------+
| k2              | array_slice(`k2`, 2, 2) |
+-----------------+-------------------------+
| [1, 2, 3]       | [2, 3]                  |
| [1, NULL, 3]    | [NULL, 3]               |
| [2, 3]          | [3]                     |
| NULL            | NULL                    |
+-----------------+-------------------------+

mysql> select k2, k2[2:2] from array_type_table_nullable_varchar;
+----------------------------+-------------------------+
| k2                         | array_slice(`k2`, 2, 2) |
+----------------------------+-------------------------+
| ['hello', 'world', 'c++']  | ['world', 'c++']        |
| ['a1', 'equals', 'b1']     | ['equals', 'b1']        |
| ['hasnull', NULL, 'value'] | [NULL, 'value']         |
| ['hasnull', NULL, 'value'] | [NULL, 'value']         |
+----------------------------+-------------------------+

mysql> select k2, array_slice(k2, 2, 2) from array_type_table_nullable_varchar;
+----------------------------+-------------------------+
| k2                         | array_slice(`k2`, 2, 2) |
+----------------------------+-------------------------+
| ['hello', 'world', 'c++']  | ['world', 'c++']        |
| ['a1', 'equals', 'b1']     | ['equals', 'b1']        |
| ['hasnull', NULL, 'value'] | [NULL, 'value']         |
| ['hasnull', NULL, 'value'] | [NULL, 'value']         |
+----------------------------+-------------------------+
```

Negative off:

```
mysql> select k2, k2[-2:1] from array_type_table_nullable;
+-----------+--------------------------+
| k2        | array_slice(`k2`, -2, 1) |
+-----------+--------------------------+
| [1, 2, 3] | [2]                      |
| [1, 2, 3] | [2]                      |
| [2, 3]    | [2]                      |
| [2, 3]    | [2]                      |
+-----------+--------------------------+

mysql> select k2, array_slice(k2, -2, 1) from array_type_table_nullable;
+-----------+--------------------------+
| k2        | array_slice(`k2`, -2, 1) |
+-----------+--------------------------+
| [1, 2, 3] | [2]                      |
| [1, 2, 3] | [2]                      |
| [2, 3]    | [2]                      |
| [2, 3]    | [2]                      |
+-----------+--------------------------+

mysql> select k2, k2[-2:2] from array_type_table_nullable_varchar;
+----------------------------+--------------------------+
| k2                         | array_slice(`k2`, -2, 2) |
+----------------------------+--------------------------+
| ['hello', 'world', 'c++']  | ['world', 'c++']         |
| ['a1', 'equals', 'b1']     | ['equals', 'b1']         |
| ['hasnull', NULL, 'value'] | [NULL, 'value']          |
| ['hasnull', NULL, 'value'] | [NULL, 'value']          |
+----------------------------+--------------------------+

mysql> select k2, array_slice(k2, -2, 2) from array_type_table_nullable_varchar;
+----------------------------+--------------------------+
| k2                         | array_slice(`k2`, -2, 2) |
+----------------------------+--------------------------+
| ['hello', 'world', 'c++']  | ['world', 'c++']         |
| ['a1', 'equals', 'b1']     | ['equals', 'b1']         |
| ['hasnull', NULL, 'value'] | [NULL, 'value']          |
| ['hasnull', NULL, 'value'] | [NULL, 'value']          |
+----------------------------+--------------------------+
```

```
mysql> select k2, array_slice(k2, 0) from array_type_table;
+-----------+-------------------------+
| k2        | array_slice(`k2`, 0) |
+-----------+-------------------------+
| [1, 2, 3] | []                      |
+-----------+-------------------------+

mysql> select k2, array_slice(k2, -5) from array_type_table;
+-----------+----------------------+
| k2        | array_slice(`k2`, -5) |
+-----------+----------------------+
| [1, 2, 3] | []                   |
+-----------+----------------------+
```

### keywords

ARRAY,SLICE,ARRAY_SLICE

