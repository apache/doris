---
{
    "title": "ARRAY_ZIP",
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

## array_zip

<version since="2.0">

array_zip

</version>

### description

Combines all all arrays into a single array. The resulting array contains the corresponding elements of the source arrays grouped into structs in the listed order of arguments.

#### Syntax

`Array<Struct<T1, T2,...>> array_zip(Array<T1>, Array<T2>, ...)`

#### Returned value

Array with elements from the source arrays grouped into tuples. Data types in the tuple are the same as types of the input arrays and in the same order as arrays are passed.

### notice

`Only supported in vectorized engine`

### example

```
mysql> select array_zip(['a', 'b', 'c'], [1, 2, 3]);
+-------------------------------------------------+
| array_zip(ARRAY('a', 'b', 'c'), ARRAY(1, 2, 3)) |
+-------------------------------------------------+
| [{'a', 1}, {'b', 2}, {'c', 3}]                  |
+-------------------------------------------------+
1 row in set (0.01 sec)
```

### keywords

ARRAY,ZIP,ARRAY_ZIP