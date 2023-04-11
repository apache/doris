---
{
    "title": "array_count",
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

## array_count

<version since="1.2.0">

array_count

</version>

### description

#### Syntax

`BIGINT array_count(ARRAY<T> arr)`

Returns the number of non-zero and non-null elements in the given array.

### notice

`Only supported in vectorized engine`

### example

```
mysql> select array_count([]);
+----------------------+
| array_count(ARRAY()) |
+----------------------+
|                    0 |
+----------------------+
1 row in set (0.02 sec)

mysql> select array_count([0,0,1,2,4,8,16,32]);
+----------------------------------------------+
| array_count(ARRAY(0, 0, 1, 2, 4, 8, 16, 32)) |
+----------------------------------------------+
|                                            6 |
+----------------------------------------------+
1 row in set (0.01 sec)

mysql> select array_count([0,0,0,0]);
+--------------------------------+
| array_count(ARRAY(0, 0, 0, 0)) |
+--------------------------------+
|                              0 |
+--------------------------------+
1 row in set (0.01 sec)

mysql> select array_count([null, null, null]); 
+--------------------------------------+
| array_count(ARRAY(NULL, NULL, NULL)) |
+--------------------------------------+
|                                    0 |
+--------------------------------------+
1 row in set (0.00 sec)

mysql> select array_count(NULL);
+-------------------+
| array_count(NULL) |
+-------------------+
|              NULL |
+-------------------+
1 row in set (0.01 sec)

```

### keywords

ARRAY, COUNT, ARRAY_COUNT

