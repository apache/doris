---
{
    "title": "ARRAY_FIRST_INDEX",
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

## array_first_index

<version since="2.0">

array_first_index

</version>

### description

#### Syntax

`ARRAY<T> array_first_index(lambda, ARRAY<T> array1, ...)`

Use an lambda expression as an input parameter to perform corresponding expression calculations on the internal data of other input ARRAY parameters. Returns the first index such that the return value of `lambda(array1[i], ...)` is not 0. Return 0 if such index is not found.

There are one or more parameters input in the lambda expression, and the number of elements of all input arrays must be the same. Legal scalar functions can be executed in lambda, aggregate functions, etc. are not supported.

```
array_first_index(x->x>1, array1);
array_first_index(x->(x%2 = 0), array1);
array_first_index(x->(abs(x)-1), array1);
array_first_index((x,y)->(x = y), array1, array2);
```

### example

```
mysql> select array_first_index(x->x+1>3, [2, 3, 4]);
+-------------------------------------------------------------------+
| array_first_index(array_map([x] -> x(0) + 1 > 3, ARRAY(2, 3, 4))) |
+-------------------------------------------------------------------+
|                                                                 2 |
+-------------------------------------------------------------------+

mysql> select array_first_index(x -> x is null, [null, 1, 2]);
+----------------------------------------------------------------------+
| array_first_index(array_map([x] -> x(0) IS NULL, ARRAY(NULL, 1, 2))) |
+----------------------------------------------------------------------+
|                                                                    1 |
+----------------------------------------------------------------------+

mysql> select array_first_index(x->power(x,2)>10, [1, 2, 3, 4]);
+---------------------------------------------------------------------------------+
| array_first_index(array_map([x] -> power(x(0), 2.0) > 10.0, ARRAY(1, 2, 3, 4))) |
+---------------------------------------------------------------------------------+
|                                                                               4 |
+---------------------------------------------------------------------------------+

mysql> select col2, col3, array_first_index((x,y)->x>y, col2, col3) from array_test;
+--------------+--------------+---------------------------------------------------------------------+
| col2         | col3         | array_first_index(array_map([x, y] -> x(0) > y(1), `col2`, `col3`)) |
+--------------+--------------+---------------------------------------------------------------------+
| [1, 2, 3]    | [3, 4, 5]    |                                                                   0 |
| [1, NULL, 2] | [NULL, 3, 1] |                                                                   3 |
| [1, 2, 3]    | [9, 8, 7]    |                                                                   0 |
| NULL         | NULL         |                                                                   0 |
+--------------+--------------+---------------------------------------------------------------------+
```

### keywords

ARRAY,FIRST_INDEX,ARRAY_FIRST_INDEX