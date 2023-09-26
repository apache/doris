---
{
    "title": "ARRAY_COUNT",
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

<version since="2.0">

array_count

</version>

### description

```sql
array_count(lambda, array1, ...)
```


Use lambda expressions as input parameters to perform corresponding expression calculations on the internal data of other input ARRAY parameters. 
Returns the number of elements such that the return value of `lambda(array1[i], ...)` is not 0. Returns 0 if no element is found that satisfies this condition.

There are one or more parameters are input in the lambda expression, which must be consistent with the number of input array columns later.The number of elements of all input arrays must be the same. Legal scalar functions can be executed in lambda, aggregate functions, etc. are not supported.


```
array_count(x->x, array1);
array_count(x->(x%2 = 0), array1);
array_count(x->(abs(x)-1), array1);
array_count((x,y)->(x = y), array1, array2);
```

### notice

`Only supported in vectorized engine`

### example

```
mysql> select array_count(x -> x, [0, 1, 2, 3]);
+--------------------------------------------------------+
| array_count(array_map([x] -> x(0), ARRAY(0, 1, 2, 3))) |
+--------------------------------------------------------+
|                                                      3 |
+--------------------------------------------------------+
1 row in set (0.00 sec)

mysql> select array_count(x -> x > 2, [0, 1, 2, 3]);
+------------------------------------------------------------+
| array_count(array_map([x] -> x(0) > 2, ARRAY(0, 1, 2, 3))) |
+------------------------------------------------------------+
|                                                          1 |
+------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select array_count(x -> x is null, [null, null, null, 1, 2]);
+----------------------------------------------------------------------------+
| array_count(array_map([x] -> x(0) IS NULL, ARRAY(NULL, NULL, NULL, 1, 2))) |
+----------------------------------------------------------------------------+
|                                                                          3 |
+----------------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select array_count(x -> power(x,2)>10, [1, 2, 3, 4, 5]);
+------------------------------------------------------------------------------+
| array_count(array_map([x] -> power(x(0), 2.0) > 10.0, ARRAY(1, 2, 3, 4, 5))) |
+------------------------------------------------------------------------------+
|                                                                            2 |
+------------------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select *, array_count((x, y) -> x>y, c_array1, c_array2) from array_test;
+------+-----------------+-------------------------+-----------------------------------------------------------------------+
| id   | c_array1        | c_array2                | array_count(array_map([x, y] -> x(0) > y(1), `c_array1`, `c_array2`)) |
+------+-----------------+-------------------------+-----------------------------------------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] |                                                                     2 |
|    2 | [6, 7, 8]       | [10, 12, 13]            |                                                                     0 |
|    3 | [1]             | [-100]                  |                                                                     1 |
|    4 | [1, NULL, 2]    | [NULL, 3, 1]            |                                                                     1 |
|    5 | []              | []                      |                                                                     0 |
|    6 | NULL            | NULL                    |                                                                     0 |
+------+-----------------+-------------------------+-----------------------------------------------------------------------+
6 rows in set (0.02 sec)

```

### keywords

ARRAY, COUNT, ARRAY_COUNT

