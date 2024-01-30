---
{
    "title": "ARRAY_MAP",
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

## array_map

<version since="dev">

array_map(lambda,array1,array2....)

</version>

### description

#### Syntax
`ARRAY<T> array_map(lambda, ARRAY<T> array1, ARRAY<T> array2)`

使用一个lambda表达式作为输入参数，对其他的输入ARRAY参数的内部数据做对应表达式计算。
在lambda表达式中输入的参数为1个或多个，必须和后面的输入array列数量一致。
在lambda中可以执行合法的标量函数，不支持聚合函数等。

```
array_map(x->x, array1);
array_map(x->(x+2), array1);
array_map(x->(abs(x)-2), array1);

array_map((x,y)->(x = y), array1, array2);
array_map((x,y)->(power(x,2)+y), array1, array2);
array_map((x,y,z)->(abs(x)+y*z), array1, array2, array3);
```

### example

```shell

mysql [test]>select *, array_map(x->x,[1,2,3]) from array_test2 order by id;
+------+-----------------+-------------------------+----------------------------------------+
| id   | c_array1        | c_array2                | array_map([x] -> x(0), ARRAY(1, 2, 3)) |
+------+-----------------+-------------------------+----------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [1, 2, 3]                              |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [1, 2, 3]                              |
|    3 | [1]             | [-100]                  | [1, 2, 3]                              |
|    4 | NULL            | NULL                    | [1, 2, 3]                              |
+------+-----------------+-------------------------+----------------------------------------+
4 rows in set (0.02 sec)

mysql [test]>select *, array_map(x->x+2,[1,2,3]) from array_test2 order by id;
+------+-----------------+-------------------------+--------------------------------------------+
| id   | c_array1        | c_array2                | array_map([x] -> x(0) + 2, ARRAY(1, 2, 3)) |
+------+-----------------+-------------------------+--------------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [3, 4, 5]                                  |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [3, 4, 5]                                  |
|    3 | [1]             | [-100]                  | [3, 4, 5]                                  |
|    4 | NULL            | NULL                    | [3, 4, 5]                                  |
+------+-----------------+-------------------------+--------------------------------------------+
4 rows in set (0.02 sec)

mysql [test]>select c_array1, c_array2, array_map(x->x,[1,2,3]) from array_test2 order by id;
+-----------------+-------------------------+----------------------------------------+
| c_array1        | c_array2                | array_map([x] -> x(0), ARRAY(1, 2, 3)) |
+-----------------+-------------------------+----------------------------------------+
| [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [1, 2, 3]                              |
| [6, 7, 8]       | [10, 12, 13]            | [1, 2, 3]                              |
| [1]             | [-100]                  | [1, 2, 3]                              |
| NULL            | NULL                    | [1, 2, 3]                              |
+-----------------+-------------------------+----------------------------------------+
4 rows in set (0.01 sec)

mysql [test]>select c_array1, c_array2, array_map(x->power(x,2),[1,2,3]) from array_test2 order by id;
+-----------------+-------------------------+----------------------------------------------------+
| c_array1        | c_array2                | array_map([x] -> power(x(0), 2.0), ARRAY(1, 2, 3)) |
+-----------------+-------------------------+----------------------------------------------------+
| [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [1, 4, 9]                                          |
| [6, 7, 8]       | [10, 12, 13]            | [1, 4, 9]                                          |
| [1]             | [-100]                  | [1, 4, 9]                                          |
| NULL            | NULL                    | [1, 4, 9]                                          |
+-----------------+-------------------------+----------------------------------------------------+

mysql [test]>select c_array1, c_array2, array_map((x,y)->x+y,c_array1,c_array2) from array_test2 order by id;
+-----------------+-------------------------+----------------------------------------------------------+
| c_array1        | c_array2                | array_map([x, y] -> x(0) + y(1), `c_array1`, `c_array2`) |
+-----------------+-------------------------+----------------------------------------------------------+
| [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [11, 22, -37, 84, -95]                                   |
| [6, 7, 8]       | [10, 12, 13]            | [16, 19, 21]                                             |
| [1]             | [-100]                  | [-99]                                                    |
| NULL            | NULL                    | NULL                                                     |
+-----------------+-------------------------+----------------------------------------------------------+
4 rows in set (0.02 sec)

mysql [test]>select c_array1, c_array2, array_map((x,y)->power(x,2)+y,c_array1, c_array2) from array_test2 order by id;
+-----------------+-------------------------+----------------------------------------------------------------------+
| c_array1        | c_array2                | array_map([x, y] -> power(x(0), 2.0) + y(1), `c_array1`, `c_array2`) |
+-----------------+-------------------------+----------------------------------------------------------------------+
| [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [11, 24, -31, 96, -75]                                               |
| [6, 7, 8]       | [10, 12, 13]            | [46, 61, 77]                                                         |
| [1]             | [-100]                  | [-99]                                                                |
| NULL            | NULL                    | NULL                                                                 |
+-----------------+-------------------------+----------------------------------------------------------------------+
4 rows in set (0.03 sec)

mysql [test]>select *,array_map(x->x=3,c_array1) from array_test2 order by id;
+------+-----------------+-------------------------+----------------------------------------+
| id   | c_array1        | c_array2                | array_map([x] -> x(0) = 3, `c_array1`) |
+------+-----------------+-------------------------+----------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [0, 0, 1, 0, 0]                        |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [0, 0, 0]                              |
|    3 | [1]             | [-100]                  | [0]                                    |
|    4 | NULL            | NULL                    | NULL                                   |
+------+-----------------+-------------------------+----------------------------------------+
4 rows in set (0.02 sec)

mysql [test]>select *,array_map(x->x>3,c_array1) from array_test2 order by id;
+------+-----------------+-------------------------+----------------------------------------+
| id   | c_array1        | c_array2                | array_map([x] -> x(0) > 3, `c_array1`) |
+------+-----------------+-------------------------+----------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [0, 0, 0, 1, 1]                        |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [1, 1, 1]                              |
|    3 | [1]             | [-100]                  | [0]                                    |
|    4 | NULL            | NULL                    | NULL                                   |
+------+-----------------+-------------------------+----------------------------------------+
4 rows in set (0.02 sec)

mysql [test]>select *,array_map((x,y)->x>y,c_array1,c_array2) from array_test2 order by id;
+------+-----------------+-------------------------+----------------------------------------------------------+
| id   | c_array1        | c_array2                | array_map([x, y] -> x(0) > y(1), `c_array1`, `c_array2`) |
+------+-----------------+-------------------------+----------------------------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [0, 0, 1, 0, 1]                                          |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [0, 0, 0]                                                |
|    3 | [1]             | [-100]                  | [1]                                                      |
|    4 | NULL            | NULL                    | NULL                                                     |
+------+-----------------+-------------------------+----------------------------------------------------------+
4 rows in set (0.02 sec)

mysql [test]>select array_map(x->cast(x as string), c_array1) from test_array_map_function;
+-----------------+-------------------------------------------------------+
| c_array1        | array_map([x] -> CAST(x(0) AS CHARACTER), `c_array1`) |
+-----------------+-------------------------------------------------------+
| [1, 2, 3, 4, 5] | ['1', '2', '3', '4', '5']                             |
| [6, 7, 8]       | ['6', '7', '8']                                       |
| []              | []                                                    |
| NULL            | NULL                                                  |
+-----------------+-------------------------------------------------------+
4 rows in set (0.01 sec)

```

### keywords

ARRAY,MAP,ARRAY_MAP

