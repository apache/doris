---
{
    "title": "ARRAY_LAST_INDEX",
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

## array_last_index

<version since="2.0">

array_last_index

</version>

### description

#### Syntax

`ARRAY<T> array_last_index(lambda, ARRAY<T> array1, ...)`

使用lambda表达式作为输入参数，对其他输入ARRAY参数的内部数据进行相应的表达式计算。 返回最后一个使得 `lambda(array1[i], ...)` 返回值不为 0 的索引。如果没找到满足此条件的索引，则返回 0。

在lambda表达式中输入的参数为1个或多个，所有输入的array的元素数量必须一致。在lambda中可以执行合法的标量函数，不支持聚合函数等。

```
array_last_index(x->x>1, array1);
array_last_index(x->(x%2 = 0), array1);
array_last_index(x->(abs(x)-1), array1);
array_last_index((x,y)->(x = y), array1, array2);
```

### example

```
mysql> select array_last_index(x->x+1>3, [2, 3, 4]);
+-------------------------------------------------------------------+
| array_last_index(array_map([x] -> x(0) + 1 > 3, ARRAY(2, 3, 4))) |
+-------------------------------------------------------------------+
|                                                                 3 |
+-------------------------------------------------------------------+

mysql> select array_last_index(x -> x is null, [null, 1, 2]);
+----------------------------------------------------------------------+
| array_last_index(array_map([x] -> x(0) IS NULL, ARRAY(NULL, 1, 2))) |
+----------------------------------------------------------------------+
|                                                                    1 |
+----------------------------------------------------------------------+

mysql> select array_last_index(x->power(x,2)>10, [1, 2, 3, 4]);
+---------------------------------------------------------------------------------+
| array_last_index(array_map([x] -> power(x(0), 2.0) > 10.0, ARRAY(1, 2, 3, 4))) |
+---------------------------------------------------------------------------------+
|                                                                               4 |
+---------------------------------------------------------------------------------+

mysql> select c_array1, c_array2, array_last_index((x,y)->x>y, c_array1, c_array2) from array_index_table order by id;
+-----------------+-------------------------+----------------------------------------------------------------------+
| c_array1        | c_array2                | array_last_index(array_map([x, y] -> x > y, `c_array1`, `c_array2`)) |
+-----------------+-------------------------+----------------------------------------------------------------------+
| [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] |                                                                    5 |
| [6, 7, 8]       | [10, 12, 13]            |                                                                    0 |
| [1]             | [-100]                  |                                                                    1 |
| [1, NULL, 2]    | [NULL, 3, 1]            |                                                                    3 |
| []              | []                      |                                                                    0 |
| NULL            | NULL                    |                                                                    0 |
+-----------------+-------------------------+----------------------------------------------------------------------+
```

### keywords

ARRAY,FIRST_INDEX,array_last_index