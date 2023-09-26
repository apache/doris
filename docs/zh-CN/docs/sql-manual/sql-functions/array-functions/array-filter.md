---
{
    "title": "ARRAY_FILTER",
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

## array_filter

<version since="2.0">

array_filter(lambda,array)

</version>

<version since="2.0.2">

array array_filter(array arr, array_bool filter_column)

</version>

### description

#### Syntax
```sql
ARRAY<T> array_filter(lambda, ARRAY<T> arr)
ARRAY<T> array_filter(ARRAY<T> arr, ARRAY<Bool> filter_column)
```

使用lambda表达式作为输入参数，计算筛选另外的输入参数ARRAY列的数据。
并过滤掉在结果中0和NULL的值。

```
array_filter(x->x>0, array1);
array_filter(x->(x+2)=10, array1);
array_filter(x->(abs(x)-2)>0, array1);
array_filter(c_array,[0,1,0]);
```

### example

```shell
mysql [test]>select c_array,array_filter(c_array,[0,1,0]) from array_test;
+-----------------+----------------------------------------------------+
| c_array         | array_filter(`c_array`, ARRAY(FALSE, TRUE, FALSE)) |
+-----------------+----------------------------------------------------+
| [1, 2, 3, 4, 5] | [2]                                                |
| [6, 7, 8]       | [7]                                                |
| []              | []                                                 |
| NULL            | NULL                                               |
+-----------------+----------------------------------------------------+

mysql [test]>select array_filter(x->(x > 1),[1,2,3,0,null]);
+----------------------------------------------------------------------------------------------+
| array_filter(ARRAY(1, 2, 3, 0, NULL), array_map([x] -> (x(0) > 1), ARRAY(1, 2, 3, 0, NULL))) |
+----------------------------------------------------------------------------------------------+
| [2, 3]                                                                                       |
+----------------------------------------------------------------------------------------------+

mysql [test]>select *, array_filter(x->x>0,c_array2) from array_test2;
+------+-----------------+-------------------------+------------------------------------------------------------------+
| id   | c_array1        | c_array2                | array_filter(`c_array2`, array_map([x] -> x(0) > 0, `c_array2`)) |
+------+-----------------+-------------------------+------------------------------------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [10, 20, 80]                                                     |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [10, 12, 13]                                                     |
|    3 | [1]             | [-100]                  | []                                                               |
|    4 | NULL            | NULL                    | NULL                                                             |
+------+-----------------+-------------------------+------------------------------------------------------------------+
4 rows in set (0.01 sec)

mysql [test]>select *, array_filter(x->x%2=0,c_array2) from array_test2;
+------+-----------------+-------------------------+----------------------------------------------------------------------+
| id   | c_array1        | c_array2                | array_filter(`c_array2`, array_map([x] -> x(0) % 2 = 0, `c_array2`)) |
+------+-----------------+-------------------------+----------------------------------------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [10, 20, -40, 80, -100]                                              |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [10, 12]                                                             |
|    3 | [1]             | [-100]                  | [-100]                                                               |
|    4 | NULL            | NULL                    | NULL                                                                 |
+------+-----------------+-------------------------+----------------------------------------------------------------------+

mysql [test]>select *, array_filter(x->(x*(-10)>0),c_array2) from array_test2;
+------+-----------------+-------------------------+----------------------------------------------------------------------------+
| id   | c_array1        | c_array2                | array_filter(`c_array2`, array_map([x] -> (x(0) * (-10) > 0), `c_array2`)) |
+------+-----------------+-------------------------+----------------------------------------------------------------------------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [-40, -100]                                                                |
|    2 | [6, 7, 8]       | [10, 12, 13]            | []                                                                         |
|    3 | [1]             | [-100]                  | [-100]                                                                     |
|    4 | NULL            | NULL                    | NULL                                                                       |
+------+-----------------+-------------------------+----------------------------------------------------------------------------+

mysql [test]>select *, array_filter(x->x>0, array_map((x,y)->(x>y), c_array1,c_array2)) as res from array_test2;
+------+-----------------+-------------------------+--------+
| id   | c_array1        | c_array2                | res    |
+------+-----------------+-------------------------+--------+
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [1, 1] |
|    2 | [6, 7, 8]       | [10, 12, 13]            | []     |
|    3 | [1]             | [-100]                  | [1]    |
|    4 | NULL            | NULL                    | NULL   |
+------+-----------------+-------------------------+--------+
```

### keywords

ARRAY,FILTER,ARRAY_FILTER

