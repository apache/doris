---
{
    "title": "ARRAY_SORTBY",
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

## array_sortby

<version since="2.0">

array_sortby

</version>

### description

#### Syntax

```sql
ARRAY<T> array_sortby(ARRAY<T> src,Array<T> key)
ARRAY<T> array_sortby(lambda,array....)
```

首先将key列升序排列，然后将src列按此顺序排序后的对应列做为结果返回;
如果输入数组src为NULL，则返回NULL。
如果输入数组key为NULL，则直接返回src数组。
如果输入数组key元素包含NULL, 则输出的排序数组会将NULL放在最前面。

### notice

`仅支持向量化引擎中使用`

### example

```
mysql [test]>select array_sortby(['a','b','c'],[3,2,1]);
+----------------------------------------------------+
| array_sortby(ARRAY('a', 'b', 'c'), ARRAY(3, 2, 1)) |
+----------------------------------------------------+
| ['c', 'b', 'a']                                    |
+----------------------------------------------------+

mysql [test]>select array_sortby([1,2,3,4,5],[10,5,1,20,80]);
+-------------------------------------------------------------+
| array_sortby(ARRAY(1, 2, 3, 4, 5), ARRAY(10, 5, 1, 20, 80)) |
+-------------------------------------------------------------+
| [3, 2, 1, 4, 5]                                             |
+-------------------------------------------------------------+

mysql [test]>select *,array_sortby(c_array1,c_array2) from test_array_sortby order by id;
+------+-----------------+-------------------------+--------------------------------------+
| id   | c_array1        | c_array2                | array_sortby(`c_array1`, `c_array2`) |
+------+-----------------+-------------------------+--------------------------------------+
|    0 | NULL            | [2]                     | NULL                                 |
|    1 | [1, 2, 3, 4, 5] | [10, 20, -40, 80, -100] | [5, 3, 1, 2, 4]                      |
|    2 | [6, 7, 8]       | [10, 12, 13]            | [6, 7, 8]                            |
|    3 | [1]             | [-100]                  | [1]                                  |
|    4 | NULL            | NULL                    | NULL                                 |
|    5 | [3]             | NULL                    | [3]                                  |
|    6 | [1, 2]          | [2, 1]                  | [2, 1]                               |
|    7 | [NULL]          | [NULL]                  | [NULL]                               |
|    8 | [1, 2, 3]       | [3, 2, 1]               | [3, 2, 1]                            |
+------+-----------------+-------------------------+--------------------------------------+

mysql [test]>select *, array_map((x,y)->(y+x),c_array1,c_array2) as arr_sum,array_sortby((x,y)->(y+x),c_array1,c_array2) as arr_sort from array_test2;
+------+-----------------+--------------+----------------+-----------------+
| id   | c_array1        | c_array2     | arr_sum        | arr_sort        |
+------+-----------------+--------------+----------------+-----------------+
|    1 | [1, 2, 3]       | [10, 11, 12] | [11, 13, 15]   | [1, 2, 3]       |
|    2 | [4, 3, 5]       | [10, 20, 30] | [14, 23, 35]   | [4, 3, 5]       |
|    3 | [-40, 30, -100] | [30, 10, 20] | [-10, 40, -80] | [-100, -40, 30] |
+------+-----------------+--------------+----------------+-----------------+
```

### keywords

ARRAY, SORT, ARRAY_SORTBY
