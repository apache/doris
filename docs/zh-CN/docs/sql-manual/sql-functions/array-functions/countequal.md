---
{
    "title": "countequal",
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

## countequal

### description

#### Syntax

`BIGINT countequal(ARRAY<T> arr, T value)`

判断数组中包含value元素的个数。返回结果如下：

```
num      - value在array中的数量；
0        - value不存在数组arr中；
NULL     - 如果数组为NULL，或者value为NULL。
```

### notice

`仅支持向量化引擎中使用`

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select *, countEqual(c_array,5) from array_test;
+------+-----------------+--------------------------+
| id   | c_array         | countequal(`c_array`, 5) |
+------+-----------------+--------------------------+
|    1 | [1, 2, 3, 4, 5] |                        1 |
|    2 | [6, 7, 8]       |                        0 |
|    3 | []              |                        0 |
|    4 | NULL            |                     NULL |
+------+-----------------+--------------------------+
```

### keywords

ARRAY,COUNTEQUAL
