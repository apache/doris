---
{
    "title": "NAMED_STRUCT",
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

## named_struct

<version since="2.0.0">

named_struct

</version>

### description

#### Syntax

`STRUCT<T1, T2, T3, ...> named_struct({VARCHAR, T1}, {VARCHAR, T2}, ...)`

根据给定的字符串和值构造并返回struct

参数个数必须为非0偶数，奇数位是field的名字，必须为常量字符串，偶数位是field的值，可以是多列或常量

### notice

`仅支持向量化引擎中使用`

### example

```
mysql> select named_struct('f1', 1, 'f2', 'a', 'f3', "abc");
+-----------------------------------------------+
| named_struct('f1', 1, 'f2', 'a', 'f3', 'abc') |
+-----------------------------------------------+
| {1, 'a', 'abc'}                               |
+-----------------------------------------------+
1 row in set (0.01 sec)

mysql> select named_struct('a', null, 'b', "v");
+-----------------------------------+
| named_struct('a', NULL, 'b', 'v') |
+-----------------------------------+
| {NULL, 'v'}                       |
+-----------------------------------+
1 row in set (0.01 sec)

mysql> select named_struct('f1', k1, 'f2', k2, 'f3', null) from test_tb;
+--------------------------------------------------+
| named_struct('f1', `k1`, 'f2', `k2`, 'f3', NULL) |
+--------------------------------------------------+
| {1, 'a', NULL}                                   |
+--------------------------------------------------+
1 row in set (0.02 sec)
```

### keywords

NAMED, STRUCT, NAMED_STRUCT