---
{
    "title": "has_all",
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

## has_all

### description

#### Syntax

`BOOLEAN has_all(ARRAY<T> set, ARRAY<T> subset)`
判断一个数组是否是另一个数组的子集

#### Arguments

`set` – 含有任何类型的数组
`subset` – 任何类型的数组，其元素将被测试是否为另一集合的子集

#### Returned value

`1`- subset是set的子集
`0`- subset不是set的子集
`NULL` - set或subset是NULL


### example

```
 SELECT has_all([1,2,3],[1,2]);
+--------------------------------------+
| has_all(ARRAY(1, 2, 3), ARRAY(1, 2)) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+

SELECT has_all([1.2,2.4,3.5],[]);
+----------------------------------------+
| has_all(ARRAY(1.2, 2.4, 3.5), ARRAY()) |
+----------------------------------------+
|                                      1 |
+----------------------------------------+

SELECT has_all([1,2,3],NULL);
+-------------------------------+
| has_all(ARRAY(1, 2, 3), NULL) |
+-------------------------------+
| NULL                          |
+-------------------------------+

SELECT has_all([100,200,NULL],[100,NULL]);
+--------------------------------------------------+
| has_all(ARRAY(100, 200, NULL), ARRAY(100, NULL)) |
+--------------------------------------------------+
|                                                1 |
+--------------------------------------------------+
```

### keywords

has_all