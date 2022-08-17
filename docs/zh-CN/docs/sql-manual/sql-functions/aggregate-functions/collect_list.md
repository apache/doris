---
{
    "title": "COLLECT_LIST",
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

## COLLECT_LIST
### description
#### Syntax

`ARRAY<T> collect_list(expr)`

返回一个包含 expr 中所有元素(不包括NULL)的数组，数组中元素顺序是不确定的。


### notice

```
仅支持向量化引擎中使用
```

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select k1,k2,k3 from collect_test order by k1;
+------+------------+-------+
| k1   | k2         | k3    |
+------+------------+-------+
|    1 | 2022-07-05 | hello |
|    2 | 2022-07-04 | NULL  |
|    2 | 2022-07-04 | hello |
|    3 | NULL       | world |
|    3 | NULL       | world |
+------+------------+-------+

mysql> select k1,collect_list(k2),collect_list(k3) from collect_test group by k1 order by k1;
+------+--------------------------+--------------------+
| k1   | collect_list(`k2`)       | collect_list(`k3`) |
+------+--------------------------+--------------------+
|    1 | [2022-07-05]             | [hello]            |
|    2 | [2022-07-04, 2022-07-04] | [hello]            |
|    3 | NULL                     | [world, world]     |
+------+--------------------------+--------------------+

```

### keywords
COLLECT_LIST,COLLECT_SET,ARRAY
