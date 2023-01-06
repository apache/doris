---
{
    "title": "GROUP_UNIQ_ARRAY",
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

## GROUP_UNIQ_ARRAY
### description
#### Syntax

`ARRAY<T> collect_set(expr[,max_size])`

从不同的参数值创建一个数组，可选参数max_size，将结果数组的大小限制为 `max_size` 个元素。

### notice

```
仅支持向量化引擎中使用
```

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select k1,k2,k3 from group_uniq_array_test order by k1;
+------+------------+-------+
| k1   | k2         | k2    |
+------+------------+-------+
|    1 | 2023-01-01 | hello |
|    2 | 2023-01-01 | NULL  |
|    2 | 2023-01-02 | hello |
|    3 | NULL       | world |
|    3 | 2023-01-02 | hello |
+------+------------+-------+

mysql> select k1,group_uniq_array(k2),group_uniq_array(k3,1) from group_uniq_array_test group by k1 order by k1;
+------+-------------------------+-------------------+
| k1   | collect_set(`k2`)       | collect_set(`k3`) |
+------+-------------------------+-------------------+
|    1 | [2023-01-01]            | [hello]           |
|    2 | [2023-01-01,2023-01-02] | [hello]           |
|    3 | [2023-01-02]            | [world]           |
+------+-------------------------+-------------------+

```

### keywords
GROUP_UNIQ_ARRAY,ARRAY