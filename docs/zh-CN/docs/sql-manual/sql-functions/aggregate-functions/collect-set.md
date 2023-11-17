---
{
    "title": "COLLECT_SET",
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

## COLLECT_SET

<version since="1.2.0">

COLLECT_SET

</version>

### description
#### Syntax

`ARRAY<T> collect_set(expr[,max_size])`

返回一个对`expr`去重后的数组。可选参数`max_size`，通过设置该参数能够将结果数组的大小限制为 `max_size` 个元素。
得到的结果数组中不包含NULL元素，数组中的元素顺序不固定。该函数具有别名`group_uniq_array`。

### notice

```
仅支持向量化引擎中使用
```

### example

```
mysql> set enable_vectorized_engine=true;

mysql> select k1,k2,k3 from collect_set_test order by k1;
+------+------------+-------+
| k1   | k2         | k3    |
+------+------------+-------+
|    1 | 2023-01-01 | hello |
|    2 | 2023-01-01 | NULL  |
|    2 | 2023-01-02 | hello |
|    3 | NULL       | world |
|    3 | 2023-01-02 | hello |
|    4 | 2023-01-02 | doris |
|    4 | 2023-01-03 | sql   |
+------+------------+-------+

mysql> select collect_set(k1),collect_set(k1,2) from collect_set_test;
+-------------------------+--------------------------+
| collect_set(`k1`)       | collect_set(`k1`,2)      |
+-------------------------+--------------------------+
| [4,3,2,1]               | [1,2]                    |
+----------------------------------------------------+

mysql> select k1,collect_set(k2),collect_set(k3,1) from collect_set_test group by k1 order by k1;
+------+-------------------------+--------------------------+
| k1   | collect_set(`k2`)       | collect_set(`k3`,1)      |
+------+-------------------------+--------------------------+
|    1 | [2023-01-01]            | [hello]                  |
|    2 | [2023-01-01,2023-01-02] | [hello]                  |
|    3 | [2023-01-02]            | [world]                  |
|    4 | [2023-01-02,2023-01-03] | [sql]                    |
+------+-------------------------+--------------------------+

```

### keywords
COLLECT_SET,GROUP_UNIQ_ARRAY,COLLECT_LIST,ARRAY
