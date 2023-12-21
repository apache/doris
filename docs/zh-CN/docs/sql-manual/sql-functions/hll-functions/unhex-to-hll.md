---
{
    "title": "UNHEX_TO_HLL",
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

## UNHEX_TO_HLL
### description
#### Syntax

`UNHEX_TO_HLL(value)`

使用场景：从doris查出hll的二进制数据，经过计算后，可以再把二进制数据导回doris。

输入为hll二进制数据的十六进制格式字符串，前两位表示hll存储类型：0为空；1为显式存储（hash值个数不大于160）；2为存储非0registers（registers个数小于4096）；3为存储所有registers。后面16位为采用小端格式的数据内容。
输出为hll对象。
当输入值不在此范围时，会返回NULL。
### example
```
mysql> select HLL_CARDINALITY(unhex_to_hll('01010600000000000000'));
+-------------------------------------------------------+
| hll_cardinality(unhex_to_hll('01010600000000000000')) |
+-------------------------------------------------------+
|                                                     1 |
+-------------------------------------------------------+
```
### keywords
HLL,UNHEX_TO_HLL
