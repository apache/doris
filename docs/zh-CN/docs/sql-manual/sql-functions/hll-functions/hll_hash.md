---
{
    "title": "HLL_HASH",
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

## HLL_HASH
### description
#### Syntax

`HLL_HASH(value)`

HLL_HASH 将一个值转换为 hll 类型。通常用于导入数据时，将普通类型的值导入到 hll 列中。

### example
```
MySQL > select HLL_CARDINALITY(HLL_HASH('abc'));
+----------------------------------+
| hll_cardinality(HLL_HASH('abc')) |
+----------------------------------+
|                                1 |
+----------------------------------+
```
### keywords
HLL,HLL_HASH
