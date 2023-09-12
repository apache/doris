---
{
    "title": "HLL_UNION_AGG",
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

## HLL_UNION_AGG
### description
#### Syntax

`HLL_UNION_AGG(hll)`


HLL是基于HyperLogLog算法的工程实现，用于保存HyperLogLog计算过程的中间结果

它只能作为表的value列类型、通过聚合来不断的减少数据量，以此来实现加快查询的目的

基于它得到的是一个估算结果，误差大概在1%左右，hll列是通过其它列或者导入数据里面的数据生成的

导入的时候通过hll_hash函数来指定数据中哪一列用于生成hll列，它常用于替代count distinct，通过结合rollup在业务上用于快速计算uv等

### example
```
MySQL > select HLL_UNION_AGG(uv_set) from test_uv;;
+-------------------------+
| HLL_UNION_AGG(`uv_set`) |
+-------------------------+
| 17721                   |
+-------------------------+
```
### keywords
HLL_UNION_AGG,HLL,UNION,AGG
