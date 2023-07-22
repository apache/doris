---
{
    "title": "STATE",
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

## STATE

<version since="2.0.0">
</version>


### description
#### Syntax

`AGGREGATE_FUNCTION_STATE(arg...)`
返回聚合函数的中间结果，可以用于后续的聚合或者通过merge组合器获得实际计算结果，也可以直接写入agg_state类型的表保存下来。
结果的类型为agg_state，agg_state中的函数签名为`AGGREGATE_FUNCTION(arg...)`。

### example
```
mysql [test]>select avg_merge(t) from (select avg_union(avg_state(1)) as t from d_table group by k1)p;
+----------------+
| avg_merge(`t`) |
+----------------+
|              1 |
+----------------+
```
### keywords
AGG_STATE,STATE
