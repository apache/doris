---
{
    "title": "PERCENTILE_APPROX",
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

## PERCENTILE_APPROX
### description
#### Syntax

`PERCENTILE_APPROX(expr, DOUBLE p[, DOUBLE compression])`


返回第p个百分位点的近似值，p的值介于0到1之间

compression参数是可选项，可设置范围是[2048, 10000]，值越大，精度越高，内存消耗越大，计算耗时越长。
compression参数未指定或设置的值在[2048, 10000]范围外，以10000的默认值运行

该函数使用固定大小的内存，因此对于高基数的列可以使用更少的内存，可用于计算tp99等统计值

### example
```
MySQL > select `table`, percentile_approx(cost_time,0.99) from log_statis group by `table`;
+---------------------+---------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+

MySQL > select `table`, percentile_approx(cost_time,0.99, 4096) from log_statis group by `table`;
+---------------------+---------------------------+
| table    | percentile_approx(`cost_time`, 0.99, 4096.0) |
+----------+--------------------------------------+
| test     |                                54.21 |
+----------+--------------------------------------+
```

### keywords
PERCENTILE_APPROX,PERCENTILE,APPROX
