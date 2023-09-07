---
{
    "title": "PERCENTILE",
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

## PERCENTILE
### description
#### Syntax

`PERCENTILE(expr, DOUBLE p)`

计算精确的百分位数，适用于小数据量。先对指定列降序排列，然后取精确的第 p 位百分数。p的值介于0到1之间

参数说明
expr：必填。值为整数（最大为bigint） 类型的列。
p：常量必填。需要精确的百分位数。取值为 [0.0,1.0]。

### example
```
MySQL > select `table`, percentile(cost_time,0.99) from log_statis group by `table`;
+---------------------+---------------------------+
| table    |        percentile(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+

MySQL > select percentile(NULL,0.3) from table1;
+-----------------------+
| percentile(NULL, 0.3) |
+-----------------------+
|                  NULL |
+-----------------------+
```

### keywords
PERCENTILE
