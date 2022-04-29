---
{
    "title": "PERCENTILE_APPROX",
    "language": "en"
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
### Description
#### Syntax

`PERCENTILE_APPROX(expr, DOUBLE p[, DOUBLE compression])`

Return the approximation of the point p, where the value of P is between 0 and 1.

Compression param is optional and can be setted to a value in the range of [2048, 10000]. The bigger compression you set, the more precise result and more time cost you will get. If it is not setted or not setted in the correct range, PERCENTILE_APPROX function will run with a default compression param of 10000.

This function uses fixed size memory, so less memory can be used for columns with high cardinality, and can be used to calculate statistics such as tp99.

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
