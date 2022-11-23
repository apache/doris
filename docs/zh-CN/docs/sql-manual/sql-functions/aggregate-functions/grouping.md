---
{
    "title": "GROUPING",
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

## GROUPING
### description
#### Syntax

`GROUPING(expr)`

GROUPING用在含有CUBE 或 ROLLUP 语句的SQL语句中，当结果集中的数据行是由CUBE 或 ROLLUP 运算产生的则该函数返回1，否则返回0。

### example
```
MySQL > SELECT COL1,GROUPING(COL2) AS 'Grouping' FROM tbl GROUP BY ROLLUP (COL1, COL2);
+------+----------+
| COL1 | Grouping |
+------+----------+
| NULL |        1 |
| 2.20 |        1 |
| 2.20 |        0 |
| 1.10 |        1 |
| 1.10 |        0 |
+------+----------+
```
### keywords
GROUPING
