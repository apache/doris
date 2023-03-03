---
{
    "title": "GROUP_CONCAT",
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

## GROUP_CONCAT
### description
#### Syntax

`VARCHAR GROUP_CONCAT([DISTINCT] VARCHAR str[, VARCHAR sep]) [ORDER BY { col_name | expr} [ASC | DESC])`


This function is an aggregation function similar to sum (), and group_concat links multiple rows of results in the result set to a string. The second parameter, sep, is a connection symbol between strings, which can be omitted. This function usually needs to be used with group by statements.

<version since="1.2"></version>
Support Order By for sorting multi-row results, sorting and aggregation columns can be different.

### example

```
mysql> select value from test;
+-------+
| value |
+-------+
| a     |
| b     |
| c     |
| c     |
+-------+

mysql> select GROUP_CONCAT(value) from test;
+-----------------------+
| GROUP_CONCAT(`value`) |
+-----------------------+
| a, b, c, c              |
+-----------------------+

mysql> select GROUP_CONCAT(value, " ") from test;
+----------------------------+
| GROUP_CONCAT(`value`, ' ') |
+----------------------------+
| a b c c                     |
+----------------------------+

mysql> select GROUP_CONCAT(DISTINCT value) from test;
+-----------------------+
| GROUP_CONCAT(`value`) |
+-----------------------+
| a, b, c               |
+-----------------------+

mysql> select GROUP_CONCAT(value, NULL) from test;
+----------------------------+
| GROUP_CONCAT(`value`, NULL)|
+----------------------------+
| NULL                       |
+----------------------------+

SELECT abs(k3), group_concat(distinct cast(abs(k2) as varchar) order by abs(k1), k5) FROM bigtable group by abs(k3) order by abs(k3);     +------------+-------------------------------------------------------------------------------+
| abs(`k3`)  | group_concat(DISTINCT CAST(abs(`k2`) AS CHARACTER), ORDER BY abs(`k1`), `k5`) |
+------------+-------------------------------------------------------------------------------+
|        103 | 255                                                                           |
|       1001 | 1989, 1986                                                                    |
|       1002 | 1989, 32767                                                                   |
|       3021 | 1991, 32767, 1992                                                             |
|       5014 | 1985, 1991                                                                    |
|      25699 | 1989                                                                          |
| 2147483647 | 255, 1991, 32767, 32767                                                       |
+------------+-------------------------------------------------------------------------------+
```

```
### keywords
GROUP_CONCAT,GROUP,CONCAT
