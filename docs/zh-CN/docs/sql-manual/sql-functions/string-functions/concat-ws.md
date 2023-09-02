---
{
    "title": "CONCAT_WS",
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

## concat_ws
### description
#### Syntax

```sql
VARCHAR concat_ws(VARCHAR sep, VARCHAR str,...)
VARCHAR concat_ws(VARCHAR sep, ARRAY array)
```


使用第一个参数 sep 作为连接符，将第二个参数以及后续所有参数(或ARRAY中的所有字符串)拼接成一个字符串。
如果分隔符是 NULL，返回 NULL。
`concat_ws`函数不会跳过空字符串，会跳过 NULL 值。

### example

```
mysql> select concat_ws("or", "d", "is");
+----------------------------+
| concat_ws('or', 'd', 'is') |
+----------------------------+
| doris                      |
+----------------------------+

mysql> select concat_ws(NULL, "d", "is");
+----------------------------+
| concat_ws(NULL, 'd', 'is') |
+----------------------------+
| NULL                       |
+----------------------------+

mysql> select concat_ws("or", "d", NULL,"is");
+---------------------------------+
| concat_ws("or", "d", NULL,"is") |
+---------------------------------+
| doris                           |
+---------------------------------+

mysql> select concat_ws("or", ["d", "is"]);
+-----------------------------------+
| concat_ws('or', ARRAY('d', 'is')) |
+-----------------------------------+
| doris                             |
+-----------------------------------+

mysql> select concat_ws(NULL, ["d", "is"]);
+-----------------------------------+
| concat_ws(NULL, ARRAY('d', 'is')) |
+-----------------------------------+
| NULL                              |
+-----------------------------------+

mysql> select concat_ws("or", ["d", NULL,"is"]);
+-----------------------------------------+
| concat_ws('or', ARRAY('d', NULL, 'is')) |
+-----------------------------------------+
| doris                                   |
+-----------------------------------------+
```
### keywords
    CONCAT_WS,CONCAT,WS,ARRAY
