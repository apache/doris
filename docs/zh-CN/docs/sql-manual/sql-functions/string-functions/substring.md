---
{
    "title": "SUBSTRING",
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

## substring
### description
#### Syntax

`VARCHAR substring(VARCHAR str, INT pos[, INT len])`

没有 `len` 参数时返回从位置 `pos` 开始的字符串 `str` 的一个子字符串，
在有 `len` 参数时返回从位置 `pos` 开始的字符串 `str` 的一个长度为 `len` 子字符串，
`pos` 参数可以使用负值，在这种情况下，子字符串是以字符串 `str` 末尾开始计算 `pos` 个字符，而不是开头,
`pos` 的值为 0 返回一个空字符串。

对于所有形式的 SUBSTRING()，要从中提取子字符串的字符串中第一个字符的位置为1。

### example

```
mysql> select substring('abc1', 2);
+-----------------------------+
| substring('abc1', 2)        |
+-----------------------------+
| bc1                         |
+-----------------------------+

mysql> select substring('abc1', -2);
+-----------------------------+
| substring('abc1', -2)       |
+-----------------------------+
| c1                          |
+-----------------------------+

mysql> select substring('abc1', 0);
+----------------------+
| substring('abc1', 0) |
+----------------------+
|                      |
+----------------------+

mysql> select substring('abc1', 5);
+-----------------------------+
| substring('abc1', 5)        |
+-----------------------------+
| NULL                        |
+-----------------------------+

mysql> select substring('abc1def', 2, 2);
+-----------------------------+
| substring('abc1def', 2, 2)  |
+-----------------------------+
| bc                          |
+-----------------------------+
```
### keywords
    SUBSTRING, STRING
