---
{
"title": "SUBSTR",
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

## substr
### description
#### Syntax

`VARCHAR substr(VARCHAR content, INT start, INT length)`

求子字符串，返回第一个参数描述的字符串中从start开始长度为len的部分字符串。首字母的下标为1。
如果参数中含有NULL值，那么函数始终返回NULL，如果只输入两个参数那么唯一的整型参数为 start 返回 start 右边部分。

### example

```
mysql> select substr("Hello doris", 2, 1);
+-----------------------------+
| substr('Hello doris', 2, 1) |
+-----------------------------+
| e                           |
+-----------------------------+
mysql> select substr("Hello doris", 1, 2);
+-----------------------------+
| substr('Hello doris', 1, 2) |
+-----------------------------+
| He                          |
+-----------------------------+
mysql>  select substr("Hello doris", 7);
+-----------------------------------------+
| substring('Hello doris', 7, 2147483647) |
+-----------------------------------------+
| doris                                   |
+-----------------------------------------+
mysql>  select substr("Hello doris", -7);
+------------------------------------------+
| substring('Hello doris', -7, 2147483647) |
+------------------------------------------+
| o doris                                  |
+------------------------------------------+
mysql>  select substr("Hello doris", NULL, 10);
+------------------------------------+
| substring('Hello doris', NULL, 10) |
+------------------------------------+
| NULL                               |
+------------------------------------+
```
### keywords
    SUBSTR
