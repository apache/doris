---
{
    "title": "STRRIGHT",
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

## strright
### description
#### Syntax

`VARCHAR strright(VARCHAR str, INT len)`


它返回具有指定长度的字符串的右边部分, 长度的单位为utf8字符。此函数的另一个别名为 `right`。
如果参数中含有NULL值，那么函数始终返回NULL，如果整型参数为负数，那么会得到字符串从第 abs(len) 个字符开始向右的部分。

### example

```
mysql> select strright("Hello doris",5);
+-------------------------+
| strright('Hello doris', 5) |
+-------------------------+
| doris                   |
+-------------------------+
mysql> select strright("Hello doris",-7);
+--------------------------+
| strright('Hello doris', -7) |
+--------------------------+
| doris                    |
+--------------------------+
mysql> select strright("Hello doris",NULL);
+----------------------------+
| strright('Hello doris', NULL) |
+----------------------------+
| NULL                       |
+----------------------------+
mysql> select strright(NULL,5);
+----------------+
| strright(NULL, 5) |
+----------------+
| NULL           |
+----------------+
```
### keywords
    STRRIGHT, RIGHT