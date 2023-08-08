---
{
    "title": "SPLIT_PART",
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

## split_part
### description
#### Syntax

`VARCHAR split_part(VARCHAR content, VARCHAR delimiter, INT field)`


根据分割符拆分字符串, 返回指定的分割部分(从一或负一开始计数)。field字段支持负数，代表从右往左倒着截取并取数。
`delimiter` 和 `field` 参数需要是常量, 不支持变量。

### example

```
mysql> select split_part("hello world", " ", 1);
+----------------------------------+
| split_part('hello world', ' ', 1) |
+----------------------------------+
| hello                            |
+----------------------------------+


mysql> select split_part("hello world", " ", 2);
+----------------------------------+
| split_part('hello world', ' ', 2) |
+----------------------------------+
| world                             |
+----------------------------------+

mysql> select split_part("2019年7月8号", "月", 1);
+-----------------------------------------+
| split_part('2019年7月8号', '月', 1)     |
+-----------------------------------------+
| 2019年7                                 |
+-----------------------------------------+

mysql> select split_part("abca", "a", 1);
+----------------------------+
| split_part('abca', 'a', 1) |
+----------------------------+
|                            |
+----------------------------+

mysql> select split_part("prefix_string", "_", -1);
+--------------------------------------+
| split_part('prefix_string', '_', -1) |
+--------------------------------------+
| string                               |
+--------------------------------------+

mysql> select split_part("prefix_string", "_", -2);
+--------------------------------------+
| split_part('prefix_string', '_', -2) |
+--------------------------------------+
| prefix                               |
+--------------------------------------+

mysql> select split_part("abc##123###234", "##", -1);
+----------------------------------------+
| split_part('abc##123###234', '##', -1) |
+----------------------------------------+
| 234                                    |
+----------------------------------------+

mysql> select split_part("abc##123###234", "##", -2);
+----------------------------------------+
| split_part('abc##123###234', '##', -2) |
+----------------------------------------+
| 123#                                   |
+----------------------------------------+
```
### keywords
    SPLIT_PART,SPLIT,PART
