---
{
    "title": "SPLIT_BY_STRING",
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

## split_by_string

<version since="1.2.2">
</version>

### description

#### Syntax

`ARRAY<STRING> split_by_string(STRING s, STRING separator)`
将字符串拆分为由字符串分隔的子字符串。它使用多个字符的常量字符串分隔符作为分隔符。如果字符串分隔符为空，它将字符串拆分为单个字符数组。

#### Arguments

`separator` — 分隔符是一个字符串，是用来分割的标志字符. 类型: `String`

`s` — 需要分割的字符串. 类型: `String`

#### Returned value(s)

返回一个包含子字符串的数组. 以下情况会返回空的子字符串:

需要分割的字符串的首尾是分隔符;

多个分隔符连续出现;

需要分割的字符串为空，而分隔符不为空.

Type: `Array(String)`

### notice

`仅支持向量化引擎中使用`

### example

```
select split_by_string('a1b1c1d','1');
+---------------------------------+
| split_by_string('a1b1c1d', '1') |
+---------------------------------+
| ['a', 'b', 'c', 'd']            |
+---------------------------------+

select split_by_string(',,a,b,c,',',');
+----------------------------------+
| split_by_string(',,a,b,c,', ',') |
+----------------------------------+
| ['', '', 'a', 'b', 'c', '']      |
+----------------------------------+

SELECT split_by_string(NULL,',');
+----------------------------+
| split_by_string(NULL, ',') |
+----------------------------+
| NULL                       |
+----------------------------+

select split_by_string('a,b,c,abcde',',');
+-------------------------------------+
| split_by_string('a,b,c,abcde', ',') |
+-------------------------------------+
| ['a', 'b', 'c', 'abcde']            |
+-------------------------------------+

select split_by_string('1,,2,3,,4,5,,abcde', ',,');
+---------------------------------------------+
| split_by_string('1,,2,3,,4,5,,abcde', ',,') |
+---------------------------------------------+
| ['1', '2,3', '4,5', 'abcde']                |
+---------------------------------------------+

select split_by_string(',,,,',',,');
+-------------------------------+
| split_by_string(',,,,', ',,') |
+-------------------------------+
| ['', '', '']                  |
+-------------------------------+

select split_by_string(',,a,,b,,c,,',',,');
+--------------------------------------+
| split_by_string(',,a,,b,,c,,', ',,') |
+--------------------------------------+
| ['', 'a', 'b', 'c', '']              |
+--------------------------------------+
```
### keywords

SPLIT_BY_STRING,SPLIT
