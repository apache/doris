---
{
    "title": "split_by_char",
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

## split_by_char 

### description

#### Syntax

```
split_by_char(s, separator)
```
将字符串拆分为由指定字符分隔的子字符串。它使用由一个字符组成的常量字符串作为分隔符，返回分割后包含子字符串的数组。如果分隔符出现在字符串的开头或结尾，或者有多个连续的分隔符，则会返回包含空字符的子字符串数组。

#### Arguments

`separator` — 分隔符是一个字符，是用来分割的标志字符. 类型: `String`

`s` — 需要分割的字符串. 类型: `String`

#### Returned value(s)

返回一个包含子字符串的数组. 以下情况会返回空的子字符串:

需要分割的字符串的首尾是分隔符;

多个分隔符连续出现;

需要分割的字符串为空.

Type: `Array(String)`

### notice

`Only supported in vectorized engine`

### example

```
SELECT split_by_char('1,2,3,abcde',',');

+-----------------------------------+
| split_by_char('1,2,3,abcde', ',') |
+-----------------------------------+
| ['1', '2', '3', 'abcde']          |
+-----------------------------------+

SELECT split_by_char('1,2,3,',',');

+------------------------------+
| split_by_char('1,2,3,', ',') |
+------------------------------+
| ['1', '2', '3', '']          |
+------------------------------+

SELECT split_by_char(NULL,',');

+--------------------------+
| split_by_char(NULL, ',') |
+--------------------------+
| NULL                     |
+--------------------------+
```
### keywords

SPLIT_BY_CHAR,SPLIT