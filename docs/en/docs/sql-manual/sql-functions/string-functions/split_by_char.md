---
{
    "title": "split_by_char",
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

## split_by_char 

### description

#### Syntax

```
split_by_char(s, separator)
```
Splits a string into substrings separated by a specified character. It uses a constant string separator which consists of exactly one character. Returns an array of selected substrings. Empty substrings may be selected if the separator occurs at the beginning or end of the string, or if there are multiple consecutive separators.

#### Arguments

`separator` — The separator which should contain exactly one character. Type: `String`

`s` — The string to split. Type: `String`

#### Returned value(s)

Returns an array of selected substrings. Empty substrings may be selected when:

A separator occurs at the beginning or end of the string;

There are multiple consecutive separators;

The original string s is empty.

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