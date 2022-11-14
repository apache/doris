---
{
    "title": "split_by_string",
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

### description

#### Syntax

```
split_by_string(s, separator)
```
Splits a string into substrings separated by a string. It uses a constant string separator of multiple characters as the separator. If the string separator is empty, it will split the string s into an array of single characters.

#### Arguments
`separator` — The separator. Type: `String`

`s` — The string to split. Type: `String`

#### Returned value(s)

Returns an array of selected substrings. Empty substrings may be selected when:

A non-empty separator occurs at the beginning or end of the string;

There are multiple consecutive separators;

The original string s is empty.

Type: `Array(String)`

### notice

`Only supported in vectorized engine`

### example

```
SELECT split_by_string('1, 2 3, 4,5, abcde', ', ');
+---------------------------------------------+
| split_by_string('1, 2 3, 4,5, abcde', ', ') |
+---------------------------------------------+
| ['1', '2 3', '4,5', 'abcde']                |
+---------------------------------------------+
SELECT split_by_string('abcde','');
+--------------------------------+
| split_by_string('1,2,3,', ',') |
+--------------------------------+
| ['a', 'b', 'c', 'd', 'e']      |
+--------------------------------+
SELECT split_by_string(NULL,',');
+----------------------------+
| split_by_string(NULL, ',') |
+----------------------------+
| NULL                       |
+----------------------------+
SELECT split_by_string('1, 2 3, , , 4,5, abcde', ', ');
+-------------------------------------------------+
| split_by_string('1, 2 3, , , 4,5, abcde', ', ') |
+-------------------------------------------------+
| ['1', '2 3', '', '', '4,5']                     |
+-------------------------------------------------+
SELECT split_by_string(', , 1, 2 3, , , 4,5, abcde, , ', ', ');
+---------------------------------------------------------+
| split_by_string(', , 1, 2 3, , , 4,5, abcde, , ', ', ') |
+---------------------------------------------------------+
| ['', '', '1', '2 3', '', '', '4,5', 'abcde', '']        |
+---------------------------------------------------------+
```
### keywords

SPLIT_BY_STRING,SPLIT