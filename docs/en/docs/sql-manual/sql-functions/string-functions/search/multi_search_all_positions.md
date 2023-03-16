---
{
    "title": "multi_search_all_positions",
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

## multi_search_all_positions
### Description
#### Syntax

`ARRAY<INT> multi_search_all_positions(VARCHAR haystack, ARRAY<VARCHAR> needles)`


Searches for the substrings `needles` in the string `haystack`, and returns array of positions of the found corresponding substrings in the string. Positions are indexed starting from 1.

### example

```
mysql> select multi_search_all_positions('Hello, World!', ['hello', '!', 'world']);
+----------------------------------------------------------------------+
| multi_search_all_positions('Hello, World!', ['hello', '!', 'world']) |
+----------------------------------------------------------------------+
| [0,13,0]                                                             |
+----------------------------------------------------------------------+

mysql> select multi_search_all_positions('abc', ['a', 'bc', 'd']);
+-----------------------------------------------------+
| multi_search_all_positions('abc', ['a', 'bc', 'd']) |
+-----------------------------------------------------+
| [1,2,0]                                             |
+-----------------------------------------------------+
```
### keywords
    MULTI_SEARCH,SEARCH,POSITIONS
