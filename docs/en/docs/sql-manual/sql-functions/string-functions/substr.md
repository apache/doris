---
{
"title": "SUBSTR",
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

## substr
### Description
#### Syntax

`VARCHAR substr(VARCHAR content, INT start, INT length)`

Find a substring, return the part of the string described by the first parameter starting from start and having a length of len. The index of the first letter is 1.
If the parameters contain a NULL value, the function will always return NULL. If only two parameters are provided, with the sole integer parameter being "start," the function will return the portion to the right of "start."

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
