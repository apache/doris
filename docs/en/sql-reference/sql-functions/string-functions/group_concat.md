---
{
    "title": "group_concat",
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

# group_concat
## description
### Syntax

`VARCHAR group_concat(VARCHAR str[, VARCHAR sep])`


This function is an aggregation function similar to sum (), and group_concat links multiple rows of results in the result set to a string. The second parameter, sep, is a connection symbol between strings, which can be omitted. This function usually needs to be used with group by statements.

## example

```
mysql> select value from test;
+-------+
| value |
+-------+
| a     |
| b     |
| c     |
+-------+

mysql> select group_concat(value) from test;
+-----------------------+
| group_concat(`value`) |
+-----------------------+
| a, b, c               |
+-----------------------+

mysql> select group_concat(value, " ") from test;
+----------------------------+
| group_concat(`value`, ' ') |
+----------------------------+
| a b c                      |
+----------------------------+
```
##keyword
GROUP_CONCAT,GROUP,CONCAT
