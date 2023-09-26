---
{
    "title": "FIND_IN_SET",
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

## find_in_set
### description
#### Syntax

`INT find_in_set(VARCHAR str, VARCHAR strlist)`


返回 strlist 中第一次出现 str 的位置（从1开始计数）。strlist 是用逗号分隔的字符串。如果没有找到，返回0。任意参数为 NULL ，返回 NULL。

### example

```
mysql> select find_in_set("b", "a,b,c");
+---------------------------+
| find_in_set('b', 'a,b,c') |
+---------------------------+
|                         2 |
+---------------------------+
```
### keywords
    FIND_IN_SET,FIND,IN,SET
