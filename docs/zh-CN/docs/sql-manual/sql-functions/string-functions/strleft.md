---
{
    "title": "STRLEFT",
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

## strleft
### description
#### Syntax

`VARCHAR strleft(VARCHAR str, INT len)`


它返回具有指定长度的字符串的左边部分，长度的单位为utf8字符，此函数的另一个别名为 `left`。
如果函数参数含有NULL值那么函数将始终返回NULL，如果整型参数小于等于0，那么将返回空 “”。

### example

```
mysql> select strleft("Hello doris",5);
+------------------------+
| strleft('Hello doris', 5) |
+------------------------+
| Hello                  |
+------------------------+
mysql> select strleft("Hello doris",-5);
+----------------------------+
| strleft('Hello doris', -5) |
+----------------------------+
|                            |
+----------------------------+
mysql> select strleft("Hello doris",NULL);
+------------------------------+
| strleft('Hello doris', NULL) |
+------------------------------+
| NULL                         |
+------------------------------+
mysql> select strleft(NULL,3);
+------------------+
| strleft(NULL, 3) |
+------------------+
| NULL             |
+------------------+
```
### keywords
    STRLEFT, LEFT