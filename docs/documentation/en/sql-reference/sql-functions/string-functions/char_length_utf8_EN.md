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

# char_length_utf8
## Description
### Syntax

'INT char_length_utf8 (VARCHAR str)'


Returns the length of the string and the number of characters returned for multi-byte characters. For example, five two-byte width words return a length of 10.

## example


```
mysql> select char_length_utf8("abc");
+-------------------------+
| char_length_utf8('abc') |
+-------------------------+
|                       3 |
+-------------------------+

mysql> select char_length_utf8("中国");
+------------------------ ---+
| char_length_utf8('中国')   |
+----------------------------+
|                          2 |
+----------------------------+
```
##keyword
CHAR_LENGTH_UTF8
