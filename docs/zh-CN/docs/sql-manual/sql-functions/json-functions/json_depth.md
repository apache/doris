---
{
"title": "json_depth",
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

## json_depth

<version since="1.2.5">

json_depth

</version>

### description
#### Syntax

`INT json_depth(VARCHAR json_doc)`

返回 JSON 文件最大深度。如果 JSON 文件为 NULL，则返回 NULL。如果输入的不是有效的 JSON 文件，则返回错误。

### example

```sql
mysql> SELECT json_depth(NULL);
+------------------+
| json_depth(NULL) |
+------------------+
|             NULL |
+------------------+

mysql> SELECT json_depth('1');
+-----------------+
| json_depth('1') |
+-----------------+
|               1 |
+-----------------+

mysql> SELECT json_depth('{}');
+------------------+
| json_depth('{}') |
+------------------+
|                1 |
+------------------+

mysql> SELECT json_depth('[]');
+------------------+
| json_depth('[]') |
+------------------+
|                1 |
+------------------+

mysql> SELECT json_depth('[1, 2]');
+----------------------+
| json_depth('[1, 2]') |
+----------------------+
|                    2 |
+----------------------+

mysql> SELECT json_depth('[[1, 2], [3, 4]]');
+--------------------------------+
| json_depth('[[1, 2], [3, 4]]') |
+--------------------------------+
|                              3 |
+--------------------------------+

mysql> SELECT json_depth('[10, {"a": 20}]');
+-------------------------------+
| json_depth('[10, {"a": 20}]') |
+-------------------------------+
|                             3 |
+-------------------------------+

mysql> SELECT json_depth('[{"a": ["b", "c"]}]');
+-----------------------------------+
| json_depth('[{"a": ["b", "c"]}]') |
+-----------------------------------+
|                                 4 |
+-----------------------------------+
```

### keywords
JSON, DEPTH, JSON_DEPTH
