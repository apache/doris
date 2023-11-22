---
{
"title": "JSON_CONTAINS",
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

## json_contains
### description
#### Syntax

`BOOLEAN json_contains(JSON json_str, JSON candidate)`

`BOOLEAN json_contains(JSON json_str, JSON candidate, VARCHAR json_path)`

`BOOLEAN json_contains(VARCHAR json_str, VARCHAR candidate, VARCHAR json_path)`


通过返回 1 或 0 来指示给定的 candidate JSON 文档是否包含在 json_str JSON json_path 路径下的文档中

### example

```
mysql> SET @j = '{"a": 1, "b": 2, "c": {"d": 4}}';
mysql> SET @j2 = '1';
mysql> SELECT JSON_CONTAINS(@j, @j2, '$.a');
+-------------------------------+
| JSON_CONTAINS(@j, @j2, '$.a') |
+-------------------------------+
|                             1 |
+-------------------------------+
mysql> SELECT JSON_CONTAINS(@j, @j2, '$.b');
+-------------------------------+
| JSON_CONTAINS(@j, @j2, '$.b') |
+-------------------------------+
|                             0 |
+-------------------------------+

mysql> SET @j2 = '{"d": 4}';
mysql> SELECT JSON_CONTAINS(@j, @j2, '$.a');
+-------------------------------+
| JSON_CONTAINS(@j, @j2, '$.a') |
+-------------------------------+
|                             0 |
+-------------------------------+
mysql> SELECT JSON_CONTAINS(@j, @j2, '$.c');
+-------------------------------+
| JSON_CONTAINS(@j, @j2, '$.c') |
+-------------------------------+
|                             1 |
+-------------------------------+

mysql> SELECT json_contains('[1, 2, {"x": 3}]', '1');
+----------------------------------------+
| json_contains('[1, 2, {"x": 3}]', '1') |
+----------------------------------------+
|                                      1 |
+----------------------------------------+
1 row in set (0.04 sec)
```
### keywords
json,json_contains
