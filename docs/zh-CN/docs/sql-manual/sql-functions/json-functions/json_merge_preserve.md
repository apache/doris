---
{
"title": "json_merge_preserve",
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

## json_merge_preserve

<version since="1.2.5">

json_merge_preserve

</version>

### description
#### Syntax

`VARCHAR json_merge_preserve(VARCHAR,...)`

合并两个或者多个 JSON 成为 JSON，如果任何一个参数为 NULL，则返回 NULL，如果解析 JSON 失败，则返回错误。  
合并规则如下，规则可参考 https://dev.mysql.com/doc/refman/8.0/en/json.html#json-normalization：
- 相邻的数组合并为一个数组。
- 相邻的对象合并为一个对象。
- 任何标量类型参数会自动封装为数组，然后参考数组合并规则进行合并。
- 相邻的对象和数组会将对象自动封装为数组，然后当前两个数组进行合并。

### example

```sql
mysql> SELECT JSON_MERGE_PRESERVE('[1, 2]', '[true, false]');
+------------------------------------------------+
| JSON_MERGE_PRESERVE('[1, 2]', '[true, false]') |
+------------------------------------------------+
| [1, 2, true, false]                            |
+------------------------------------------------+

mysql> SELECT JSON_MERGE_PRESERVE('{"name": "x"}', '{"id": 47}');
+----------------------------------------------------+
| JSON_MERGE_PRESERVE('{"name": "x"}', '{"id": 47}') |
+----------------------------------------------------+
| {"id": 47, "name": "x"}                            |
+----------------------------------------------------+

mysql> SELECT JSON_MERGE_PRESERVE('1', 'true');
+----------------------------------+
| JSON_MERGE_PRESERVE('1', 'true') |
+----------------------------------+
| [1, true]                        |
+----------------------------------+

mysql> SELECT JSON_MERGE_PRESERVE('[1, 2]', '{"id": 47}');
+---------------------------------------------+
| JSON_MERGE_PRESERVE('[1, 2]', '{"id": 47}') |
+---------------------------------------------+
| [1, 2, {"id": 47}]                          |
+---------------------------------------------+

mysql> SELECT JSON_MERGE_PRESERVE('{ "a": 1, "b": 2 }',
     >    '{ "a": 3, "c": 4 }');
+--------------------------------------------------------------+
| JSON_MERGE_PRESERVE('{ "a": 1, "b": 2 }','{ "a": 3, "c":4 }') |
+--------------------------------------------------------------+
| {"a": [1, 3], "b": 2, "c": 4}                                |
+--------------------------------------------------------------+

mysql> SELECT JSON_MERGE_PRESERVE('{ "a": 1, "b": 2 }','{ "a": 3, "c": 4 }',
     >    '{ "a": 5, "d": 6 }');
+----------------------------------------------------------------------------------+
| JSON_MERGE_PRESERVE('{ "a": 1, "b": 2 }','{ "a": 3, "c": 4 }','{ "a": 5, "d": 6 }') |
+----------------------------------------------------------------------------------+
| {"a": [1, 3, 5], "b": 2, "c": 4, "d": 6}                                         |
+----------------------------------------------------------------------------------+
```

### keywords
JSON, MERGE, MERGE_PRESERVE
