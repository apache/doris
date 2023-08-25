---
{
"title": "json_merge_patch",
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

## json_merge_patch

<version since="1.2.8">

json_merge_patch

</version>

### description
#### Syntax

`VARCHAR json_merge_preserve(VARCHAR, VARCHAR, ...)`

合并两个或者多个 JSON 成为 JSON。
合并规则如下，规则可参考 https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-merge-patch
- 如果第一个参数不是对象，则合并结果与第二个参数合并的结果相同。
- 如果第二个参数不是对象，则合并结果为第二个参数。
- 如果两个参数都是对象，则合并结果是具有以下成员的对象：
    - 第一个对象的所有成员在第二个对象中没有相同键的对应成员。
    - 第二个对象的所有成员在第一个对象中没有对应的键，并且其值不是 JSON null。
    - 键同时存在第一个和第二个对象中的值不是 JSON null 的所有成员。这些成员的值是将第一个对象中的值与第二个对象中的值递归合并的结果。

### example

```sql
mysql> SELECT JSON_MERGE_PATCH('[1, 2]', '[true, false]');
+------------------------------------------------+
| json_merge_patch('[1, 2]', '[true, false]')    |
+------------------------------------------------+
| [true,false]                                   |
+------------------------------------------------+

mysql> SELECT JSON_MERGE_PATCH('{"name": "x"}', '{"id": 47}');
+----------------------------------------------------+
| json_merge_patch('{"name": "x"}', '{"id": 47}')    |
+----------------------------------------------------+
| {"name":"x","id":47}                               |
+----------------------------------------------------+

mysql> SELECT JSON_MERGE_PATCH('1', 'true');
+----------------------------------+
| json_merge_patch('1', 'true')    |
+----------------------------------+
| true                             |
+----------------------------------+

mysql> SELECT JSON_MERGE_PATCH('[1, 2]', '{"id": 47}');
+---------------------------------------------+
| json_merge_patch('[1, 2]', '{"id": 47}')    |
+---------------------------------------------+
| {"id":47}                                   |
+---------------------------------------------+

mysql> SELECT JSON_MERGE_PATCH('{ "a": 1, "b": 2 }',
     >    '{ "a": 3, "c": 4 }');
+--------------------------------------------------------------+
| json_merge_patch('{ "a": 1, "b": 2 }','{ "a": 3, "c":4 }')   |
+--------------------------------------------------------------+
| {"a":3,"b":2,"c":4}                                          |
+--------------------------------------------------------------+

mysql> SELECT JSON_MERGE_PATCH('{ "a": 1, "b": 2 }','{ "a": 3, "c": 4 }',
     >    '{ "a": 5, "d": 6 }');
+----------------------------------------------------------------------------------+
| json_merge_patch('{ "a": 1, "b": 2 }','{ "a": 3, "c": 4 }','{ "a": 5, "d": 6 }') |
+----------------------------------------------------------------------------------+
| {"a":5,"b":2,"c":4,"d":6}                                                        |
+----------------------------------------------------------------------------------+
```

### keywords
JSON, MERGE, MERGE_PATCH
