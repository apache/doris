---
{
"title": "json_merge_patch",
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

## json_merge_patch

<version since="1.2.8">

json_merge_patch

</version>

### description
#### Syntax

`VARCHAR json_merge_preserve(VARCHAR, VARCHAR, ...)`

Merge multiple or multiple JSONs into JSON.
The merge rules are as follows, the rules can be referred to https://dev.mysql.com/doc/refman/8.0/en/json-modification-functions.html#function_json-merge-patch
- If the first argument is not an object, the result of the merge is the same as if an empty object had been merged with the second argument.
- If the second argument is not an object, the result of the merge is the second argument.
- If both arguments are objects, the result of the merge is an object with the following members:
    - All members of the first object which do not have a corresponding member with the same key in the second object.
    - All members of the second object which do not have a corresponding key in the first object, and whose value is not the JSON null literal.
    - All members with a key that exists in both the first and the second object, and whose value in the second object is not the JSON null literal. The values of these members are the results of recursively merging the value in the first object with the value in the second object.

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
