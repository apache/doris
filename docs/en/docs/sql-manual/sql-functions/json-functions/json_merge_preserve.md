---
{
"title": "json_merge_preserve",
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

## json_merge_preserve

<version since="1.2.5">

json_merge_preserve

</version>

### description
#### Syntax

`VARCHAR json_merge_preserve(VARCHAR,...)`
Merges two or more JSON documents and returns the merged result. Returns NULL if any argument is NULL. An error occurs if any argument is not a valid JSON document.

Merging takes place according to the following rules. For additional information, see Normalization, Merging, and Autowrapping of JSON Values.

- Adjacent arrays are merged to a single array.

- Adjacent objects are merged to a single object.

- A scalar value is autowrapped as an array and merged as an array.

- An adjacent array and object are merged by autowrapping the object as an array and merging the two arrays.

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
