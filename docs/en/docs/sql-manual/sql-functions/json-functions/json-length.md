---
{
"title": "JSON_LENGTH",
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

## json_length
### description
#### Syntax

`INT json_length(JSON json_str)`
`INT json_length(JSON json_str, VARCHAR json_path)`

If specified path, the JSON_LENGTH() function returns the length of the data matching the path in the JSON document, otherwise it returns the length of the JSON document. The function calculates the length of the JSON document according to the following rules:

* The length of a scalar is 1. For example, the length of 1, '"x"', true, false, null is all 1.
* The length of an array is the number of array elements. For example, the length of [1, 2] is 2.
* The length of an object is the number of object members. For example, the length of {"x": 1} is 1.

### example

```
mysql> SELECT json_length('{"k1":"v31","k2":300}');
+--------------------------------------+
| json_length('{"k1":"v31","k2":300}') |
+--------------------------------------+
|                                    2 |
+--------------------------------------+
1 row in set (0.26 sec)

mysql> SELECT json_length('"abc"');
+----------------------+
| json_length('"abc"') |
+----------------------+
|                    1 |
+----------------------+
1 row in set (0.17 sec)

mysql> SELECT json_length('{"x": 1, "y": [1, 2]}', '$.y');
+---------------------------------------------+
| json_length('{"x": 1, "y": [1, 2]}', '$.y') |
+---------------------------------------------+
|                                           2 |
+---------------------------------------------+
1 row in set (0.07 sec)
```
### keywords
json,json_length
