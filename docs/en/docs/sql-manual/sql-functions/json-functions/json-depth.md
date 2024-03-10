---
{
"title": "JSON_DEPTH",
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

## json_depth
### description
#### Syntax

`INT json_depth(JSON json_str)`

returns the maximum depth of a JSON document.

JSON_DEPTH() calculates depth according to the following rules:

* The depth of an empty array, or an empty object, or a scalar is 1.
* The depth of an nonempty array containing only elements of depth 1 is 2.
* The depth of an nonempty object containing only member values of depth 1 is 2.
* return NULL if the argument is NULL.

### example

```
mysql> select JSON_DEPTH('[]');
+------------------+
| json_depth('[]') |
+------------------+
|                1 |
+------------------+
1 row in set (0.07 sec)

mysql> select JSON_DEPTH('1');
+-----------------+
| json_depth('1') |
+-----------------+
|               1 |
+-----------------+
1 row in set (0.09 sec)

mysql> select JSON_DEPTH('[1, 2]');
+----------------------+
| json_depth('[1, 2]') |
+----------------------+
|                    2 |
+----------------------+
1 row in set (0.05 sec)

mysql> select JSON_DEPTH('[1, [2, 3]]');
+---------------------------+
| json_depth('[1, [2, 3]]') |
+---------------------------+
|                         3 |
+---------------------------+
1 row in set (0.05 sec)

mysql> select JSON_DEPTH('{"x": {"y": 1}}');
+-------------------------------+
| json_depth('{"x": {"y": 1}}') |
+-------------------------------+
|                             3 |
+-------------------------------+
1 row in set (0.06 sec)

mysql> select JSON_DEPTH(NULL);
+------------------+
| json_depth(NULL) |
+------------------+
|             NULL |
+------------------+
1 row in set (0.08 sec)
```
### keywords
json,json_depth
