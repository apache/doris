---
{
    "title": "JSON_SET",
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

## json_set

<version since="dev"></version>

### Description
#### Syntax

`VARCHAR json_set(VARCHAR json_str, VARCHAR path, VARCHAR val[, VARCHAR path, VARCHAR val] ...)`


`json_set` function inserts or updates data in a JSON and returns the result.Returns NULL if `json_str` or `path` is NULL. Otherwise, an error occurs if the `json_str` argument is not a valid JSON or any path argument is not a valid path expression or contains a * wildcard.

The path-value pairs are evaluated left to right.

A path-value pair for an existing path in the json overwrites the existing json value with the new value. A path-value pair for a nonexisting path in the json adds the value to the json if the path identifies one of these types of values:

* A member not present in an existing object. The member is added to the object and associated with the new value.

* A position past the end of an existing array. The array is extended with the new value. If the existing value is not an array, it is autowrapped as an array, then extended with the new value.

Otherwise, a path-value pair for a nonexisting path in the json is ignored and has no effect.

### example

```
MySQL> select json_set(null, null, null);
+------------------------------+
| json_set(NULL, NULL, 'NULL') |
+------------------------------+
| NULL                         |
+------------------------------+

MySQL> select json_set('{"k": 1}', "$.k", 2);
+------------------------------------+
| json_set('{\"k\": 1}', '$.k', '2') |
+------------------------------------+
| {"k":2}                            |
+------------------------------------+

MySQL> select json_set('{"k": 1}', "$.j", 2);
+------------------------------------+
| json_set('{\"k\": 1}', '$.j', '2') |
+------------------------------------+
| {"k":1,"j":2}                      |
+------------------------------------+
```

### keywords
JSON, json_set
