---
{
    "title": "JSON_CONTAINS_PATH",
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

## json_contains_path
### description
#### Syntax

`BOOLEAN JSON_CONTAINS_PATH(json, one_or_all, path[, path] ...)`

> json：必填。 JSON document。
>
> one_or_all：必填。可用值：“one”、“all”。是否检查所有路径。
>
> path：必填。您应该至少指定一个路径表达式。

### example

```
mysql> SELECT
    ->     JSON_CONTAINS_PATH('[1, 2, {"x": 3}]', 'all', '$[0]') as `$[0]`,
    ->     JSON_CONTAINS_PATH('[1, 2, {"x": 3}]', 'all', '$[3]') as `$[3]`,
    ->     JSON_CONTAINS_PATH('[1, 2, {"x": 3}]', 'all', '$[2].x') as `$[2].x`;
+------+------+--------+
| $[0] | $[3] | $[2].x |
+------+------+--------+
|    1 |    0 |      1 |
+------+------+--------+

mysql> SELECT
    ->     JSON_CONTAINS_PATH('[1, 2, {"x": 3}]', 'one', '$[0]', '$[3]') as `one`,
    ->     JSON_CONTAINS_PATH('[1, 2, {"x": 3}]', 'all', '$[0]', '$[3]') as `all`;
+------+------+
| one  | all  |
+------+------+
|    1 |    0 |
+------+------+
```
### keywords
json,json_contains_path
