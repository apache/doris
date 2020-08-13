---
{
    "title": "get_json_int",
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

# get_json_int
## Description
### Syntax

`INT get_json_int(VARCHAR json_str, VARCHAR json_path)


Parse and retrieve the integer content of the specified path in the JSON string.
Where json_path must start with the $symbol and use. as the path splitter. If the path contains..., double quotation marks can be used to surround it.
Use [] to denote array subscripts, starting at 0.
The content of path cannot contain ",[and].
If the json_string format is incorrect, or the json_path format is incorrect, or matches cannot be found, NULL is returned.

## example

1. Get the value of key as "k1"

```
mysql> SELECT get_json_int('{"k1":1, "k2":"2"}', "$.k1");
+--------------------------------------------+
| get_json_int('{"k1":1, "k2":"2"}', '$.k1') |
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+
```

2. Get the second element of the array whose key is "my. key"

```
mysql> SELECT get_json_int('{"k1":"v1", "my.key":[1, 2, 3]}', '$."my.key"[1]');
+------------------------------------------------------------------+
| get_json_int('{"k1":"v1", "my.key":[1, 2, 3]}', '$."my.key"[1]') |
+------------------------------------------------------------------+
|                                                                2 |
+------------------------------------------------------------------+
```

3. Get the first element in an array whose secondary path is k1. key - > K2
```
mysql> SELECT get_json_int('{"k1.key":{"k2":[1, 2]}}', '$."k1.key".k2[0]');
+--------------------------------------------------------------+
| get_json_int('{"k1.key":{"k2":[1, 2]}}', '$."k1.key".k2[0]') |
+--------------------------------------------------------------+
|                                                            1 |
+--------------------------------------------------------------+
```
## keyword
GET_JSON_INT,GET,JSON,INT
