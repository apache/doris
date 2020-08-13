---
{
    "title": "get_json_double",
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

# get_json_double
## description
### Syntax

`DOUBLE get_json_double(VARCHAR json_str, VARCHAR json_path)


解析并获取 json 字符串内指定路径的浮点型内容。
其中 json_path 必须以 $ 符号作为开头，使用 . 作为路径分割符。如果路径中包含 . ，则可以使用双引号包围。
使用 [ ] 表示数组下标，从 0 开始。
path 的内容不能包含 ", [ 和 ]。
如果 json_string 格式不对，或 json_path 格式不对，或无法找到匹配项，则返回 NULL。

## example

1. 获取 key 为 "k1" 的 value

```
mysql> SELECT get_json_double('{"k1":1.3, "k2":"2"}', "$.k1");
+-------------------------------------------------+
| get_json_double('{"k1":1.3, "k2":"2"}', '$.k1') |
+-------------------------------------------------+
|                                             1.3 |
+-------------------------------------------------+
```

2. 获取 key 为 "my.key" 的数组中第二个元素

```
mysql> SELECT get_json_double('{"k1":"v1", "my.key":[1.1, 2.2, 3.3]}', '$."my.key"[1]');
+---------------------------------------------------------------------------+
| get_json_double('{"k1":"v1", "my.key":[1.1, 2.2, 3.3]}', '$."my.key"[1]') |
+---------------------------------------------------------------------------+
|                                                                       2.2 |
+---------------------------------------------------------------------------+
```

3. 获取二级路径为 k1.key -> k2 的数组中，第一个元素
```
mysql> SELECT get_json_double('{"k1.key":{"k2":[1.1, 2.2]}}', '$."k1.key".k2[0]');
+---------------------------------------------------------------------+
| get_json_double('{"k1.key":{"k2":[1.1, 2.2]}}', '$."k1.key".k2[0]') |
+---------------------------------------------------------------------+
|                                                                 1.1 |
+---------------------------------------------------------------------+
```
## keyword
GET_JSON_DOUBLE,GET,JSON,DOUBLE
