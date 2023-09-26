---
{
    "title": "GET_JSON_STRING",
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

## get_json_string
### description
#### Syntax

`VARCHAR get_json_string(VARCHAR json_str, VARCHAR json_path)`


解析并获取 json 字符串内指定路径的字符串内容。
其中 json_path 必须以 $ 符号作为开头，使用 . 作为路径分割符。如果路径中包含 . ，则可以使用双引号包围。
使用 [ ] 表示数组下标，从 0 开始。
path 的内容不能包含 ", [ 和 ]。
如果 json_string 格式不对，或 json_path 格式不对，或无法找到匹配项，则返回 NULL。

另外，推荐使用jsonb类型和jsonb_extract_XXX函数实现同样的功能。

### example

1. 获取 key 为 "k1" 的 value

```
mysql> SELECT get_json_string('{"k1":"v1", "k2":"v2"}', "$.k1");
+---------------------------------------------------+
| get_json_string('{"k1":"v1", "k2":"v2"}', '$.k1') |
+---------------------------------------------------+
| v1                                                |
+---------------------------------------------------+
```

2. 获取 key 为 "my.key" 的数组中第二个元素

```
mysql> SELECT get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]');
+------------------------------------------------------------------------------+
| get_json_string('{"k1":"v1", "my.key":["e1", "e2", "e3"]}', '$."my.key"[1]') |
+------------------------------------------------------------------------------+
| e2                                                                           |
+------------------------------------------------------------------------------+
```

3. 获取二级路径为 k1.key -> k2 的数组中，第一个元素
```
mysql> SELECT get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]');
+-----------------------------------------------------------------------+
| get_json_string('{"k1.key":{"k2":["v1", "v2"]}}', '$."k1.key".k2[0]') |
+-----------------------------------------------------------------------+
| v1                                                                    |
+-----------------------------------------------------------------------+
```

4. 获取数组中，key 为 "k1" 的所有 value
```
mysql> SELECT get_json_string('[{"k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v4"}]', '$.k1');
+---------------------------------------------------------------------------------+
| get_json_string('[{"k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v4"}]', '$.k1') |
+---------------------------------------------------------------------------------+
| ["v1","v3","v4"]                                                                |
+---------------------------------------------------------------------------------+
```
### keywords
GET_JSON_STRING,GET,JSON,STRING
