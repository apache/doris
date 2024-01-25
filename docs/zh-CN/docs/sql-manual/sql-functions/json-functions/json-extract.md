---
{
    "title": "JSON_EXTRACT",
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

## json_extract

<version since="dev"></version>

### description
#### Syntax

```sql
VARCHAR json_extract(VARCHAR json_str, VARCHAR path[, VARCHAR path] ...)
JSON jsonb_extract(JSON j, VARCHAR json_path)
BOOLEAN json_extract_isnull(JSON j, VARCHAR json_path)
BOOLEAN json_extract_bool(JSON j, VARCHAR json_path)
INT json_extract_int(JSON j, VARCHAR json_path)
BIGINT json_extract_bigint(JSON j, VARCHAR json_path)
LARGEINT json_extract_largeint(JSON j, VARCHAR json_path)
DOUBLE json_extract_double(JSON j, VARCHAR json_path)
STRING json_extract_string(JSON j, VARCHAR json_path)
```



json_extract是一系列函数，从JSON类型的数据中提取json_path指定的字段，根据要提取的字段类型不同提供不同的系列函数。
- json_extract对VARCHAR类型的json string返回VARCHAR类型
- jsonb_extract返回JSON类型
- json_extract_isnull返回是否为json null的BOOLEAN类型
- json_extract_bool返回BOOLEAN类型
- json_extract_int返回INT类型
- json_extract_bigint返回BIGINT类型
- json_extract_largeint返回LARGEINT类型
- json_extract_double返回DOUBLE类型
- json_extract_STRING返回STRING类型

json path的语法如下
- '$' 代表json root
- '.k1' 代表json object中key为'k1'的元素
  - 如果 key 列值包含 ".", json_path 中需要用双引号，例如 SELECT json_extract('{"k1.a":"abc","k2":300}', '$."k1.a"'); 
- '[i]' 代表json array中下标为i的元素
  - 获取 json_array 的最后一个元素可以用'$[last]'，倒数第二个元素可以用'$[last-1]'，以此类推

特殊情况处理如下：
- 如果 json_path 指定的字段在JSON中不存在，返回NULL
- 如果 json_path 指定的字段在JSON中的实际类型和json_extract_t指定的类型不一致，如果能无损转换成指定类型返回指定类型t，如果不能则返回NULL

### example

参考 [json tutorial](../../sql-reference/Data-Types/JSON.md) 中的示例

```
mysql> SELECT json_extract('{"id": 123, "name": "doris"}', '$.id');
+------------------------------------------------------+
| json_extract('{"id": 123, "name": "doris"}', '$.id') |
+------------------------------------------------------+
| 123                                                  |
+------------------------------------------------------+
1 row in set (0.01 sec)

mysql> SELECT json_extract('[1, 2, 3]', '$.[1]');
+------------------------------------+
| json_extract('[1, 2, 3]', '$.[1]') |
+------------------------------------+
| 2                                  |
+------------------------------------+
1 row in set (0.01 sec)

mysql> SELECT json_extract('{"k1": "v1", "k2": { "k21": 6.6, "k22": [1, 2] } }', '$.k1', '$.k2.k21', '$.k2.k22', '$.k2.k22[1]');
+-------------------------------------------------------------------------------------------------------------------+
| json_extract('{"k1": "v1", "k2": { "k21": 6.6, "k22": [1, 2] } }', '$.k1', '$.k2.k21', '$.k2.k22', '$.k2.k22[1]') |
+-------------------------------------------------------------------------------------------------------------------+
| ["v1",6.6,[1,2],2]                                                                                                |
+-------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> SELECT json_extract('{"id": 123, "name": "doris"}', '$.aaa', '$.name');
+-----------------------------------------------------------------+
| json_extract('{"id": 123, "name": "doris"}', '$.aaa', '$.name') |
+-----------------------------------------------------------------+
| [null,"doris"]                                                  |
+-----------------------------------------------------------------+
1 row in set (0.01 sec)
```

### keywords
JSONB, JSON, json_extract, json_extract_isnull, json_extract_bool, json_extract_int, json_extract_bigint, json_extract_largeint, json_extract_double, json_extract_string
