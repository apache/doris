---
{
    "title": "jsonb_extract",
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

## jsonb_extract
### description
#### Syntax

`JSONB jsonb_extract(JSONB j, VARCHAR json_path)`

`BOOLEAN jsonb_extract_isnull(JSONB j, VARCHAR json_path)`

`BOOLEAN jsonb_extract_bool(JSONB j, VARCHAR json_path)`

`INT jsonb_extract_int(JSONB j, VARCHAR json_path)`

`BIGINT jsonb_extract_bigint(JSONB j, VARCHAR json_path)`

`DOUBLE jsonb_extract_double(JSONB j, VARCHAR json_path)`

`STRING jsonb_extract_string(JSONB j, VARCHAR json_path)`


jsonb_extract是一系列函数，从JSONB类型的数据中提取json_path指定的字段，根据要提取的字段类型不同提供不同的系列函数。
- jsonb_extract返回JSONB类型
- jsonb_extract_isnull返回是否为json null的BOOLEAN类型
- jsonb_extract_bool返回BOOLEAN类型
- jsonb_extract_int返回INT类型
- jsonb_extract_bigint返回BIGINT类型
- jsonb_extract_double返回DOUBLE类型
- jsonb_extract_STRING返回STRING类型

特殊情况处理如下：
- 如果json_path指定的字段在JSON中不存在，返回NULL
- 如果json_path指定的字段在JSON中的实际类型和jsonb_extract_t指定的类型不一致，如果能无损转换成指定类型返回指定类型t，如果不能则返回NULL


`BOOLEAN jsonb_exists_path(JSONB j, VARCHAR json_path)`

`STRING jsonb_type(JSONB j, VARCHAR json_path)`

这两个jsonb函数用来判断字段是否存在和字段类型
- jsonb_exists_path用来判断json_path指定的字段在JSONB数据中是否存在，如果存在返回TRUE，不存在返回FALSE
- jsonb_exists_path用来判断json_path指定的字段在JSONB数据中的类型，如果字段不存在返回NULL，如果存在返回下面的类型之一
  - object
  - array
  - null
  - bool
  - int
  - bigint
  - double
  - string


### example

参考jsonb tutorial中的示例


### keywords
JSONB, JSON, jsonb_extract, jsonb_extract_isnull, jsonb_extract_bool, jsonb_extract_int, jsonb_extract_bigint, jsonb_extract_double, jsonb_extract_string, jsonb_exists_path, jsonb_type
