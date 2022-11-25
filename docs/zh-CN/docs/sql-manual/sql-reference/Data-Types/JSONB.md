---
{
    "title": "JSONB",
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

## JSONB
### description
    JSONB(JSON Binary)类型
        二进制JSON类型，采用二进制JSONB格式存储，通过jsonb函数访问JSON内部字段。

### note
    与普通STRING类型存储的JSON字符串相比，JSONB类型有两点优势
    1. 数据写入时进行JSON格式校验
    2. 二进制存储格式更加高效，通过jsonb_extract等函数可以高效访问JSON内部字段，比get_json_xx函数快几倍

### example
    JSONB类型通过jsonb_parse函数从JSON字符串创建，通过jsonb_extract系列函数访问内部字段，具体使用方式参考jsonb tutorial。

### keywords

    JSONB JSON
