---
{
    "title": "jsonb_exists_path",
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

## jsonb_exists_path

### description

用来判断json_path指定的字段在JSONB数据中是否存在，如果存在返回TRUE，不存在返回FALSE

#### Syntax

```sql
BOOLEAN jsonb_exists_path(JSONB j, VARCHAR json_path)
```

### example

参考 [jsonb tutorial](../../sql-reference/Data-Types/JSONB.md) 中的示例

### keywords

jsonb_exists_path

