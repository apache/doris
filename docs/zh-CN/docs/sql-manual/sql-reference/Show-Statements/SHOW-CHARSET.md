---
{
    "title": "SHOW-CHARSET",
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

## SHOW-CHARSET

### Description

"SHOW CHARACTER" 命令用于显示当前数据库管理系统中可用的字符集（character set）以及与每个字符集相关联的一些属性。这些属性可能包括字符集的名称、默认排序规则、最大字节长度等。通过运行 "SHOW CHARACTER" 命令，可以查看系统中支持的字符集列表及其详细信息。

SHOW CHARACTER 命令返回以下字段：


Charset：字符集
Description：描述
Default Collation：默认校对名称
Maxlen：最大字节长度

### Example

```sql
mysql> show chatset;

| Charset   | Description     | Default collation | Maxlen |
|-----------|-----------------|-------------------|--------|
| utf8mb4   | UTF-8 Unicode   | utf8mb4_0900_bin  | 4      |

```

### Keywords

    SHOW, CHARSET

### Best Practice

