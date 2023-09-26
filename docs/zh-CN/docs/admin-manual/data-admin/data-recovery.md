---
{
    "title": "数据恢复",
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

# 数据恢复

对于Unique Key Merge on Write表，在某些Doris的版本中存在bug，可能会导致系统在计算delete bitmap时出现错误，导致出现重复主键，此时可以利用full compaction功能进行数据的修复。本功能对于非Unique Key Merge on Write表无效。

该功能需要 Doris 版本 2.0+。

使用该功能，需要尽可能停止导入，否则可能会出现导入超时等问题。

## 简要原理说明

执行full compaction后，会对delete bitmap进行重新计算，将错误的delete bitmap数据删除，以完成数据的修复。

## 使用说明

`POST /api/compaction/run?tablet_id={int}&compact_type=full`

或

`POST /api/compaction/run?table_id={int}&compact_type=full`

注意，tablet_id和table_id只能指定一个，不能够同时指定，指定table_id后会自动对此table下所有tablet执行full_compaction。

## 使用例子

```
curl -X POST "http://127.0.0.1:8040/api/compaction/run?tablet_id=10015&compact_type=full"
curl -X POST "http://127.0.0.1:8040/api/compaction/run?table_id=10104&compact_type=full"
```