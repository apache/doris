---
{
    "title": "DataX doriswriter",
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

# DataX doriswriter

[DataX](https://github.com/alibaba/DataX) doriswriter 插件，用于通过 DataX 同步其他数据源的数据到 Doris 中。

## 编译安装

插件代码位于 [这里](https://github.com/apache/incubator-doris/tree/master/extension/DataX)

该插件通过 Doris 的 Stream Load 功能进行数据的同步和导入。需要配合 DataX 服务使用。

请参阅 [README](https://github.com/apache/incubator-doris/blob/master/extension/DataX/README) 文档进行编译安装操作。

## 插件使用

doriswriter 插件的使用说明请参阅 [这里](https://github.com/apache/incubator-doris/blob/master/extension/DataX/doriswriter/doc/doriswriter.md)