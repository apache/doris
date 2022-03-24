---
{
    "title": "使用示例",
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

# 使用示例

Doris 代码库中提供了丰富的使用示例，能够帮助 Doris 用户快速上手体验 Doris 的功能。

## 示例说明

示例代码都存放在 Doris 代码库的 [`samples/`](https://github.com/apache/incubator-doris/tree/master/samples) 目录下。

```
.
├── connect
├── doris-demo
├── insert
└── mini_load
```

* `connect/`

    该目录下主要展示了各个程序语言连接 Doris 的代码示例。
    
* `doris-demo/`

    该目下主要以 Maven 工程的形式，展示了 Doris 多个功能的代码示例。如 spark-connector 和 flink-connector 的使用示例、与 Spring 框架集成的示例、Stream Load 导入示例等等。
    
* `insert/`

    该目录展示了通过 python 或 shell 脚本调用 Doris 的 Insert 命令导入数据的一些代码示例。
    
* `miniload/`

    该目录展示了通过 python 调用 mini load 进行数据导入的代码示例。但因为 mini load 功能已由 stream load 功能代替，建议使用 stream load 功能进行数据导入。