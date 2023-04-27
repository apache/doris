---
{
    "title": "Release 1.1.2",
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


在 Apache Doris 1.1.2 版本中，我们引入了新的 Memtracker、极大程度上避免 OOM 类问题的发生，提升了向量化执行引擎在多数查询场景的性能表现，修复了诸多导致 BE 和 FE 发生异常的问题，优化了在湖仓联邦查询场景的部分体验问题并提升访问外部数据的性能。

相较于 1.1.1 版本，在 1.1.2 版本中有超过 170 个 Issue 和性能优化项被合入，系统稳定性和性能都得到进一步加强。与此同时，1.1.2 版本还将作为 Apache Doris 首个 LTS （Long-term Support）长周期支持版本，后续长期维护和支持，推荐所有用户下载和升级。

# 新增功能

### MemTracker

MemTracker 是一个用于分析内存使用情况的统计工具，在 1.1.1 版本中我们引入了简易版 Memtracker 用以控制 BE 侧内存。在 1.1.2 版本中，我们引入了新的 MemTracker，在向量化执行引擎和非向量化执行引擎中都更为准确。

### 增加展示和取消正在执行 Query 的 API

`GET /rest/v2/manager/query/current_queries`

`GET /rest/v2/manager/query/kill/{query_id}`

具体使用参考文档 [Query Profile Action](https://doris.apache.org/zh-CN/docs/dev/admin-manual/http-actions/fe/manager/query-profile-action?_highlight=current&_highlight=query#request)

### 支持读写 Emoji 表情通过 ODBC 外表


# 优化改进

### 数据湖相关改进

- 扫描 HDFS ORC 文件时性能提升约 300%。[#11501](https://github.com/apache/doris/pull/11501)

- 查询 Iceberg 表支持 HDFS 的 HA 模式。

- 支持查询由 [Apache Tez](https://tez.apache.org/) 创建的 Hive 数据

- 添加阿里云 OSS 作为 Hive 外部支持

### 在 Spark Load 中增加对 String 字符串类型和 Text 文本类型的支持


### 在非向量化引擎支持复用 Block，在某些场景中有 50%性能提升。[#11392](https://github.com/apache/doris/pull/11392)

### 提升 Like 和正则表达式的性能

### 禁用 TCMalloc 的 aggressive_memory_decommit。

在查询或导入时将会有 40% 性能提升，也可以在配置文件中通过 `tc_enable_aggressive_memory_decommit`来修改

# Bug Fix

### 修复部分可能导致 FE 失败或者数据损坏的问题

- 在 HA 环境中，BDBJE 将保留尽可能多的文件，通过增加配置 `bdbje_reserved_disk_bytes `以避免产生太多的 BDBJE 文件，BDBJE 日志只有在接近磁盘限制时才会删除。

- 修复了 BDBJE 中的重要错误，该错误将导致 FE 副本无法正确启动或数据损坏。

### 修复 FE 在查询过程中会在 waitFor_rpc 上 Hang 住以及 BE 在高并发情况下会 Hang 住的问题。

[#12459](https://github.com/apache/doris/pull/12459) [#12458](https://github.com/apache/doris/pull/12458) [#12392](https://github.com/apache/doris/pull/12392)

### 修复向量化执行引擎查询时得到错误结果的问题。

[#11754](https://github.com/apache/doris/pull/11754) [#11694](https://github.com/apache/doris/pull/11694)

### 修复许多 Planner 导致 BE Core 或者处于不正常状态的问题。

[#12080](https://github.com/apache/doris/pull/12080) [#12075](https://github.com/apache/doris/pull/12075) [#12040](https://github.com/apache/doris/pull/12040) [#12003](https://github.com/apache/doris/pull/12003) [#12007](https://github.com/apache/doris/pull/12007) [#11971](https://github.com/apache/doris/pull/11971) [#11933](https://github.com/apache/doris/pull/11933) [#11861](https://github.com/apache/doris/pull/11861) [#11859](https://github.com/apache/doris/pull/11859) [#11855](https://github.com/apache/doris/pull/11855) [#11837](https://github.com/apache/doris/pull/11837) [#11834](https://github.com/apache/doris/pull/11834) [#11821](https://github.com/apache/doris/pull/11821) [#11782](https://github.com/apache/doris/pull/11782) [#11723](https://github.com/apache/doris/pull/11723) [#11569](https://github.com/apache/doris/pull/11569)

