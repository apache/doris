---
{
    "title": "Release 2.0-Alpha",
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

Apache Doris 2.0-Alpha 版本是 2.0 系列的首个版本，包含了倒排索引、高并发点查询、冷热数据分离、Pipeline 执行引擎、全新查询优化器 Nereids 等众多重要特性，主要是作为最新特性的功能验证。因此建议在新的测试集群中部署 2.0-Alpha 版本进行测试，但**不应部署在生产集群中**。


# 重要特性

### 1. 半结构化数据存储与极速分析

- 全新倒排索引：支持全文检索以及更加高效的等值查询、范围查询
  - 增加了字符串类型的全文检索
    - 支持英文、中文分词
    - 支持字符串类型和字符串数组类型的全文检索
  - 支持字符串、数值、日期时间类型的等值查询和范围查询
  - 支持多个条件的逻辑组合，不仅支持 AND，还支持 OR 和 not 
  - 在 esrally http 日志基准测试中，与 Elasticsearch 相比效率更高：数据导入速度提高了 4 倍，存储资源消耗减少了80%，查询速度提高了 2 倍以上

	参考文档：[https://doris.apache.org/zh-CN/docs/dev/data-table/index/inverted-index](https://doris.apache.org/zh-CN/docs/dev/data-table/index/inverted-index)

- 复杂数据类型
  - JSONB 数据类型通过 simdjson 获得更高效的首次数据解析速度
  - ARRAY 数据类型更加成熟，增加了数十个数组函数
  - 新增 MAP 数据类型，用于存储 Key-Value 键值对数据
  - 新增 STRUCT 数据类型，通过数据类型的嵌套可以存储更加复杂的数据结构

### 2. 高并发低延迟点查询

- 引入行式存储格式和行式缓存以加快整行数据的读取速度
- 实现短路径查询计划，在执行主键查询如 `SELECT * FROM tablex WHERE id = xxx`性能表现更佳
- 使用 PreparedStatement 缓存已编译的查询计划
- 在单台 16 Core 64G 内存 4*1T 硬盘规格的云服务器上实现了单节点 30000 QPS 的并发表现，较过去版本提升超 20倍

	参考文档：[https://doris.apache.org/zh-CN/docs/dev/query-acceleration/hight-concurrent-point-query](https://doris.apache.org/zh-CN/docs/dev/query-acceleration/hight-concurrent-point-query)

### 3. Vertical Compaction（默认开启）

- 将 Rowset 按照列切分为列组，按列合并数据，单次合并只需要加载部分列的数据，因此能够极大减少合并过程中的内存占用，提高压缩的执行速度。
- 在实际测试中，Vertical Compaction 使用内存仅为原有 Compaction 算法的 1/10，Compaction 速率提升 15%。

	参考文档：[https://doris.apache.org/zh-CN/docs/dev/advanced/best-practice/compaction/#vertical-compaction](https://doris.apache.org/zh-CN/docs/dev/advanced/best-practice/compaction/#vertical-compaction)

### 4. 冷热数据分离

- 用户可以通过 SQL 设置冷热数据策略，从而将历史数据转移到对象存储等廉价存储中，以降低存储成本。
- 冷数据仍然可以被直接访问，Doris 提供了本地缓存以提高冷数据的访问效率。

	参考文档：[https://doris.apache.org/zh-CN/docs/dev/advanced/cold_hot_separation](https://doris.apache.org/zh-CN/docs/dev/advanced/cold_hot_separation)


### 5. Pipeline 执行引擎（默认未开启）

- 阻塞算子异步化：各个查询执行过程之中的阻塞算子被拆分成不同 Pipeline，各个 Pipeline 能否获取执行线程调度执行取决于前置数据是否就绪。阻塞算子将不再占用线程资源，不再产生线程切换的开销。
- 自适应负载：采用多级反馈队列来调度查询优先级。在混合负载场景中，每个查询都可以公平地分配到一个固定的线程调度时间片，从而确保 Doris 可以在不同的负载下具有更稳定的性能表现。
- 可控的线程数目：Pipeline 执行引擎默认的执行线程数目为 CPU 和核数，Doris 启动了对应的执行线程池进行执行线程的管理。用户的 SQL 执行和线程进行了解绑，对于线程的资源使用更加可控。

	参考文档：[https://doris.apache.org/zh-CN/docs/dev/query-acceleration/pipeline-execution-engine](https://doris.apache.org/zh-CN/docs/dev/query-acceleration/pipeline-execution-engine)

### 6. 基于代价模型的全新查询优化器 Nereids （默认未开启）

- 更智能：新优化器将每个 RBO 和 CBO 的优化点以规则的形式呈现。对于每一个规则，新优化器都提供了一组用于描述查询计划形状的模式，可以精确的匹配可优化的查询计划。基于此，新优化器更好的可以支持诸如多层子查询嵌套等更为复杂的查询语句。
同时新优化器的 CBO 基于先进的 cascades 框架，使用了更为丰富的数据统计信息，并应用了维度更科学的代价模型。这使得新优化器在面对多表 Join 的查询时更加得心应手。
- 更健壮：新优化器的所有优化规则，均在逻辑执行计划树上完成。当查询语法语义解析完成后，变转换为一颗树状结构。相比于旧优化器的内部数据结构更为合理、统一。以子查询处理为例，新优化器基于新的数据结构，避免了旧优化器中众多规则对于子查询的单独处理。进而减少了优化规则逻辑错误的可能。
- 更灵活：新优化器的架构设计更合理，更现代。可以方便的扩展优化规则和处理阶段。能够更为迅速的响应用户的需求。

	参考文档：[https://doris.apache.org/zh-CN/docs/dev/query-acceleration/nereids](https://doris.apache.org/zh-CN/docs/dev/query-acceleration/nereids)

# 行为变更

- 默认开启 Light Weight Schema Change。
- 默认使用 datev2、datetimev2、decimalv3 来创建表，不支持 datav1、datetimev1、decimalv2 创建表。

	参考 PR：[https://github.com/apache/doris/pull/19077](https://github.com/apache/doris/pull/19077)

- 在 JDBC 和 Iceberg 的 Catalog 中默认使用 Decimalv3。

	参考 PR：[https://github.com/apache/doris/pull/18926](https://github.com/apache/doris/pull/18926)

- 在 BE 的启动脚本中，增加了 max_openfiles 和 swap 的检查，所以如果系统配置不合理，BE 有可能会启动失败。

	参考 PR：[https://github.com/apache/doris/pull/18888](https://github.com/apache/doris/pull/18888)

- 禁止在 localhost 访问 FE 时无密码登录。

	参考 PR：[https://github.com/apache/doris/pull/18816](https://github.com/apache/doris/pull/18816)

- 当系统中存在 Multi Catalog 时，查询 Information Schema 的数据时，默认只显示 Internal Catalog 的数据。

	参考 PR：[https://github.com/apache/doris/pull/18662](https://github.com/apache/doris/pull/18662)

- 对 Doris 进程名重命名为 DorisFE 和 DorisBE。

	参考 PR：[https://github.com/apache/doris/pull/18167](https://github.com/apache/doris/pull/18167)

- 系统中移除了非向量化代码，所以 `enable_vectorized_engine` 参数不再生效。

	参考 PR：[https://github.com/apache/doris/pull/18166](https://github.com/apache/doris/pull/18166)

- 限制了表达式树的深度，默认为 200。

	参考 PR：[https://github.com/apache/doris/pull/17314](https://github.com/apache/doris/pull/17314)

- 为了与 BI 工具兼容，在 `show create table` 时，将 datev2 和 datetimev2 显示为 date 和 datetime. 

	参考 PR：[https://github.com/apache/doris/pull/18358](https://github.com/apache/doris/pull/18358)

