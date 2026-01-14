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

## 🌍 阅读其他语言版本

[English](../../README.md) • [简体中文](README.md) • [繁體中文](../zh-TW/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Español](../es-ES/README.md) • [Français](../fr-FR/README.md) • [Deutsch](../de-DE/README.md) • [العربية](../ar-SA/README.md) • [Türkçe](../tr-TR/README.md) • [Tiếng Việt](../vi-VN/README.md) • [বাংলা](../bn-BD/README.md) • [Română](../ro-RO/README.md) • [Polski](../pl-PL/README.md) • [हिन्दी](../hi-IN/README.md) • [فارسی](../fa-IR/README.md) • [Slovenščina](../sl-SI/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Português](../pt-BR/README.md) • [Русский](../ru-RU/README.md) • [Italiano](../it-IT/README.md) • [ไทย](../th-TH/README.md) • [Українська](../uk-UA/README.md)

<div align="center">

# Apache Doris

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![OSSRank](https://shields.io/endpoint?url=https://ossrank.com/shield/516)](https://ossrank.com/p/516)
[![Commit activity](https://img.shields.io/github/commit-activity/m/apache/doris)](https://github.com/apache/doris/commits/master/)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/gettingStarted/what-is-apache-doris)

<div>

[![Official Website](<https://img.shields.io/badge/-Visit%20the%20Official%20Website%20%E2%86%92-rgb(15,214,106)?style=for-the-badge>)](https://doris.apache.org/)
[![Quick Download](<https://img.shields.io/badge/-Quick%20%20Download%20%E2%86%92-rgb(66,56,255)?style=for-the-badge>)](https://doris.apache.org/download)


</div>


<div>
    <a href="https://twitter.com/doris_apache"><img src="https://img.shields.io/badge/- @Doris_Apache -424549?style=social&logo=x" height=25></a>
    &nbsp;
    <a href="https://github.com/apache/doris/discussions"><img src="https://img.shields.io/badge/- Discussion -red?style=social&logo=discourse" height=25></a>
    &nbsp;
    <a href="https://doris.apache.org/slack"><img src="https://img.shields.io/badge/-Slack-4A154B?style=social&logo=slack" height=25 alt="Slack"></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---

<p align="center">

  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

</p>




Apache Doris 是一个基于 MPP 架构的易用、高性能、实时的分析型数据库，以其极速和易用性而闻名。它仅需亚秒级响应时间即可返回海量数据下的查询结果，不仅可以支持高并发的点查询场景，也能支持高吞吐的复杂分析场景。

所有这些特性使得 Apache Doris 成为报表分析、即席查询、统一数仓构建、数据湖查询加速等场景的理想工具。在 Apache Doris 上，用户可以构建各种应用，如用户行为分析、AB 测试平台、日志检索分析、用户画像分析、订单分析等。

🎉 查看 🔗[所有版本](https://doris.apache.org/docs/releasenotes/all-release)，您将找到过去一年发布的 Apache Doris 版本的按时间顺序总结。

👀 探索 🔗[官方网站](https://doris.apache.org/)，详细了解 Apache Doris 的核心功能、博客和用户案例。

## 📈 使用场景

如下图所示，经过各种数据集成和处理后，数据源通常存储在实时数据仓库 Apache Doris 和离线数据湖或数据仓库（在 Apache Hive、Apache Iceberg 或 Apache Hudi 中）中。

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris 广泛应用于以下场景：

- **实时数据分析**：

  - **实时报表和决策**：Doris 为内部和外部企业使用提供实时更新的报表和仪表板，支持自动化流程中的实时决策。
  
  - **即席分析**：Doris 提供多维数据分析能力，支持快速商业智能分析和即席查询，帮助用户快速从复杂数据中挖掘洞察。
  
  - **用户画像和行为分析**：Doris 可以分析用户行为，如参与度、留存率和转化率，同时支持人群洞察和人群选择等行为分析场景。

- **数据湖分析**：

  - **数据湖查询加速**：Doris 通过其高效的查询引擎加速数据湖数据查询。
  
  - **联邦分析**：Doris 支持跨多个数据源的联邦查询，简化架构并消除数据孤岛。
  
  - **实时数据处理**：Doris 结合实时数据流和批处理能力，满足高并发和低延迟复杂业务需求。

- **基于 SQL 的可观测性**：

  - **日志和事件分析**：Doris 支持对分布式系统中的日志和事件进行实时或批量分析，帮助识别问题并优化性能。


## 整体架构

Apache Doris 使用 MySQL 协议，与 MySQL 语法高度兼容，并支持标准 SQL。用户可以通过各种客户端工具访问 Apache Doris，并且它可以与 BI 工具无缝集成。

### 存算一体架构

Apache Doris 的存算一体架构简洁且易于维护。如下图所示，它仅由两种类型的进程组成：

- **Frontend (FE)：** 主要负责处理用户请求、查询解析和规划、元数据管理和节点管理任务。

- **Backend (BE)：** 主要负责数据存储和查询执行。数据被分区为分片，并在 BE 节点之间以多个副本存储。

![Apache Doris 的整体架构](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

在生产环境中，可以部署多个 FE 节点以实现容灾。每个 FE 节点维护元数据的完整副本。FE 节点分为三种角色：

| 角色      | 功能                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | FE Master 节点负责元数据的读写操作。当 Master 节点发生元数据变更时，它们通过 BDB JE 协议同步到 Follower 或 Observer 节点。 |
| Follower  | Follower 节点负责读取元数据。如果 Master 节点失败，可以选择 Follower 节点作为新的 Master。 |
| Observer  | Observer 节点负责读取元数据，主要用于增加查询并发。它不参与集群领导选举。 |

FE 和 BE 进程都可以水平扩展，使单个集群能够支持数百台机器和数十 PB 的存储容量。FE 和 BE 进程使用一致性协议来确保服务的高可用性和数据的高可靠性。存算一体架构高度集成，显著降低了分布式系统的运维复杂度。


## Apache Doris 的核心特性

- **高可用性**：在 Apache Doris 中，元数据和数据都以多个副本存储，通过 quorum 协议同步数据日志。一旦大多数副本完成写入，数据写入即被视为成功，确保即使少数节点失败，集群仍然可用。Apache Doris 支持同城和跨地域容灾，支持双集群主从模式。当某些节点出现故障时，集群可以自动隔离故障节点，防止整体集群可用性受到影响。

- **高兼容性**：Apache Doris 与 MySQL 协议高度兼容，支持标准 SQL 语法，涵盖大多数 MySQL 和 Hive 函数。这种高兼容性使用户能够无缝迁移和集成现有应用程序和工具。Apache Doris 支持 MySQL 生态系统，使用户能够使用 MySQL 客户端工具连接 Doris，实现更便捷的运维。它还支持 BI 报表工具和数据传输工具的 MySQL 协议兼容性，确保数据分析和数据传输过程的效率和稳定性。

- **实时数据仓库**：基于 Apache Doris，可以构建实时数据仓库服务。Apache Doris 提供秒级数据摄取能力，在几秒钟内将上游在线事务数据库的增量变更捕获到 Doris 中。利用向量化引擎、MPP 架构和 Pipeline 执行引擎，Doris 提供亚秒级数据查询能力，从而构建高性能、低延迟的实时数据仓库平台。

- **统一数据湖**：Apache Doris 可以基于外部数据源（如数据湖或关系数据库）构建统一数据湖架构。Doris 统一数据湖解决方案实现了数据湖和数据仓库之间的无缝集成和自由数据流动，帮助用户直接利用数据仓库能力解决数据湖中的数据分析问题，同时充分利用数据湖数据管理能力来增强数据价值。

- **灵活建模**：Apache Doris 提供各种建模方法，如宽表模型、预聚合模型、星型/雪花型模式等。在数据导入期间，数据可以扁平化为宽表，并通过 Flink 或 Spark 等计算引擎写入 Doris，或者数据可以直接导入到 Doris，通过视图、物化视图或实时多表连接执行数据建模操作。

## 技术概述

Doris 提供高效的 SQL 接口，完全兼容 MySQL 协议。其查询引擎基于 MPP（大规模并行处理）架构，能够高效执行复杂的分析查询并实现低延迟实时查询。通过用于数据编码和压缩的列式存储技术，它显著优化了查询性能和存储压缩比。

### 接口

Apache Doris 采用 MySQL 协议，支持标准 SQL，并与 MySQL 语法高度兼容。用户可以通过各种客户端工具访问 Apache Doris，并与 BI 工具无缝集成，包括但不限于 Smartbi、DataEase、FineBI、Tableau、Power BI 和 Apache Superset。Apache Doris 可以作为任何支持 MySQL 协议的 BI 工具的数据源。

### 存储引擎

Apache Doris 具有列式存储引擎，按列对数据进行编码、压缩和读取。这使得数据压缩比非常高，并大大减少了不必要的数据扫描，从而更有效地利用 IO 和 CPU 资源。

Apache Doris 支持各种索引结构以最小化数据扫描：

- **排序复合键索引**：用户最多可以指定三列形成复合排序键。这可以有效地剪枝数据，更好地支持高并发报表场景。

- **Min/Max 索引**：这在数值类型的等价和范围查询中实现有效的数据过滤。

- **BloomFilter 索引**：这在等价过滤和高基数列的剪枝中非常有效。

- **倒排索引**：这实现了对任何字段的快速搜索。

Apache Doris 支持多种数据模型，并针对不同场景进行了优化：

- **明细模型（Duplicate Key Model）：** 一种明细数据模型，旨在满足事实表的详细存储需求。

- **主键模型（Unique Key Model）：** 确保唯一键；具有相同键的数据会被覆盖，实现行级数据更新。

- **聚合模型（Aggregate Key Model）：** 合并具有相同键的值列，通过预聚合显著提高性能。

Apache Doris 还支持强一致的单表物化视图和异步刷新的多表物化视图。单表物化视图由系统自动刷新和维护，无需用户手动干预。多表物化视图可以使用集群内调度或外部调度工具定期刷新，降低数据建模的复杂度。

### 🔍 查询引擎

Apache Doris 具有基于 MPP 的查询引擎，用于节点间和节点内的并行执行。它支持大表的分布式 shuffle join，以更好地处理复杂查询。

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Apache Doris 的查询引擎完全向量化，所有内存结构都以列式格式布局。这可以大大减少虚函数调用，提高缓存命中率，并有效利用 SIMD 指令。Apache Doris 在宽表聚合场景中的性能比非向量化引擎高 5~10 倍。

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris 使用自适应查询执行技术，根据运行时统计信息动态调整执行计划。例如，它可以生成运行时过滤器并将其推送到探测端。具体来说，它将过滤器推送到探测端最底层的扫描节点，这大大减少了要处理的数据量并提高了连接性能。Apache Doris 的运行时过滤器支持 In/Min/Max/Bloom Filter。

Apache Doris 使用 Pipeline 执行引擎，将查询分解为多个子任务进行并行执行，充分利用多核 CPU 能力。它同时通过限制查询线程数来解决线程爆炸问题。Pipeline 执行引擎减少了数据复制和共享，优化了排序和聚合操作，从而显著提高了查询效率和吞吐量。

在优化器方面，Apache Doris 采用 CBO（基于成本的优化器）、RBO（基于规则的优化器）和 HBO（基于历史的优化器）的组合优化策略。RBO 支持常量折叠、子查询重写、谓词下推等。CBO 支持连接重排序和其他优化。HBO 根据历史查询信息推荐最佳执行计划。这些多重优化措施确保 Doris 能够在各种类型的查询中枚举高性能查询计划。


## 🎆 为什么选择 Apache Doris？

- 🎯 **易于使用**：两个进程，无其他依赖；在线集群扩展，自动副本恢复；兼容 MySQL 协议，使用标准 SQL。

- 🚀 **高性能**：通过列式存储引擎、现代 MPP 架构、向量化查询引擎、预聚合物化视图和数据索引，实现低延迟和高吞吐量查询的极快性能。

- 🖥️ **统一系统**：单一系统可以支持实时数据服务、交互式数据分析和离线数据处理场景。

- ⚛️ **联邦查询**：支持对 Hive、Iceberg、Hudi 等数据湖和 MySQL、Elasticsearch 等数据库进行联邦查询。

- ⏩ **多种数据导入方式**：支持从 HDFS/S3 批量导入和从 MySQL Binlog/Kafka 流式导入；支持通过 HTTP 接口进行微批写入和使用 JDBC 中的 Insert 进行实时写入。

- 🚙 **丰富生态**：Spark 使用 Spark-Doris-Connector 读写 Doris；Flink-Doris-Connector 使 Flink CDC 能够实现精确一次的数据写入到 Doris；提供 DBT Doris Adapter 以使用 DBT 在 Doris 中转换数据。

## 🙌 贡献者

**Apache Doris 已成功从 Apache 孵化器毕业，并于 2022 年 6 月成为顶级项目**。

我们深深感谢 🔗[社区贡献者](https://github.com/apache/doris/graphs/contributors) 对 Apache Doris 的贡献。

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 用户

Apache Doris 现在在中国和世界各地拥有广泛的用户基础，截至今天，**Apache Doris 在全球数千家公司的生产环境中使用**。按市值或估值计算，中国前 50 大互联网公司中超过 80% 的公司长期使用 Apache Doris，包括百度、美团、小米、京东、字节跳动、腾讯、网易、快手、新浪、360、米哈游和贝壳控股。它还在金融、能源、制造业和电信等一些传统行业中得到广泛应用。

Apache Doris 的用户：🔗[用户](https://doris.apache.org/users)

在 Apache Doris 网站上添加您的公司徽标：🔗[添加您的公司](https://github.com/apache/doris/discussions/27683)
 
## 👣 快速开始

### 📚 文档

所有文档   🔗[文档](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ 下载 

所有版本和二进制版本 🔗[下载](https://doris.apache.org/download) 

### 🗄️ 编译

查看如何编译  🔗[编译](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 安装

查看如何安装和部署 🔗[安装和部署](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 组件

### 📝 Doris Connector

Doris 通过 Connector 支持 Spark/Flink 读取存储在 Doris 中的数据，也支持通过 Connector 将数据写入 Doris。

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 社区和支持

### 📤 订阅邮件列表

邮件列表是 Apache 社区中最受认可的交流形式。查看如何 🔗[订阅邮件列表](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 报告问题或提交 Pull Request

如果您遇到任何问题，请随时提交 🔗[GitHub Issue](https://github.com/apache/doris/issues) 或在 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) 中发布，并通过提交 🔗[Pull Request](https://github.com/apache/doris/pulls) 来修复

### 🍻 如何贡献

我们欢迎您的建议、评论（包括批评）、意见和贡献。查看 🔗[如何贡献](https://doris.apache.org/community/how-to-contribute/) 和 🔗[代码提交指南](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Doris 改进提案 (DSIP)

🔗[Doris 改进提案 (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) 可以被视为 **所有主要功能更新或改进的设计文档集合**。

### 🔑 后端 C++ 编码规范
🔗 [后端 C++ 编码规范](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) 应严格遵守，这将帮助我们实现更好的代码质量。

## 💬 联系我们

通过以下邮件列表联系我们。

| 名称                                                                          | 范围                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | 开发相关讨论 | [订阅](mailto:dev-subscribe@doris.apache.org)   | [取消订阅](mailto:dev-unsubscribe@doris.apache.org)   | [归档](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 链接

* Apache Doris 官方网站 - [网站](https://doris.apache.org)
* 开发者邮件列表 - <dev@doris.apache.org>。发送邮件至 <dev-subscribe@doris.apache.org>，按照回复订阅邮件列表。
* Slack 频道 - [加入 Slack](https://doris.apache.org/slack)
* Twitter - [关注 @doris_apache](https://twitter.com/doris_apache)


## 📜 许可证

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **注意**
> 某些第三方依赖项的许可证与 Apache 2.0 许可证不兼容。因此，您需要禁用某些 Doris 功能以符合 Apache 2.0 许可证。有关详细信息，请参阅 `thirdparty/LICENSE.txt`


