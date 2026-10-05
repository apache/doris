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

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](README.md) • [繁體中文](../zh-TW/README.md)

<div align="center">

# Apache Doris

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![Slack](https://img.shields.io/badge/Join%20Our%20Community-Slack-blue)](https://doris.apache.org/slack)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/dev/getting-started/what-is-apache-doris)

<div>

[![Official Website](<https://img.shields.io/badge/-Visit%20the%20Official%20Website%20%E2%86%92-rgb(15,214,106)?style=for-the-badge>)](https://doris.apache.org/)
[![Quick Download](<https://img.shields.io/badge/-Quick%20%20Download%20%E2%86%92-rgb(66,56,255)?style=for-the-badge>)](https://doris.apache.org/download)


</div>


<div>
    <a href="https://twitter.com/doris_apache"><img src="https://img.shields.io/badge/- @Doris_Apache -424549?style=social&logo=x" height=25></a>
    &nbsp;
    <a href="https://github.com/apache/doris/discussions"><img src="https://img.shields.io/badge/- Discussion -red?style=social&logo=discourse" height=25></a>
    &nbsp;
    <a href="https://doris.apache.org/slack" height=25></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---

<div align="center">
  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>

Apache Doris 是基于 MPP 架构构建的开源实时分析与搜索数据库。它提供高速 SQL 分析、Lakehouse 查询加速，以及面向结构化数据、文本数据和向量数据的混合搜索能力。

访问 [官方网站](https://doris.apache.org/) 了解最新的产品概览、使用场景、生态更新、博客和用户案例。版本更新请参阅[全部发布说明](https://doris.apache.org/releases/all-release)。

## 📈 使用场景

| 使用场景 | 提供的能力 |
| -------- | ---------------- |
| [面向客户的分析](https://doris.apache.org/use-cases/customer-facing-analytics) | 为外部用户提供亚秒级交互式分析。 |
| [数据仓库](https://doris.apache.org/use-cases/data-warehousing) | 跨业务域构建统一的实时数据仓库。 |
| [可观测性](https://doris.apache.org/use-cases/observability) | 使用 SQL 分析高吞吐日志、事件和指标。 |
| [Doris for AI](https://doris.apache.org/use-cases/ai) | 在一个 SQL 引擎中使用向量、文本、JSON 和结构化搜索。 |

## 🚀 核心能力

Apache Doris 围绕三项核心能力构建。网站是详细产品说明和示例的权威来源。

| 能力 | 提供的能力 |
| ---------- | ---------------- |
| [实时分析](https://doris.apache.org/#real-time-analytics) | 在高并发场景下实现流式导入、增量转换和亚秒级查询。 |
| [Lakehouse 分析](https://doris.apache.org/#lakehouse-analytics) | 对 Iceberg、Delta Lake 和 Hudi 等开放表格式进行高速 SQL 分析。 |
| [混合搜索](https://doris.apache.org/#hybrid-search) | 面向 AI 和搜索工作负载，对 JSON、全文和向量数据进行 SQL 原生分析。 |

## 🔌 生态系统

Doris 位于现代数据栈的中心。它连接上游数据库、流系统和 Lakehouse 存储，并对接下游 BI、AI、分析和可观测性工具。

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

如需了解最新的生态覆盖范围，请访问[官方网站](https://doris.apache.org/)以及[连接与集成文档](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro)。

## 👣 快速开始

- [什么是 Apache Doris](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [快速入门](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [下载](https://doris.apache.org/download)
- [安装](https://doris.apache.org/docs/dev/install/intro)
- [从源码编译](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 架构

Apache Doris 同时支持计算存储耦合和计算存储分离部署。在分离模式下，无状态计算组运行在共享对象存储之上，因此你可以按需扩展计算资源并隔离工作负载。

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

请在[部署指南](https://doris.apache.org/docs/dev/install/intro)和[部署模式指南](https://doris.apache.org/docs/dev/install/choosing-deployment-mode)中了解更多信息。

## 📣 项目动态

| 资源 | 提供的内容 |
| -------- | ---------------- |
| [社区报告](https://doris.apache.org/community-report/) | 每周更新社区活动、已合并 PR、贡献者和功能进展。 |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | 面向 AI 与混合搜索、查询引擎、存储和数据湖工作的 2026 年规划讨论。 |

## 🧩 组件

Doris 为常见数据工程流程提供连接器和工具。

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 用户

全球数千家公司在生产环境中使用 Apache Doris，覆盖互联网服务、金融、零售、物流、制造、能源、电信、AI 等行业。

- [Apache Doris 用户](https://doris.apache.org/why-doris/users/)
- [添加你的公司](https://github.com/apache/doris/discussions/27683)

## 🙌 贡献者

Apache Doris 从 Apache Incubator 毕业，并于 2022 年 6 月成为 Apache Top-Level Project。感谢所有帮助构建 Doris 的[社区贡献者](https://github.com/apache/doris/graphs/contributors)。

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 社区与支持

- [GitHub 问题](https://github.com/apache/doris/issues)
- [GitHub 讨论](https://github.com/apache/doris/discussions)
- [Pull Request 列表](https://github.com/apache/doris/pulls)
- [如何贡献](https://doris.apache.org/community/how-to-contribute/)
- [代码提交指南](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 联系我们

- [加入 Slack](https://doris.apache.org/slack)
- [订阅开发者邮件列表](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 链接

- Apache Doris 官方网站：[doris.apache.org](https://doris.apache.org)
- X：[@doris_apache](https://twitter.com/doris_apache)
- Medium：[@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 许可证

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **注意**
> 第三方依赖的某些许可证与 Apache 2.0 License 不兼容。因此，你需要禁用
部分 Doris 功能，以符合 Apache 2.0 License。详情请参阅 `thirdparty/LICENSE.txt`
