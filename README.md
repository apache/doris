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

## 🌍 Read this in other language

[English](README.md) • [العربية](docs/ar-SA/README.md) • [বাংলা](docs/bn-BD/README.md) • [Deutsch](docs/de-DE/README.md) • [Español](docs/es-ES/README.md) • [فارسی](docs/fa-IR/README.md) • [Français](docs/fr-FR/README.md) • [हिन्दी](docs/hi-IN/README.md) • [Bahasa Indonesia](docs/id-ID/README.md) • [Italiano](docs/it-IT/README.md) • [日本語](docs/ja-JP/README.md) • [한국어](docs/ko-KR/README.md) • [Polski](docs/pl-PL/README.md) • [Português](docs/pt-BR/README.md) • [Română](docs/ro-RO/README.md) • [Русский](docs/ru-RU/README.md) • [Slovenščina](docs/sl-SI/README.md) • [ไทย](docs/th-TH/README.md) • [Türkçe](docs/tr-TR/README.md) • [Українська](docs/uk-UA/README.md) • [Tiếng Việt](docs/vi-VN/README.md) • [简体中文](docs/zh-CN/README.md) • [繁體中文](docs/zh-TW/README.md)

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

<p align="center">

  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

</p>

Apache Doris is an open-source, real-time analytics and search database built on MPP architecture. It provides fast SQL analytics, lakehouse query acceleration, and hybrid search across structured, text, and vector data.

Explore the [official website](https://doris.apache.org/) for the latest product overview, use cases, ecosystem updates, blogs, and user stories. For version updates, see [all release notes](https://doris.apache.org/releases/all-release).

## 📈 Use Cases

- [Customer-Facing Analytics](https://doris.apache.org/use-cases/customer-facing-analytics): Ship sub-second interactive analytics to external users.
- [Data Warehousing](https://doris.apache.org/use-cases/data-warehousing): Build one real-time warehouse across business domains.
- [Observability](https://doris.apache.org/use-cases/observability): Analyze high-throughput logs, events, and metrics with SQL.
- [Doris for AI](https://doris.apache.org/use-cases/ai): Use vector, text, JSON, and structured search in one SQL engine.

## 🚀 Core Capabilities

Apache Doris is built around three core capabilities. The website is the source of truth for detailed product descriptions and examples.

| Capability | What it provides |
| ---------- | ---------------- |
| [Real-Time Analytics](https://doris.apache.org/) | Streaming ingestion, incremental transformation, and sub-second queries under high concurrency. |
| [Lakehouse Analytics](https://doris.apache.org/) | Fast SQL analytics over open table formats such as Iceberg, Delta Lake, and Hudi. |
| [Hybrid Search](https://doris.apache.org/) | SQL-native analytics across JSON, full-text, and vector data for AI and search workloads. |

## 🔌 Ecosystem

Doris sits at the center of the modern data stack. It connects upstream databases, streaming systems, and lakehouse storage with downstream BI, AI, analytics, and observability tools.

<!-- Placeholder: this image should be exported from the website HomeNext EcosystemSection. -->
<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/ecosystem-overview.png" alt="Apache Doris ecosystem" width="850" />
</p>

For the latest ecosystem coverage, visit the [official website](https://doris.apache.org/) and the [connection and integration documentation](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro).

## 👣 Get Started

- [What is Apache Doris](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [Quick Start](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [Download](https://doris.apache.org/download)
- [Installation](https://doris.apache.org/docs/dev/install/intro)
- [Compile from source](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 Architecture

Apache Doris supports both compute-storage coupled and compute-storage decoupled deployments. In decoupled mode, stateless compute groups run over shared object storage, so you can scale compute on demand and isolate workloads.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

Learn more in the [deployment guide](https://doris.apache.org/docs/dev/install/intro) and [deployment mode guide](https://doris.apache.org/docs/dev/install/choosing-deployment-mode).

## 🧩 Components

Doris provides connectors and tools for common data engineering workflows.

- [apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)
- [apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 Users

Apache Doris is used in production by thousands of companies worldwide across internet services, finance, retail, logistics, manufacturing, energy, telecommunications, AI, and other industries.

- [Apache Doris users](https://doris.apache.org/why-doris/users/)
- [Add your company](https://github.com/apache/doris/discussions/27683)

## 🙌 Contributors

Apache Doris graduated from the Apache Incubator and became an Apache Top-Level Project in June 2022. Thanks to all [community contributors](https://github.com/apache/doris/graphs/contributors) who help build Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 Community and Support

- [Subscribe to the dev mailing list](https://doris.apache.org/community/subscribe-mail-list)
- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [How to contribute](https://doris.apache.org/community/how-to-contribute/)
- [Code submission guide](https://doris.apache.org/community/how-to-contribute/pull-request/)
- [Doris Improvement Proposals](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals)
- [Backend C++ coding specification](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637)

## 💬 Contact Us

| Name | Scope | Subscribe | Unsubscribe | Archives |
|:-----|:------|:----------|:------------|:---------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org) | Development discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org) | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org) | [Archives](http://mail-archives.apache.org/mod_mbox/doris-dev/) |

## 🧰 Links

- Apache Doris official website: [doris.apache.org](https://doris.apache.org)
- Slack channel: [Join Slack](https://doris.apache.org/slack)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Note**
> Some licenses of the third-party dependencies are not compatible with Apache 2.0 License. So you need to disable
some Doris features to comply with Apache 2.0 License. For details, refer to the `thirdparty/LICENSE.txt`
