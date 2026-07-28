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

## 🌍 閱讀其他語言版本

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [簡體中文](../zh-CN/README.md) • [繁體中文](README.md)

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

Apache Doris 是一個以 MPP 架構打造的開源即時分析與搜尋資料庫。它提供快速 SQL 分析、湖倉查詢加速，以及橫跨結構化資料、文字資料與向量資料的混合搜尋。

請瀏覽[官方網站](https://doris.apache.org/)，了解最新的產品概覽、使用情境、生態系更新、部落格文章與使用者案例。若要查看版本更新，請參閱[所有版本資訊](https://doris.apache.org/releases/all-release)。

## 📈 使用情境

| 使用情境 | 提供內容 |
| -------- | ---------------- |
| [客戶端分析](https://doris.apache.org/use-cases/customer-facing-analytics) | 為外部使用者提供亞秒級互動式分析。 |
| [資料倉儲](https://doris.apache.org/use-cases/data-warehousing) | 建構橫跨業務領域的單一即時資料倉儲。 |
| [可觀測性](https://doris.apache.org/use-cases/observability) | 使用 SQL 分析高吞吐量的日誌、事件與指標。 |
| [Doris 用於 AI](https://doris.apache.org/use-cases/ai) | 在單一 SQL 引擎中使用向量、文字、JSON 與結構化搜尋。 |

## 🚀 核心能力

Apache Doris 以三項核心能力為中心建構。官方網站是詳細產品說明與範例的準確來源。

| 能力 | 提供內容 |
| ---------- | ---------------- |
| [即時分析](https://doris.apache.org/#real-time-analytics) | 在高併發下進行串流匯入、增量轉換與亞秒級查詢。 |
| [湖倉分析](https://doris.apache.org/#lakehouse-analytics) | 對 Iceberg、Delta Lake、Hudi 等開放資料表格式進行快速 SQL 分析。 |
| [混合搜尋](https://doris.apache.org/#hybrid-search) | 以 SQL 原生方式分析 JSON、全文與向量資料，支援 AI 與搜尋工作負載。 |

## 🔌 生態系

Doris 位於現代資料堆疊的核心。它連接上游資料庫、串流系統與湖倉儲存，並銜接下游 BI、AI、分析與可觀測性工具。

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris 生態系" width="850" />
</p>

若要了解最新的生態系涵蓋範圍，請造訪[官方網站](https://doris.apache.org/)以及[連線與整合文件](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro)。

## 👣 開始使用

- [什麼是 Apache Doris](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [快速開始](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [下載](https://doris.apache.org/download)
- [安裝](https://doris.apache.org/docs/dev/install/intro)
- [從原始碼編譯](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 架構

Apache Doris 支援計算與儲存耦合，以及計算與儲存解耦的部署方式。在解耦模式下，無狀態計算群組會在共享物件儲存上執行，因此你可以依需求擴充計算資源並隔離工作負載。

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris 計算與儲存解耦架構" width="850" />
</p>

請在[部署指南](https://doris.apache.org/docs/dev/install/intro)與[部署模式指南](https://doris.apache.org/docs/dev/install/choosing-deployment-mode)了解更多資訊。

## 📣 專案更新

| 資源 | 提供內容 |
| -------- | ---------------- |
| [社群報告](https://doris.apache.org/community-report/) | 每週更新社群活動、已合併的 PR、貢獻者與功能進度。 |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | 關於 AI 與混合搜尋、查詢引擎、儲存和資料湖工作的 2026 年規劃討論。 |

## 🧩 元件

Doris 為常見資料工程工作流程提供連接器與工具。

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 使用者

Apache Doris 已被全球數千家公司用於生產環境，範圍涵蓋網路服務、金融、零售、物流、製造、能源、電信、AI 與其他產業。

- [Apache Doris 使用者](https://doris.apache.org/why-doris/users/)
- [新增你的公司](https://github.com/apache/doris/discussions/27683)

## 🙌 貢獻者

Apache Doris 從 Apache Incubator 畢業，並於 2022 年 6 月成為 Apache Top-Level Project。感謝所有協助建構 Doris 的[社群貢獻者](https://github.com/apache/doris/graphs/contributors)。

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 社群與支援

- [GitHub 議題](https://github.com/apache/doris/issues)
- [GitHub 討論](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [如何貢獻](https://doris.apache.org/community/how-to-contribute/)
- [程式碼提交指南](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 聯絡我們

- [加入 Slack](https://doris.apache.org/slack)
- [訂閱開發者郵件列表](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 連結

- Apache Doris 官方網站：[doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 授權

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **注意**
> 第三方相依套件的部分授權條款與 Apache 2.0 License 不相容。因此，你需要停用
部分 Doris 功能以符合 Apache 2.0 License。詳細資訊請參閱 `thirdparty/LICENSE.txt`
