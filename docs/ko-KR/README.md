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

## 🌍 다른 언어로 읽기

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris는 MPP 아키텍처를 기반으로 구축된 오픈 소스 실시간 분석 및 검색 데이터베이스입니다. 빠른 SQL 분석, 레이크하우스 쿼리 가속, 그리고 구조화된 데이터, 텍스트, 벡터 데이터를 아우르는 하이브리드 검색을 제공합니다.

최신 제품 개요, 사용 사례, 생태계 업데이트, 블로그, 사용자 사례는 [공식 웹사이트](https://doris.apache.org/)에서 확인하세요. 버전 업데이트는 [전체 릴리스 노트](https://doris.apache.org/releases/all-release)를 참조하세요.

## 📈 사용 사례

| 사용 사례 | 제공하는 것 |
| -------- | ---------------- |
| [고객 대상 분석](https://doris.apache.org/use-cases/customer-facing-analytics) | 외부 사용자에게 1초 미만의 대화형 분석을 제공합니다. |
| [데이터 웨어하우징](https://doris.apache.org/use-cases/data-warehousing) | 비즈니스 도메인 전반에 걸쳐 하나의 실시간 웨어하우스를 구축합니다. |
| [옵저버빌리티](https://doris.apache.org/use-cases/observability) | SQL로 대량의 로그, 이벤트, 메트릭을 분석합니다. |
| [AI를 위한 Doris](https://doris.apache.org/use-cases/ai) | 하나의 SQL 엔진에서 벡터, 텍스트, JSON, 구조화된 검색을 사용합니다. |

## 🚀 핵심 기능

Apache Doris는 세 가지 핵심 역량을 중심으로 설계되었습니다. 자세한 제품 설명과 예시는 웹사이트를 기준으로 확인하세요.

| 역량 | 제공하는 것 |
| ---------- | ---------------- |
| [실시간 분석](https://doris.apache.org/#real-time-analytics) | 높은 동시성 환경에서 스트리밍 수집, 증분 변환, 1초 미만 쿼리를 제공합니다. |
| [레이크하우스 분석](https://doris.apache.org/#lakehouse-analytics) | Iceberg, Delta Lake, Hudi 같은 오픈 테이블 형식에서 빠른 SQL 분석을 제공합니다. |
| [하이브리드 검색](https://doris.apache.org/#hybrid-search) | AI 및 검색 워크로드를 위해 JSON, 전문, 벡터 데이터를 SQL 네이티브 방식으로 분석합니다. |

## 🔌 생태계

Doris는 현대 데이터 스택의 중심에 있습니다. 업스트림 데이터베이스, 스트리밍 시스템, 레이크하우스 스토리지를 다운스트림 BI, AI, 분석, 옵저버빌리티 도구와 연결합니다.

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

최신 생태계 지원 범위는 [공식 웹사이트](https://doris.apache.org/)와 [연결 및 통합 문서](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro)를 참조하세요.

## 👣 시작하기

- [Apache Doris란 무엇인가요?](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [빠른 시작](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [다운로드](https://doris.apache.org/download)
- [설치](https://doris.apache.org/docs/dev/install/intro)
- [소스에서 컴파일하기](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 아키텍처

Apache Doris는 컴퓨팅과 스토리지가 결합된 배포와 분리된 배포를 모두 지원합니다. 분리 모드에서는 상태를 갖지 않는 컴퓨팅 그룹이 공유 객체 스토리지 위에서 실행되므로, 필요에 따라 컴퓨팅을 확장하고 워크로드를 격리할 수 있습니다.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

[배포 가이드](https://doris.apache.org/docs/dev/install/intro)와 [배포 모드 가이드](https://doris.apache.org/docs/dev/install/choosing-deployment-mode)에서 자세히 알아보세요.

## 📣 프로젝트 업데이트

| 리소스 | 제공하는 것 |
| -------- | ---------------- |
| [커뮤니티 리포트](https://doris.apache.org/community-report/) | 커뮤니티 활동, 병합된 PR, 기여자, 기능 진행 상황에 대한 주간 업데이트를 제공합니다. |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | AI와 하이브리드 검색, 쿼리 엔진, 스토리지, 데이터 레이크 작업에 대한 2026년 계획 논의입니다. |

## 🧩 컴포넌트

Doris는 일반적인 데이터 엔지니어링 워크플로를 위한 커넥터와 도구를 제공합니다.

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 사용자

Apache Doris는 인터넷 서비스, 금융, 리테일, 물류, 제조, 에너지, 통신, AI 등 다양한 산업에서 전 세계 수천 개 기업의 프로덕션 환경에 사용되고 있습니다.

- [Apache Doris 사용자](https://doris.apache.org/why-doris/users/)
- [회사 추가하기](https://github.com/apache/doris/discussions/27683)

## 🙌 기여자

Apache Doris는 Apache Incubator를 졸업하고 2022년 6월 Apache Top-Level Project가 되었습니다. Doris를 함께 만들어 가는 모든 [커뮤니티 기여자](https://github.com/apache/doris/graphs/contributors)에게 감사드립니다.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 커뮤니티 및 지원

- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [기여하는 방법](https://doris.apache.org/community/how-to-contribute/)
- [코드 제출 가이드](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 문의하기

- [Slack 참여하기](https://doris.apache.org/slack)
- [개발자 메일링 리스트 구독하기](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 링크

- Apache Doris 공식 웹사이트: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 라이선스

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **참고**
> 일부 서드파티 종속성의 라이선스는 Apache 2.0 License와 호환되지 않습니다. 따라서 Apache 2.0 License를 준수하려면
일부 Doris 기능을 비활성화해야 합니다. 자세한 내용은 `thirdparty/LICENSE.txt`를 참조하세요.
