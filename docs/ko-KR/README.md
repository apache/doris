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

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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
    <a href="https://doris.apache.org/slack" height=25></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---




<p align="center">

  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

</p>




Apache Doris는 MPP 아키텍처를 기반으로 한 사용하기 쉽고 고성능이며 실시간 분석 데이터베이스로, 극도로 빠른 속도와 사용 편의성으로 유명합니다. 대량의 데이터에서도 서브초 수준의 응답 시간으로 쿼리 결과를 반환할 수 있으며, 높은 동시성 포인트 쿼리 시나리오뿐만 아니라 높은 처리량의 복잡한 분석 시나리오도 지원할 수 있습니다.

이러한 모든 특성으로 인해 Apache Doris는 보고서 분석, 임시 쿼리, 통합 데이터 웨어하우스 구축, 데이터 레이크 쿼리 가속화 등의 시나리오에 이상적인 도구가 되었습니다. Apache Doris에서 사용자는 사용자 행동 분석, AB 테스트 플랫폼, 로그 검색 분석, 사용자 프로필 분석, 주문 분석 등 다양한 애플리케이션을 구축할 수 있습니다.

🎉 🔗[모든 릴리스](https://doris.apache.org/docs/releasenotes/all-release)를 확인하여 지난 1년간 릴리스된 Apache Doris 버전의 시간순 요약을 찾아보세요.

👀 🔗[공식 웹사이트](https://doris.apache.org/)를 탐색하여 Apache Doris의 핵심 기능, 블로그 및 사용자 사례를 자세히 알아보세요.

## 📈 사용 시나리오

아래 그림과 같이 다양한 데이터 통합 및 처리 후, 데이터 소스는 일반적으로 실시간 데이터 웨어하우스 Apache Doris와 오프라인 데이터 레이크 또는 데이터 웨어하우스(Apache Hive, Apache Iceberg 또는 Apache Hudi 내)에 저장됩니다.

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris는 다음 시나리오에서 광범위하게 사용됩니다:

- **실시간 데이터 분석**:

  - **실시간 보고 및 의사 결정**: Doris는 내부 및 외부 기업 사용을 위한 실시간 업데이트 보고서 및 대시보드를 제공하여 자동화된 프로세스에서 실시간 의사 결정을 지원합니다.
  
  - **임시 분석**: Doris는 다차원 데이터 분석 기능을 제공하여 빠른 비즈니스 인텔리전스 분석 및 임시 쿼리를 가능하게 하여 사용자가 복잡한 데이터에서 빠르게 인사이트를 발견할 수 있도록 합니다.
  
  - **사용자 프로파일링 및 행동 분석**: Doris는 참여, 유지, 전환과 같은 사용자 행동을 분석할 수 있으며, 행동 분석을 위한 인구 통계 인사이트 및 군중 선택과 같은 시나리오도 지원합니다.

- **레이크하우스 분석**:

  - **레이크하우스 쿼리 가속화**: Doris는 효율적인 쿼리 엔진으로 레이크하우스 데이터 쿼리를 가속화합니다.
  
  - **연합 분석**: Doris는 여러 데이터 소스에 걸친 연합 쿼리를 지원하여 아키텍처를 단순화하고 데이터 사일로를 제거합니다.
  
  - **실시간 데이터 처리**: Doris는 실시간 데이터 스트림과 배치 데이터 처리 기능을 결합하여 높은 동시성과 낮은 지연 시간의 복잡한 비즈니스 요구 사항을 충족합니다.

- **SQL 기반 관찰 가능성**:

  - **로그 및 이벤트 분석**: Doris는 분산 시스템의 로그 및 이벤트에 대한 실시간 또는 배치 분석을 가능하게 하여 문제를 식별하고 성능을 최적화하는 데 도움이 됩니다.


## 전체 아키텍처

Apache Doris는 MySQL 프로토콜을 사용하며, MySQL 구문과 높은 호환성을 가지며 표준 SQL을 지원합니다. 사용자는 다양한 클라이언트 도구를 통해 Apache Doris에 액세스할 수 있으며, BI 도구와 원활하게 통합할 수 있습니다.

### 스토리지-컴퓨팅 통합 아키텍처

Apache Doris의 스토리지-컴퓨팅 통합 아키텍처는 간소화되어 있고 유지 관리가 쉽습니다. 아래 그림과 같이 두 가지 유형의 프로세스로만 구성됩니다:

- **Frontend (FE):** 주로 사용자 요청 처리, 쿼리 구문 분석 및 계획, 메타데이터 관리, 노드 관리 작업을 담당합니다.

- **Backend (BE):** 주로 데이터 스토리지 및 쿼리 실행을 담당합니다. 데이터는 샤드로 분할되어 BE 노드 간에 여러 복제본으로 저장됩니다.

![Apache Doris의 전체 아키텍처](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

프로덕션 환경에서는 재해 복구를 위해 여러 FE 노드를 배포할 수 있습니다. 각 FE 노드는 메타데이터의 전체 복사본을 유지합니다. FE 노드는 세 가지 역할로 나뉩니다:

| 역할      | 기능                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | FE Master 노드는 메타데이터 읽기 및 쓰기 작업을 담당합니다. Master 노드에서 메타데이터 변경이 발생하면 BDB JE 프로토콜을 통해 Follower 또는 Observer 노드에 동기화됩니다. |
| Follower  | Follower 노드는 메타데이터 읽기를 담당합니다. Master 노드가 실패하면 Follower 노드를 새로운 Master로 선택할 수 있습니다. |
| Observer  | Observer 노드는 메타데이터 읽기를 담당하며 주로 쿼리 동시성을 높이는 데 사용됩니다. 클러스터 리더십 선출에 참여하지 않습니다. |

FE와 BE 프로세스는 모두 수평 확장 가능하여 단일 클러스터가 수백 대의 머신과 수십 페타바이트의 스토리지 용량을 지원할 수 있습니다. FE와 BE 프로세스는 일관성 프로토콜을 사용하여 서비스의 고가용성과 데이터의 높은 신뢰성을 보장합니다. 스토리지-컴퓨팅 통합 아키텍처는 높은 통합도를 가지며 분산 시스템의 운영 복잡성을 크게 줄입니다.


## Apache Doris의 핵심 기능

- **고가용성**: Apache Doris에서 메타데이터와 데이터는 모두 여러 복제본으로 저장되며, quorum 프로토콜을 통해 데이터 로그를 동기화합니다. 대다수 복제본이 쓰기를 완료하면 데이터 쓰기가 성공한 것으로 간주되어 몇 개의 노드가 실패하더라도 클러스터가 계속 사용 가능한 상태를 유지합니다. Apache Doris는 동일 도시 및 지역 간 재해 복구를 지원하여 이중 클러스터 마스터-슬레이브 모드를 가능하게 합니다. 일부 노드에 장애가 발생하면 클러스터가 자동으로 장애 노드를 격리하여 전체 클러스터 가용성이 영향을 받지 않도록 합니다.

- **높은 호환성**: Apache Doris는 MySQL 프로토콜과 높은 호환성을 가지며 표준 SQL 구문을 지원하여 대부분의 MySQL 및 Hive 함수를 포함합니다. 이 높은 호환성으로 인해 사용자는 기존 애플리케이션과 도구를 원활하게 마이그레이션하고 통합할 수 있습니다. Apache Doris는 MySQL 생태계를 지원하여 사용자가 MySQL 클라이언트 도구를 사용하여 Doris에 연결하여 더 편리한 운영 및 유지 관리를 할 수 있습니다. 또한 BI 보고 도구 및 데이터 전송 도구에 대한 MySQL 프로토콜 호환성을 지원하여 데이터 분석 및 데이터 전송 프로세스의 효율성과 안정성을 보장합니다.

- **실시간 데이터 웨어하우스**: Apache Doris를 기반으로 실시간 데이터 웨어하우스 서비스를 구축할 수 있습니다. Apache Doris는 초 단위 데이터 수집 기능을 제공하여 몇 초 내에 업스트림 온라인 트랜잭션 데이터베이스의 증분 변경 사항을 Doris로 캡처합니다. 벡터화 엔진, MPP 아키텍처 및 Pipeline 실행 엔진을 활용하여 Doris는 서브초 수준의 데이터 쿼리 기능을 제공하여 고성능, 낮은 지연 시간의 실시간 데이터 웨어하우스 플랫폼을 구축합니다.

- **통합 레이크하우스**: Apache Doris는 데이터 레이크 또는 관계형 데이터베이스와 같은 외부 데이터 소스를 기반으로 통합 레이크하우스 아키텍처를 구축할 수 있습니다. Doris 통합 레이크하우스 솔루션은 데이터 레이크와 데이터 웨어하우스 간의 원활한 통합과 자유로운 데이터 흐름을 가능하게 하여 사용자가 데이터 웨어하우스 기능을 직접 활용하여 데이터 레이크의 데이터 분석 문제를 해결할 수 있도록 하면서 데이터 레이크 데이터 관리 기능을 완전히 활용하여 데이터 가치를 향상시킵니다.

- **유연한 모델링**: Apache Doris는 와이드 테이블 모델, 사전 집계 모델, 스타/스노우플레이크 스키마 등 다양한 모델링 접근 방식을 제공합니다. 데이터 가져오기 중에 데이터는 와이드 테이블로 평탄화되어 Flink 또는 Spark와 같은 컴퓨팅 엔진을 통해 Doris에 작성되거나 데이터를 직접 Doris로 가져와 뷰, 구체화된 뷰 또는 실시간 다중 테이블 조인을 통해 데이터 모델링 작업을 수행할 수 있습니다.

## 기술 개요

Doris는 효율적인 SQL 인터페이스를 제공하며 MySQL 프로토콜과 완전히 호환됩니다. 쿼리 엔진은 MPP(대규모 병렬 처리) 아키텍처를 기반으로 하며 복잡한 분석 쿼리를 효율적으로 실행하고 낮은 지연 시간의 실시간 쿼리를 달성할 수 있습니다. 데이터 인코딩 및 압축을 위한 컬럼형 스토리지 기술을 통해 쿼리 성능과 스토리지 압축 비율을 크게 최적화합니다.

### 인터페이스

Apache Doris는 MySQL 프로토콜을 채택하고 표준 SQL을 지원하며 MySQL 구문과 높은 호환성을 가집니다. 사용자는 다양한 클라이언트 도구를 통해 Apache Doris에 액세스할 수 있으며 Smartbi, DataEase, FineBI, Tableau, Power BI, Apache Superset 등을 포함하되 이에 국한되지 않는 BI 도구와 원활하게 통합할 수 있습니다. Apache Doris는 MySQL 프로토콜을 지원하는 모든 BI 도구의 데이터 소스로 작동할 수 있습니다.

### 스토리지 엔진

Apache Doris는 컬럼형 스토리지 엔진을 가지고 있으며, 열별로 데이터를 인코딩, 압축 및 읽습니다. 이를 통해 매우 높은 데이터 압축 비율을 달성하고 불필요한 데이터 스캔을 크게 줄여 IO 및 CPU 리소스를 더 효율적으로 사용할 수 있습니다.

Apache Doris는 데이터 스캔을 최소화하기 위해 다양한 인덱스 구조를 지원합니다:

- **정렬 복합 키 인덱스**: 사용자는 최대 3개의 열을 지정하여 복합 정렬 키를 형성할 수 있습니다. 이를 통해 데이터를 효과적으로 가지치기하여 높은 동시성 보고 시나리오를 더 잘 지원할 수 있습니다.

- **Min/Max 인덱스**: 이를 통해 숫자 유형의 등가 및 범위 쿼리에서 효과적인 데이터 필터링이 가능합니다.

- **BloomFilter 인덱스**: 이는 높은 카디널리티 열의 등가 필터링 및 가지치기에 매우 효과적입니다.

- **역 인덱스**: 이를 통해 모든 필드에 대한 빠른 검색이 가능합니다.

Apache Doris는 다양한 데이터 모델을 지원하며 다양한 시나리오에 대해 최적화되었습니다:

- **상세 모델(Duplicate Key Model):** 사실 테이블의 상세 스토리지 요구 사항을 충족하도록 설계된 상세 데이터 모델.

- **기본 키 모델(Unique Key Model):** 고유 키를 보장합니다. 동일한 키를 가진 데이터는 덮어쓰여 행 수준 데이터 업데이트가 가능합니다.

- **집계 모델(Aggregate Key Model):** 동일한 키를 가진 값 열을 병합하여 사전 집계를 통해 성능을 크게 향상시킵니다.

Apache Doris는 강력한 일관성을 가진 단일 테이블 구체화된 뷰와 비동기적으로 새로 고쳐지는 다중 테이블 구체화된 뷰도 지원합니다. 단일 테이블 구체화된 뷰는 시스템에 의해 자동으로 새로 고쳐지고 유지 관리되며 사용자의 수동 개입이 필요하지 않습니다. 다중 테이블 구체화된 뷰는 클러스터 내 스케줄링 또는 외부 스케줄링 도구를 사용하여 주기적으로 새로 고칠 수 있어 데이터 모델링의 복잡성을 줄입니다.

### 🔍 쿼리 엔진

Apache Doris는 노드 간 및 노드 내 병렬 실행을 위한 MPP 기반 쿼리 엔진을 가지고 있습니다. 대형 테이블의 분산 셔플 조인을 지원하여 복잡한 쿼리를 더 잘 처리합니다.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Apache Doris의 쿼리 엔진은 완전히 벡터화되어 있으며 모든 메모리 구조가 컬럼형 형식으로 배치됩니다. 이를 통해 가상 함수 호출을 크게 줄이고 캐시 적중률을 높이며 SIMD 명령을 효율적으로 사용할 수 있습니다. Apache Doris는 와이드 테이블 집계 시나리오에서 비벡터화 엔진보다 5~10배 높은 성능을 제공합니다.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris는 적응형 쿼리 실행 기술을 사용하여 런타임 통계를 기반으로 실행 계획을 동적으로 조정합니다. 예를 들어 런타임 필터를 생성하여 프로브 측으로 푸시할 수 있습니다. 구체적으로 필터를 프로브 측의 최하위 스캔 노드로 푸시하여 처리할 데이터 양을 크게 줄이고 조인 성능을 향상시킵니다. Apache Doris의 런타임 필터는 In/Min/Max/Bloom Filter를 지원합니다.

Apache Doris는 Pipeline 실행 엔진을 사용하여 쿼리를 여러 하위 작업으로 분해하여 병렬 실행하며 멀티코어 CPU 기능을 완전히 활용합니다. 동시에 쿼리 스레드 수를 제한하여 스레드 폭발 문제를 해결합니다. Pipeline 실행 엔진은 데이터 복사 및 공유를 줄이고 정렬 및 집계 작업을 최적화하여 쿼리 효율성과 처리량을 크게 향상시킵니다.

최적화 측면에서 Apache Doris는 CBO(비용 기반 최적화 프로그램), RBO(규칙 기반 최적화 프로그램) 및 HBO(역사 기반 최적화 프로그램)의 조합 최적화 전략을 채택합니다. RBO는 상수 폴딩, 하위 쿼리 재작성, 조건자 푸시다운 등을 지원합니다. CBO는 조인 재정렬 및 기타 최적화를 지원합니다. HBO는 과거 쿼리 정보를 기반으로 최적의 실행 계획을 권장합니다. 이러한 여러 최적화 조치는 Doris가 다양한 유형의 쿼리에서 고성능 쿼리 계획을 열거할 수 있도록 보장합니다.


## 🎆 Apache Doris를 선택하는 이유는 무엇입니까?

- 🎯 **사용하기 쉬움**: 두 개의 프로세스, 다른 종속성 없음; 온라인 클러스터 확장, 자동 복제본 복구; MySQL 프로토콜과 호환되며 표준 SQL을 사용합니다.

- 🚀 **고성능**: 컬럼형 스토리지 엔진, 최신 MPP 아키텍처, 벡터화 쿼리 엔진, 사전 집계 구체화된 뷰 및 데이터 인덱스를 사용하여 낮은 지연 시간 및 높은 처리량 쿼리에 대한 극도로 빠른 성능.

- 🖥️ **단일 통합**: 단일 시스템이 실시간 데이터 서비스, 대화형 데이터 분석 및 오프라인 데이터 처리 시나리오를 지원할 수 있습니다.

- ⚛️ **연합 쿼리**: Hive, Iceberg, Hudi와 같은 데이터 레이크 및 MySQL, Elasticsearch와 같은 데이터베이스의 연합 쿼리를 지원합니다.

- ⏩ **다양한 데이터 가져오기 방법**: HDFS/S3에서 배치 가져오기 및 MySQL Binlog/Kafka에서 스트림 가져오기를 지원합니다. HTTP 인터페이스를 통한 마이크로 배치 쓰기 및 JDBC의 Insert를 사용한 실시간 쓰기를 지원합니다.

- 🚙 **풍부한 생태계**: Spark는 Spark-Doris-Connector를 사용하여 Doris를 읽고 씁니다. Flink-Doris-Connector는 Flink CDC가 Doris에 정확히 한 번의 데이터 쓰기를 구현할 수 있도록 합니다. DBT Doris Adapter가 제공되어 DBT로 Doris의 데이터를 변환할 수 있습니다.

## 🙌 기여자

**Apache Doris는 Apache 인큐베이터에서 성공적으로 졸업하여 2022년 6월에 최상위 프로젝트가 되었습니다**.

Apache Doris에 대한 기여에 대해 🔗[커뮤니티 기여자](https://github.com/apache/doris/graphs/contributors)에게 깊이 감사드립니다.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 사용자

Apache Doris는 현재 중국과 전 세계에서 광범위한 사용자 기반을 보유하고 있으며, 오늘 현재 **Apache Doris는 전 세계 수천 개의 회사의 프로덕션 환경에서 사용되고 있습니다**. 시가총액 또는 평가액 기준으로 중국 상위 50개 인터넷 회사의 80% 이상이 오랫동안 Apache Doris를 사용해 왔으며, 여기에는 Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo, Ke Holdings가 포함됩니다. 또한 금융, 에너지, 제조업, 통신과 같은 일부 전통적인 산업에서도 광범위하게 사용되고 있습니다.

Apache Doris의 사용자: 🔗[사용자](https://doris.apache.org/users)

Apache Doris 웹사이트에 회사 로고 추가: 🔗[회사 추가](https://github.com/apache/doris/discussions/27683)
 
## 👣 시작하기

### 📚 문서

모든 문서   🔗[문서](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ 다운로드 

모든 릴리스 및 바이너리 버전 🔗[다운로드](https://doris.apache.org/download) 

### 🗄️ 컴파일

컴파일 방법 보기  🔗[컴파일](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 설치

설치 및 배포 방법 보기 🔗[설치 및 배포](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 구성 요소

### 📝 Doris Connector

Doris는 Connector를 통해 Spark/Flink가 Doris에 저장된 데이터를 읽는 것을 지원하며, Connector를 통해 Doris에 데이터를 쓰는 것도 지원합니다.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 커뮤니티 및 지원

### 📤 메일링 리스트 구독

메일링 리스트는 Apache 커뮤니티에서 가장 인정받는 커뮤니케이션 형태입니다. 🔗[메일링 리스트 구독](https://doris.apache.org/community/subscribe-mail-list) 방법을 참조하세요

### 🙋 문제 보고 또는 Pull Request 제출

질문이 있으시면 🔗[GitHub Issue](https://github.com/apache/doris/issues)를 제출하거나 🔗[GitHub Discussion](https://github.com/apache/doris/discussions)에 게시하고 🔗[Pull Request](https://github.com/apache/doris/pulls)를 제출하여 수정하세요

### 🍻 기여 방법

귀하의 제안, 의견(비판 포함), 의견 및 기여를 환영합니다. 🔗[기여 방법](https://doris.apache.org/community/how-to-contribute/) 및 🔗[코드 제출 가이드](https://doris.apache.org/community/how-to-contribute/pull-request/)를 참조하세요

### ⌨️ Doris 개선 제안 (DSIP)

🔗[Doris 개선 제안 (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals)은 **모든 주요 기능 업데이트 또는 개선에 대한 설계 문서 모음**으로 생각할 수 있습니다.

### 🔑 백엔드 C++ 코딩 규격
🔗 [백엔드 C++ 코딩 규격](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637)을 엄격히 준수해야 하며, 이는 더 나은 코드 품질을 달성하는 데 도움이 됩니다.

## 💬 문의하기

다음 메일링 리스트를 통해 문의하세요.

| 이름                                                                          | 범위                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | 개발 관련 토론 | [구독](mailto:dev-subscribe@doris.apache.org)   | [구독 취소](mailto:dev-unsubscribe@doris.apache.org)   | [아카이브](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 링크

* Apache Doris 공식 웹사이트 - [사이트](https://doris.apache.org)
* 개발자 메일링 리스트 - <dev@doris.apache.org>. <dev-subscribe@doris.apache.org>로 메일을 보내고 답장을 따라 메일링 리스트를 구독하세요.
* Slack 채널 - [Slack 참가](https://doris.apache.org/slack)
* Twitter - [@doris_apache 팔로우](https://twitter.com/doris_apache)


## 📜 라이선스

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **참고**
> 일부 타사 종속성의 라이선스는 Apache 2.0 라이선스와 호환되지 않습니다. 따라서 Apache 2.0 라이선스를 준수하려면 일부 Doris 기능을 비활성화해야 합니다. 자세한 내용은 `thirdparty/LICENSE.txt`를 참조하세요.



