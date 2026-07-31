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

## 🌍 Читать на других языках

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris — это база данных с открытым исходным кодом для аналитики и поиска в реальном времени, построенная на архитектуре MPP. Она обеспечивает быструю SQL-аналитику, ускорение запросов к lakehouse и гибридный поиск по структурированным, текстовым и векторным данным.

На [официальном сайте](https://doris.apache.org/) вы найдете актуальный обзор продукта, сценарии использования, новости экосистемы, блоги и истории пользователей. Информацию об обновлениях версий см. в разделе [все заметки о выпусках](https://doris.apache.org/releases/all-release).

## 📈 Сценарии использования

| Сценарий использования | Что он дает |
| -------- | ---------------- |
| [Клиентская аналитика](https://doris.apache.org/use-cases/customer-facing-analytics) | Предоставляйте внешним пользователям интерактивную аналитику с задержкой менее секунды. |
| [Хранилище данных](https://doris.apache.org/use-cases/data-warehousing) | Создавайте единое хранилище реального времени для разных бизнес-доменов. |
| [Наблюдаемость](https://doris.apache.org/use-cases/observability) | Анализируйте высоконагруженные логи, события и метрики с помощью SQL. |
| [Doris для AI](https://doris.apache.org/use-cases/ai) | Используйте в одном SQL-движке векторный, текстовый, JSON и структурированный поиск. |

## 🚀 Ключевые возможности

Apache Doris построен вокруг трех ключевых возможностей. Официальный сайт является источником достоверной информации с подробными описаниями продукта и примерами.

| Возможность | Что она дает |
| ---------- | ---------------- |
| [Аналитика в реальном времени](https://doris.apache.org/#real-time-analytics) | Потоковая загрузка, инкрементальные преобразования и запросы с задержкой менее секунды при высокой конкурентной нагрузке. |
| [Lakehouse-аналитика](https://doris.apache.org/#lakehouse-analytics) | Быстрая SQL-аналитика по открытым табличным форматам, таким как Iceberg, Delta Lake и Hudi. |
| [Гибридный поиск](https://doris.apache.org/#hybrid-search) | Нативная для SQL аналитика по JSON, полнотекстовым и векторным данным для AI- и поисковых нагрузок. |

## 🔌 Экосистема

Doris находится в центре современного стека данных. Она связывает исходные базы данных, потоковые системы и lakehouse-хранилища с downstream-инструментами для BI, AI, аналитики и наблюдаемости.

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

Актуальную информацию об экосистеме см. на [официальном сайте](https://doris.apache.org/) и в [документации по подключениям и интеграциям](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro).

## 👣 Начало работы

- [Что такое Apache Doris](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [Быстрый старт](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [Загрузка](https://doris.apache.org/download)
- [Установка](https://doris.apache.org/docs/dev/install/intro)
- [Скомпилировать из исходников](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 Архитектура

Apache Doris поддерживает развертывания как с тесно связанными вычислениями и хранением, так и с их разделением. В разделенном режиме вычислительные группы без состояния работают поверх общего объектного хранилища, поэтому вы можете масштабировать вычисления по требованию и изолировать рабочие нагрузки.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

Подробнее см. в [руководстве по развертыванию](https://doris.apache.org/docs/dev/install/intro) и [руководстве по выбору режима развертывания](https://doris.apache.org/docs/dev/install/choosing-deployment-mode).

## 📣 Обновления проекта

| Ресурс | Что он дает |
| -------- | ---------------- |
| [Отчет сообщества](https://doris.apache.org/community-report/) | Еженедельные обновления об активности сообщества, объединенных PR, участниках и ходе разработки функций. |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | Обсуждение планов на 2026 год по AI и гибридному поиску, движку запросов, хранению и работе с data lake. |

## 🧩 Компоненты

Doris предоставляет коннекторы и инструменты для распространенных процессов data engineering.

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 Пользователи

Apache Doris используется в production тысячами компаний по всему миру в интернет-сервисах, финансах, розничной торговле, логистике, производстве, энергетике, телекоммуникациях, AI и других отраслях.

- [Пользователи Apache Doris](https://doris.apache.org/why-doris/users/)
- [Добавьте свою компанию](https://github.com/apache/doris/discussions/27683)

## 🙌 Участники

Apache Doris вышел из Apache Incubator и стал Apache Top-Level Project в июне 2022 года. Спасибо всем [участникам сообщества](https://github.com/apache/doris/graphs/contributors), которые помогают развивать Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 Сообщество и поддержка

- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [Как внести свой вклад](https://doris.apache.org/community/how-to-contribute/)
- [Руководство по отправке кода](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 Связаться с нами

- [Присоединяйтесь к Slack](https://doris.apache.org/slack)
- [Подпишитесь на список рассылки для разработчиков](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 Ссылки

- Официальный сайт Apache Doris: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 Лицензия

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Примечание**
> Некоторые лицензии сторонних зависимостей несовместимы с Apache 2.0 License. Поэтому вам нужно отключить
некоторые функции Doris, чтобы соответствовать Apache 2.0 License. Подробности см. в `thirdparty/LICENSE.txt`.
