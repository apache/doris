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

## 🌍 اقرأ هذا بلغات أخرى

[English](../../README.md) • [العربية](README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris قاعدة بيانات مفتوحة المصدر للتحليلات والبحث في الوقت الفعلي، مبنية على بنية MPP. يوفر تحليلات SQL سريعة، وتسريعًا لاستعلامات مستودع بحيرة البيانات، وبحثًا هجينًا عبر البيانات المهيكلة والنصية والمتجهية.

استكشف [الموقع الرسمي](https://doris.apache.org/) للاطلاع على أحدث نظرة عامة على المنتج وحالات الاستخدام وتحديثات المنظومة والمدونات وقصص المستخدمين. ولتحديثات الإصدارات، راجع [جميع ملاحظات الإصدار](https://doris.apache.org/releases/all-release).

## 📈 حالات الاستخدام

| حالة الاستخدام | ما توفره |
| -------- | ---------------- |
| [التحليلات الموجهة للعملاء](https://doris.apache.org/use-cases/customer-facing-analytics) | تقديم تحليلات تفاعلية بزمن استجابة أقل من ثانية للمستخدمين الخارجيين. |
| [مستودعات البيانات](https://doris.apache.org/use-cases/data-warehousing) | بناء مستودع واحد في الوقت الفعلي عبر مجالات الأعمال. |
| [المراقبة التشغيلية](https://doris.apache.org/use-cases/observability) | تحليل السجلات والأحداث والمقاييس عالية الإنتاجية باستخدام SQL. |
| [Doris من أجل AI](https://doris.apache.org/use-cases/ai) | استخدام البحث المتجهي والنصي وJSON والبحث المهيكل في محرك SQL واحد. |

## 🚀 القدرات الأساسية

يرتكز Apache Doris على ثلاث قدرات أساسية. الموقع الإلكتروني هو مصدر الحقيقة لأوصاف المنتج التفصيلية والأمثلة.

| القدرة | ما توفره |
| ---------- | ---------------- |
| [التحليلات في الوقت الفعلي](https://doris.apache.org/#real-time-analytics) | إدخال البيانات المتدفقة، والتحويل التزايدي، واستعلامات بزمن استجابة أقل من ثانية مع تزامن عال. |
| [تحليلات مستودع بحيرة البيانات](https://doris.apache.org/#lakehouse-analytics) | تحليلات SQL سريعة فوق تنسيقات الجداول المفتوحة مثل Iceberg وDelta Lake وHudi. |
| [البحث الهجين](https://doris.apache.org/#hybrid-search) | تحليلات أصلية في SQL عبر JSON والنص الكامل والبيانات المتجهية لأعباء عمل AI والبحث. |

## 🔌 النظام البيئي

يقع Doris في قلب مكدس البيانات الحديث. يربط قواعد البيانات وأنظمة البث وتخزين مستودع بحيرة البيانات في المنبع مع أدوات BI وAI والتحليلات والمراقبة التشغيلية في المصب.

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

للاطلاع على أحدث تغطية للمنظومة، تفضل بزيارة [الموقع الرسمي](https://doris.apache.org/) و[وثائق الاتصال والتكامل](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro).

## 👣 ابدأ

- [ما هو Apache Doris](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [البدء السريع](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [التنزيل](https://doris.apache.org/download)
- [التثبيت](https://doris.apache.org/docs/dev/install/intro)
- [الترجمة من المصدر](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 البنية

يدعم Apache Doris نمطي نشر: النشر المقترن بين الحوسبة والتخزين، والنشر المفصول بين الحوسبة والتخزين. في النمط المفصول، تعمل مجموعات حوسبة عديمة الحالة فوق تخزين كائنات مشترك، مما يتيح لك توسيع الحوسبة عند الطلب وعزل أعباء العمل.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

تعرّف على المزيد في [دليل النشر](https://doris.apache.org/docs/dev/install/intro) و[دليل اختيار نمط النشر](https://doris.apache.org/docs/dev/install/choosing-deployment-mode).

## 📣 تحديثات المشروع

| المورد | ما يوفره |
| -------- | ---------------- |
| [تقرير المجتمع](https://doris.apache.org/community-report/) | تحديثات أسبوعية حول نشاط المجتمع، وطلبات السحب المدمجة، والمساهمين، وتقدم الميزات. |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | نقاش التخطيط لعام 2026 لأعمال AI والبحث الهجين ومحرك الاستعلام والتخزين وبحيرة البيانات. |

## 🧩 المكونات

يوفر Doris موصلات وأدوات لسير عمل هندسة البيانات الشائعة.

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 المستخدمون

تستخدم آلاف الشركات حول العالم Apache Doris في بيئات الإنتاج ضمن خدمات الإنترنت والقطاع المالي والتجزئة والخدمات اللوجستية والتصنيع والطاقة والاتصالات وAI وغيرها من القطاعات.

- [مستخدمو Apache Doris](https://doris.apache.org/why-doris/users/)
- [أضف شركتك](https://github.com/apache/doris/discussions/27683)

## 🙌 المساهمون

تخرج Apache Doris من Apache Incubator وأصبح Apache Top-Level Project في يونيو 2022. شكرًا لجميع [مساهمي المجتمع](https://github.com/apache/doris/graphs/contributors) الذين يساعدون في بناء Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 المجتمع والدعم

- [مشكلات GitHub](https://github.com/apache/doris/issues)
- [مناقشات GitHub](https://github.com/apache/doris/discussions)
- [طلبات السحب](https://github.com/apache/doris/pulls)
- [كيفية المساهمة](https://doris.apache.org/community/how-to-contribute/)
- [دليل إرسال الكود](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 تواصل معنا

- [انضم إلى Slack](https://doris.apache.org/slack)
- [اشترك في القائمة البريدية للتطوير](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 الروابط

- الموقع الرسمي لـ Apache Doris: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 الترخيص

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **ملاحظة**
> بعض تراخيص تبعيات الطرف الثالث غير متوافقة مع Apache License, Version 2.0. لذلك تحتاج إلى تعطيل
بعض ميزات Doris للامتثال إلى Apache License, Version 2.0. للتفاصيل، راجع `thirdparty/LICENSE.txt`
