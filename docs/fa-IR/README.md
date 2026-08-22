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

## 🌍 این مطلب را به زبان‌های دیگر بخوانید

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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
    <a href="https://doris.apache.org/slack"><img src="https://img.shields.io/badge/-Slack-4A154B?style=social&logo=slack" height=25 alt="Slack"></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---

<div align="center">
  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>

Apache Doris یک پایگاه داده متن‌باز برای تحلیل و جستجوی بلادرنگ است که بر پایه معماری MPP ساخته شده است. این پایگاه داده تحلیل سریع با SQL، شتاب‌دهی به پرس‌وجوهای lakehouse و جستجوی ترکیبی روی داده‌های ساختاریافته، متنی و برداری را فراهم می‌کند.

برای آشنایی با تازه‌ترین نمای کلی محصول، موارد استفاده، به‌روزرسانی‌های اکوسیستم، وبلاگ‌ها و داستان‌های کاربران، [وب‌سایت رسمی](https://doris.apache.org/) را ببینید. برای به‌روزرسانی‌های نسخه، [همه یادداشت‌های انتشار](https://doris.apache.org/releases/all-release) را ببینید.

## 📈 موارد استفاده

| مورد استفاده | چه چیزی فراهم می‌کند |
| -------- | ---------------- |
| [تحلیل برای کاربران نهایی](https://doris.apache.org/use-cases/customer-facing-analytics) | تحلیل تعاملی با زمان پاسخ کمتر از یک ثانیه را در اختیار کاربران بیرونی قرار دهید. |
| [انبار داده](https://doris.apache.org/use-cases/data-warehousing) | یک انبار داده بلادرنگ برای حوزه‌های مختلف کسب‌وکار بسازید. |
| [مشاهده‌پذیری](https://doris.apache.org/use-cases/observability) | لاگ‌ها، رویدادها و معیارهای پرترافیک را با SQL تحلیل کنید. |
| [Doris برای AI](https://doris.apache.org/use-cases/ai) | جستجوی برداری، متنی، JSON و ساختاریافته را در یک موتور SQL به کار ببرید. |

## 🚀 توانمندی‌های اصلی

Apache Doris بر سه توانمندی اصلی بنا شده است. وب‌سایت، مرجع اصلی توضیحات و نمونه‌های دقیق محصول است.

| توانمندی | چه چیزی فراهم می‌کند |
| ---------- | ---------------- |
| [تحلیل بلادرنگ](https://doris.apache.org/#real-time-analytics) | ورود داده جریانی، تبدیل افزایشی و پرس‌وجوهای کمتر از یک ثانیه در هم‌زمانی بالا. |
| [تحلیل Lakehouse](https://doris.apache.org/#lakehouse-analytics) | تحلیل سریع SQL روی قالب‌های جدول باز مانند Iceberg، Delta Lake و Hudi. |
| [جستجوی ترکیبی](https://doris.apache.org/#hybrid-search) | تحلیل بومی SQL روی JSON، متن کامل و داده‌های برداری برای بارهای کاری AI و جستجو. |

## 🔌 اکوسیستم

Doris در مرکز پشته داده مدرن قرار می‌گیرد. این سیستم پایگاه‌های داده بالادستی، سامانه‌های جریانی و ذخیره‌سازی lakehouse را به ابزارهای پایین‌دستی BI، AI، تحلیل و مشاهده‌پذیری متصل می‌کند.

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

برای تازه‌ترین پوشش اکوسیستم، [وب‌سایت رسمی](https://doris.apache.org/) و [مستندات اتصال و یکپارچه‌سازی](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro) را ببینید.

## 👣 شروع کار

- [Apache Doris چیست](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [شروع سریع](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [دانلود](https://doris.apache.org/download)
- [نصب](https://doris.apache.org/docs/dev/install/intro)
- [کامپایل از سورس](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 معماری

Apache Doris هم از استقرار با محاسبه و ذخیره‌سازی یکپارچه و هم از استقرار با محاسبه و ذخیره‌سازی جدا پشتیبانی می‌کند. در حالت جدا، گروه‌های محاسباتی بدون حالت روی ذخیره‌سازی شیء مشترک اجرا می‌شوند، بنابراین می‌توانید توان محاسباتی را بر اساس نیاز مقیاس دهید و بارهای کاری را از هم جدا کنید.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

در [راهنمای استقرار](https://doris.apache.org/docs/dev/install/intro) و [راهنمای حالت استقرار](https://doris.apache.org/docs/dev/install/choosing-deployment-mode) بیشتر بخوانید.

## 📣 به‌روزرسانی‌های پروژه

| منبع | چه چیزی فراهم می‌کند |
| -------- | ---------------- |
| [گزارش جامعه](https://doris.apache.org/community-report/) | به‌روزرسانی‌های هفتگی درباره فعالیت جامعه، درخواست‌های Pull ادغام‌شده، مشارکت‌کنندگان و پیشرفت قابلیت‌ها. |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | گفت‌وگوی برنامه‌ریزی 2026 برای AI و جستجوی ترکیبی، موتور پرس‌وجو، ذخیره‌سازی و کارهای مربوط به دریاچه داده. |

## 🧩 اجزا

Doris برای گردش‌کارهای رایج مهندسی داده، اتصال‌دهنده‌ها و ابزارهایی فراهم می‌کند.

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 کاربران

Apache Doris در محیط‌های تولیدی هزاران شرکت در سراسر جهان و در حوزه‌هایی مانند خدمات اینترنتی، مالی، خرده‌فروشی، لجستیک، تولید، انرژی، مخابرات، AI و صنایع دیگر استفاده می‌شود.

- [کاربران Apache Doris](https://doris.apache.org/why-doris/users/)
- [شرکت خود را اضافه کنید](https://github.com/apache/doris/discussions/27683)

## 🙌 مشارکت‌کنندگان

Apache Doris از Apache Incubator فارغ‌التحصیل شد و در ژوئن 2022 به یک Apache Top-Level Project تبدیل شد. از همه [مشارکت‌کنندگان جامعه](https://github.com/apache/doris/graphs/contributors) که به ساخت Doris کمک می‌کنند سپاسگزاریم.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 جامعه و پشتیبانی

- [مسائل GitHub](https://github.com/apache/doris/issues)
- [گفت‌وگوهای GitHub](https://github.com/apache/doris/discussions)
- [درخواست‌های Pull](https://github.com/apache/doris/pulls)
- [روش مشارکت](https://doris.apache.org/community/how-to-contribute/)
- [راهنمای ارسال کد](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 تماس با ما

- [به Slack بپیوندید](https://doris.apache.org/slack)
- [عضویت در فهرست ایمیل توسعه‌دهندگان](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 پیوندها

- وب‌سایت رسمی Apache Doris: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 مجوز

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **نکته**
> برخی مجوزهای وابستگی‌های شخص ثالث با Apache 2.0 License سازگار نیستند. بنابراین برای رعایت Apache 2.0 License باید
برخی قابلیت‌های Doris را غیرفعال کنید. برای جزئیات، به `thirdparty/LICENSE.txt` مراجعه کنید
