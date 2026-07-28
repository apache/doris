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

## 🌍 অন্যান্য ভাষায় পড়ুন

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris হলো MPP আর্কিটেকচারের ওপর নির্মিত একটি ওপেন সোর্স, রিয়েল-টাইম অ্যানালিটিক্স ও সার্চ ডাটাবেস। এটি দ্রুত SQL অ্যানালিটিক্স, লেকহাউস ক্যোয়ারি অ্যাক্সেলারেশন এবং স্ট্রাকচার্ড, টেক্সট ও ভেক্টর ডেটার ওপর হাইব্রিড সার্চ সুবিধা দেয়।

সর্বশেষ পণ্য পরিচিতি, ব্যবহারের ক্ষেত্র, ইকোসিস্টেম আপডেট, ব্লগ এবং ব্যবহারকারীদের অভিজ্ঞতা জানতে [অফিসিয়াল ওয়েবসাইট](https://doris.apache.org/) দেখুন। সংস্করণ আপডেটের জন্য [সব রিলিজ নোট](https://doris.apache.org/releases/all-release) দেখুন।

## 📈 ব্যবহারের ক্ষেত্র

| ব্যবহারের ক্ষেত্র | যা দেয় |
| -------- | ---------------- |
| [গ্রাহক-মুখী অ্যানালিটিক্স](https://doris.apache.org/use-cases/customer-facing-analytics) | বাইরের ব্যবহারকারীদের জন্য সাব-সেকেন্ড ইন্টারঅ্যাকটিভ অ্যানালিটিক্স সরবরাহ করুন। |
| [ডেটা ওয়্যারহাউসিং](https://doris.apache.org/use-cases/data-warehousing) | বিভিন্ন ব্যবসায়িক ডোমেইনজুড়ে একক রিয়েল-টাইম ওয়্যারহাউস তৈরি করুন। |
| [অবজারভেবিলিটি](https://doris.apache.org/use-cases/observability) | SQL দিয়ে উচ্চ-থ্রুপুট লগ, ইভেন্ট ও মেট্রিক বিশ্লেষণ করুন। |
| [AI-এর জন্য Doris](https://doris.apache.org/use-cases/ai) | এক SQL ইঞ্জিনেই ভেক্টর, টেক্সট, JSON ও স্ট্রাকচার্ড সার্চ ব্যবহার করুন। |

## 🚀 মূল সক্ষমতা

Apache Doris তিনটি মূল সক্ষমতার ওপর ভিত্তি করে তৈরি। বিস্তারিত পণ্য বিবরণ ও উদাহরণের জন্য ওয়েবসাইটই নির্ভরযোগ্য উৎস।

| সক্ষমতা | যা দেয় |
| ---------- | ---------------- |
| [রিয়েল-টাইম অ্যানালিটিক্স](https://doris.apache.org/#real-time-analytics) | বেশি কনকারেন্সির মধ্যেও স্ট্রিমিং ইনজেশন, ইনক্রিমেন্টাল ট্রান্সফরমেশন এবং সাব-সেকেন্ড ক্যোয়ারি। |
| [লেকহাউস অ্যানালিটিক্স](https://doris.apache.org/#lakehouse-analytics) | Iceberg, Delta Lake ও Hudi-এর মতো ওপেন টেবিল ফরম্যাটের ওপর দ্রুত SQL অ্যানালিটিক্স। |
| [হাইব্রিড সার্চ](https://doris.apache.org/#hybrid-search) | AI ও সার্চ ওয়ার্কলোডের জন্য JSON, ফুল-টেক্সট ও ভেক্টর ডেটাজুড়ে SQL-native অ্যানালিটিক্স। |

## 🔌 ইকোসিস্টেম

Doris আধুনিক ডেটা স্ট্যাকের কেন্দ্রে কাজ করে। এটি আপস্ট্রিম ডাটাবেস, স্ট্রিমিং সিস্টেম ও লেকহাউস স্টোরেজকে ডাউনস্ট্রিম BI, AI, অ্যানালিটিক্স এবং অবজারভেবিলিটি টুলের সঙ্গে যুক্ত করে।

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

সর্বশেষ ইকোসিস্টেম তথ্যের জন্য [অফিসিয়াল ওয়েবসাইট](https://doris.apache.org/) এবং [কানেকশন ও ইন্টিগ্রেশন ডকুমেন্টেশন](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro) দেখুন।

## 👣 শুরু করুন

- [Apache Doris কী](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [দ্রুত শুরু](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [ডাউনলোড](https://doris.apache.org/download)
- [ইনস্টলেশন](https://doris.apache.org/docs/dev/install/intro)
- [সোর্স থেকে কম্পাইল](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 আর্কিটেকচার

Apache Doris কম্পিউট-স্টোরেজ কাপলড ও কম্পিউট-স্টোরেজ ডিকাপলড, দুই ধরনের ডিপ্লয়মেন্টই সমর্থন করে। ডিকাপলড মোডে স্টেটলেস কম্পিউট গ্রুপ শেয়ার্ড অবজেক্ট স্টোরেজের ওপর চলে, ফলে আপনি চাহিদা অনুযায়ী কম্পিউট স্কেল করতে এবং ওয়ার্কলোড আলাদা রাখতে পারেন।

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

[ডিপ্লয়মেন্ট গাইড](https://doris.apache.org/docs/dev/install/intro) এবং [ডিপ্লয়মেন্ট মোড গাইড](https://doris.apache.org/docs/dev/install/choosing-deployment-mode) থেকে আরও জানুন।

## 📣 প্রকল্পের আপডেট

| রিসোর্স | যা দেয় |
| -------- | ---------------- |
| [কমিউনিটি রিপোর্ট](https://doris.apache.org/community-report/) | কমিউনিটি কার্যক্রম, মার্জ হওয়া PR, অবদানকারী এবং ফিচার অগ্রগতি নিয়ে সাপ্তাহিক আপডেট। |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | AI ও হাইব্রিড সার্চ, ক্যোয়ারি ইঞ্জিন, স্টোরেজ এবং ডেটা লেক কাজের জন্য 2026 পরিকল্পনা আলোচনা। |

## 🧩 উপাদান

Doris প্রচলিত ডেটা ইঞ্জিনিয়ারিং ওয়ার্কফ্লোর জন্য কানেক্টর ও টুল দেয়।

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 ব্যবহারকারী

Apache Doris বিশ্বজুড়ে হাজারো কোম্পানির প্রোডাকশন পরিবেশে ব্যবহৃত হয়, যার মধ্যে ইন্টারনেট সার্ভিস, ফাইন্যান্স, রিটেইল, লজিস্টিকস, ম্যানুফ্যাকচারিং, এনার্জি, টেলিকমিউনিকেশন, AI এবং অন্যান্য শিল্প রয়েছে।

- [Apache Doris ব্যবহারকারীরা](https://doris.apache.org/why-doris/users/)
- [আপনার কোম্পানি যোগ করুন](https://github.com/apache/doris/discussions/27683)

## 🙌 অবদানকারী

Apache Doris Apache Incubator থেকে গ্র্যাজুয়েট হয়ে 2022 সালের জুনে Apache Top-Level Project হয়েছে। Doris তৈরিতে সহায়তা করা সব [কমিউনিটি অবদানকারীকে](https://github.com/apache/doris/graphs/contributors) ধন্যবাদ।

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 কমিউনিটি ও সহায়তা

- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [কীভাবে অবদান রাখবেন](https://doris.apache.org/community/how-to-contribute/)
- [কোড জমা দেওয়ার নির্দেশিকা](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 যোগাযোগ করুন

- [Slack-এ যোগ দিন](https://doris.apache.org/slack)
- [dev মেলিং লিস্টে সাবস্ক্রাইব করুন](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 লিঙ্ক

- Apache Doris অফিসিয়াল ওয়েবসাইট: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 লাইসেন্স

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **নোট**
> থার্ড-পার্টি ডিপেন্ডেন্সির কিছু লাইসেন্স Apache 2.0 License-এর সঙ্গে সামঞ্জস্যপূর্ণ নয়। তাই আপনাকে
Apache 2.0 License মেনে চলতে কিছু Doris ফিচার নিষ্ক্রিয় করতে হবে। বিস্তারিত জানতে `thirdparty/LICENSE.txt` দেখুন
