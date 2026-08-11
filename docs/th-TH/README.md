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

## 🌍 อ่านในภาษาอื่น

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris เป็นฐานข้อมูลวิเคราะห์และค้นหาแบบเรียลไทม์ชนิดโอเพนซอร์สที่สร้างบนสถาปัตยกรรม MPP รองรับการวิเคราะห์ SQL ที่รวดเร็ว การเร่งความเร็วการสืบค้นใน lakehouse และการค้นหาแบบไฮบริดบนข้อมูลแบบมีโครงสร้าง ข้อความ และเวกเตอร์

ดูภาพรวมผลิตภัณฑ์ล่าสุด กรณีการใช้งาน อัปเดตระบบนิเวศ บล็อก และเรื่องราวจากผู้ใช้ได้ที่ [เว็บไซต์ทางการ](https://doris.apache.org/) สำหรับอัปเดตเวอร์ชัน โปรดดู [บันทึกประจำรุ่นทั้งหมด](https://doris.apache.org/releases/all-release)

## 📈 กรณีการใช้งาน

| กรณีการใช้งาน | สิ่งที่ได้รับ |
| -------- | ---------------- |
| [การวิเคราะห์สำหรับผู้ใช้ปลายทาง](https://doris.apache.org/use-cases/customer-facing-analytics) | ส่งมอบการวิเคราะห์แบบโต้ตอบระดับต่ำกว่าวินาทีให้ผู้ใช้ภายนอก |
| [คลังข้อมูล](https://doris.apache.org/use-cases/data-warehousing) | สร้างคลังข้อมูลเรียลไทม์หนึ่งเดียวสำหรับหลายโดเมนธุรกิจ |
| [การสังเกตการณ์ระบบ](https://doris.apache.org/use-cases/observability) | วิเคราะห์ล็อก เหตุการณ์ และเมตริกปริมาณสูงด้วย SQL |
| [Doris สำหรับ AI](https://doris.apache.org/use-cases/ai) | ใช้การค้นหาเวกเตอร์ ข้อความ JSON และข้อมูลแบบมีโครงสร้างในเอนจิน SQL เดียว |

## 🚀 ความสามารถหลัก

Apache Doris สร้างขึ้นรอบความสามารถหลักสามด้าน เว็บไซต์คือแหล่งข้อมูลหลักสำหรับคำอธิบายผลิตภัณฑ์และตัวอย่างโดยละเอียด

| ความสามารถ | สิ่งที่ได้รับ |
| ---------- | ---------------- |
| [การวิเคราะห์แบบเรียลไทม์](https://doris.apache.org/#real-time-analytics) | การนำเข้าข้อมูลแบบสตรีม การแปลงข้อมูลแบบเพิ่มทีละส่วน และการสืบค้นระดับต่ำกว่าวินาทีภายใต้ภาวะพร้อมกันสูง |
| [การวิเคราะห์ Lakehouse](https://doris.apache.org/#lakehouse-analytics) | การวิเคราะห์ SQL ที่รวดเร็วบนรูปแบบตารางเปิด เช่น Iceberg, Delta Lake และ Hudi |
| [การค้นหาแบบไฮบริด](https://doris.apache.org/#hybrid-search) | การวิเคราะห์แบบเนทีฟบน SQL บนข้อมูล JSON ข้อความเต็ม และเวกเตอร์สำหรับงาน AI และงานค้นหา |

## 🔌 ระบบนิเวศ

Doris อยู่ที่ศูนย์กลางของสแต็กข้อมูลสมัยใหม่ โดยเชื่อมต่อฐานข้อมูลต้นทาง ระบบสตรีมมิง และพื้นที่จัดเก็บแบบ lakehouse เข้ากับเครื่องมือปลายทางสำหรับ BI, AI, การวิเคราะห์ และการสังเกตการณ์ระบบ

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

สำหรับข้อมูลระบบนิเวศล่าสุด โปรดไปที่ [เว็บไซต์ทางการ](https://doris.apache.org/) และ [เอกสารการเชื่อมต่อและการผสานรวม](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro)

## 👣 เริ่มต้นใช้งาน

- [Apache Doris คืออะไร](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [เริ่มต้นอย่างรวดเร็ว](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [ดาวน์โหลด](https://doris.apache.org/download)
- [ติดตั้ง](https://doris.apache.org/docs/dev/install/intro)
- [คอมไพล์จากซอร์ส](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 สถาปัตยกรรม

Apache Doris รองรับทั้งการปรับใช้แบบ compute-storage coupled และ compute-storage decoupled ในโหมด decoupled กลุ่มประมวลผลแบบ stateless จะทำงานบน object storage ที่ใช้ร่วมกัน คุณจึงขยายขนาดการประมวลผลตามต้องการและแยก workload ออกจากกันได้

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

ดูเพิ่มเติมใน [คู่มือการปรับใช้](https://doris.apache.org/docs/dev/install/intro) และ [คู่มือโหมดการปรับใช้](https://doris.apache.org/docs/dev/install/choosing-deployment-mode)

## 📣 อัปเดตโครงการ

| ทรัพยากร | สิ่งที่ได้รับ |
| -------- | ---------------- |
| [รายงานชุมชน](https://doris.apache.org/community-report/) | อัปเดตรายสัปดาห์เกี่ยวกับกิจกรรมชุมชน PR ที่ merge แล้ว ผู้ร่วมพัฒนา และความคืบหน้าของฟีเจอร์ |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | การอภิปรายแผนปี 2026 สำหรับ AI และการค้นหาแบบไฮบริด เอนจินสืบค้น พื้นที่จัดเก็บ และงาน data lake |

## 🧩 ส่วนประกอบ

Doris มีตัวเชื่อมต่อและเครื่องมือสำหรับเวิร์กโฟลว์วิศวกรรมข้อมูลที่ใช้กันทั่วไป

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 ผู้ใช้

บริษัทหลายพันแห่งทั่วโลกใช้ Apache Doris ในระบบจริง ครอบคลุมบริการอินเทอร์เน็ต การเงิน ค้าปลีก โลจิสติกส์ การผลิต พลังงาน โทรคมนาคม AI และอุตสาหกรรมอื่น ๆ

- [ผู้ใช้ Apache Doris](https://doris.apache.org/why-doris/users/)
- [เพิ่มบริษัทของคุณ](https://github.com/apache/doris/discussions/27683)

## 🙌 ผู้ร่วมพัฒนา

Apache Doris สำเร็จการบ่มเพาะจาก Apache Incubator และกลายเป็น Apache Top-Level Project ในเดือนมิถุนายน 2022 ขอขอบคุณ [ผู้ร่วมพัฒนาชุมชน](https://github.com/apache/doris/graphs/contributors) ทุกคนที่ช่วยกันสร้าง Doris

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 ชุมชนและการสนับสนุน

- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [วิธีร่วมพัฒนา](https://doris.apache.org/community/how-to-contribute/)
- [คู่มือการส่งโค้ด](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 ติดต่อเรา

- [เข้าร่วม Slack](https://doris.apache.org/slack)
- [สมัครรับรายชื่ออีเมล dev](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 ลิงก์

- เว็บไซต์ทางการของ Apache Doris: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 สัญญาอนุญาต

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **หมายเหตุ**
> สัญญาอนุญาตบางรายการของ dependencies บุคคลที่สามไม่เข้ากันกับ Apache 2.0 License ดังนั้นคุณต้องปิดใช้
ฟีเจอร์บางอย่างของ Doris เพื่อให้เป็นไปตาม Apache 2.0 License ดูรายละเอียดได้ที่ `thirdparty/LICENSE.txt`
