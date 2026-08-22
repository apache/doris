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

## 🌍 इसे अन्य भाषाओं में पढ़ें

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris एक ओपन-सोर्स, रियल-टाइम विश्लेषण और खोज डेटाबेस है, जिसे MPP आर्किटेक्चर पर बनाया गया है। यह तेज SQL विश्लेषण, लेकहाउस क्वेरी एक्सेलरेशन, और संरचित, टेक्स्ट तथा वेक्टर डेटा में हाइब्रिड खोज उपलब्ध कराता है।

नवीनतम उत्पाद अवलोकन, उपयोग के मामले, इकोसिस्टम अपडेट, ब्लॉग और उपयोगकर्ता कहानियों के लिए [आधिकारिक वेबसाइट](https://doris.apache.org/) देखें। वर्जन अपडेट के लिए [सभी रिलीज़ नोट](https://doris.apache.org/releases/all-release) देखें।

## 📈 उपयोग के मामले

| उपयोग का मामला | यह क्या देता है |
| -------- | ---------------- |
| [ग्राहक-केंद्रित विश्लेषण](https://doris.apache.org/use-cases/customer-facing-analytics) | बाहरी उपयोगकर्ताओं तक sub-second इंटरैक्टिव विश्लेषण पहुंचाएं। |
| [डेटा वेयरहाउसिंग](https://doris.apache.org/use-cases/data-warehousing) | व्यावसायिक डोमेन में फैला हुआ एक रियल-टाइम वेयरहाउस बनाएं। |
| [ऑब्जर्वेबिलिटी](https://doris.apache.org/use-cases/observability) | SQL के साथ high-throughput लॉग, इवेंट और मेट्रिक्स का विश्लेषण करें। |
| [AI के लिए Doris](https://doris.apache.org/use-cases/ai) | एक SQL इंजन में वेक्टर, टेक्स्ट, JSON और संरचित खोज का उपयोग करें। |

## 🚀 मुख्य क्षमताएं

Apache Doris तीन मुख्य क्षमताओं के इर्द-गिर्द बना है। विस्तृत उत्पाद विवरण और उदाहरणों के लिए वेबसाइट ही प्रामाणिक स्रोत है।

| क्षमता | यह क्या देती है |
| ---------- | ---------------- |
| [रियल-टाइम विश्लेषण](https://doris.apache.org/#real-time-analytics) | high concurrency में streaming ingestion, incremental transformation और sub-second queries। |
| [लेकहाउस विश्लेषण](https://doris.apache.org/#lakehouse-analytics) | Iceberg, Delta Lake और Hudi जैसे खुले टेबल फॉर्मैट पर तेज SQL विश्लेषण। |
| [हाइब्रिड खोज](https://doris.apache.org/#hybrid-search) | AI और खोज वर्कलोड के लिए JSON, full-text और वेक्टर डेटा पर SQL-native विश्लेषण। |

## 🔌 इकोसिस्टम

Doris आधुनिक डेटा स्टैक के केंद्र में काम करता है। यह अपस्ट्रीम डेटाबेस, स्ट्रीमिंग सिस्टम और लेकहाउस स्टोरेज को डाउनस्ट्रीम BI, AI, विश्लेषण और ऑब्जर्वेबिलिटी टूल से जोड़ता है।

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

नवीनतम इकोसिस्टम कवरेज के लिए [आधिकारिक वेबसाइट](https://doris.apache.org/) और [कनेक्शन तथा इंटीग्रेशन दस्तावेज़](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro) देखें।

## 👣 शुरू करें

- [Apache Doris क्या है](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [त्वरित शुरुआत](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [डाउनलोड](https://doris.apache.org/download)
- [इंस्टॉलेशन](https://doris.apache.org/docs/dev/install/intro)
- [स्रोत से compile करें](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 आर्किटेक्चर

Apache Doris compute-storage coupled और compute-storage decoupled, दोनों तरह के deployment का समर्थन करता है। decoupled mode में stateless compute groups साझा object storage पर चलते हैं, इसलिए आप compute को मांग के अनुसार scale कर सकते हैं और workloads को अलग रख सकते हैं।

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

[परिनियोजन मार्गदर्शिका](https://doris.apache.org/docs/dev/install/intro) और [परिनियोजन मोड मार्गदर्शिका](https://doris.apache.org/docs/dev/install/choosing-deployment-mode) में और जानें।

## 📣 प्रोजेक्ट अपडेट

| संसाधन | यह क्या देता है |
| -------- | ---------------- |
| [सामुदायिक रिपोर्ट](https://doris.apache.org/community-report/) | सामुदायिक गतिविधि, merge किए गए PR, योगदानकर्ताओं और फीचर प्रगति पर साप्ताहिक अपडेट। |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | AI और हाइब्रिड खोज, क्वेरी इंजन, स्टोरेज तथा डेटा लेक कार्य के लिए 2026 की योजना पर चर्चा। |

## 🧩 घटक

Doris सामान्य डेटा इंजीनियरिंग वर्कफ़्लो के लिए कनेक्टर और टूल प्रदान करता है।

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 उपयोगकर्ता

Apache Doris का उपयोग दुनिया भर की हजारों कंपनियां इंटरनेट सेवाओं, वित्त, रिटेल, लॉजिस्टिक्स, मैन्युफैक्चरिंग, ऊर्जा, दूरसंचार, AI और अन्य उद्योगों में प्रोडक्शन में करती हैं।

- [Apache Doris उपयोगकर्ता](https://doris.apache.org/why-doris/users/)
- [अपनी कंपनी जोड़ें](https://github.com/apache/doris/discussions/27683)

## 🙌 योगदानकर्ता

Apache Doris Apache Incubator से आगे बढ़कर जून 2022 में Apache Top-Level Project बना। Doris को बनाने में मदद करने वाले सभी [सामुदायिक योगदानकर्ताओं](https://github.com/apache/doris/graphs/contributors) का धन्यवाद।

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 समुदाय और सहायता

- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [पुल रिक्वेस्ट](https://github.com/apache/doris/pulls)
- [योगदान कैसे करें](https://doris.apache.org/community/how-to-contribute/)
- [कोड सबमिशन गाइड](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 हमसे संपर्क करें

- [Slack से जुड़ें](https://doris.apache.org/slack)
- [dev मेलिंग सूची की सदस्यता लें](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 लिंक

- Apache Doris की आधिकारिक वेबसाइट: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 लाइसेंस

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **नोट**
> तृतीय-पक्ष निर्भरताओं के कुछ लाइसेंस Apache 2.0 License के साथ संगत नहीं हैं। इसलिए Apache 2.0 License का पालन करने के लिए आपको
Doris की कुछ सुविधाएं बंद करनी होंगी। विवरण के लिए `thirdparty/LICENSE.txt` देखें
