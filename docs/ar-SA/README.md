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

[العربية](README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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
    <a href="https://doris.apache.org/slack"><img src="https://img.shields.io/badge/-Slack-4A154B?style=social&logo=slack" height=25 alt="Slack"></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---

<p align="center">

  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

</p>




Apache Doris هو قاعدة بيانات تحليلية سهلة الاستخدام وعالية الأداء وفي الوقت الفعلي تعتمد على بنية MPP، معروفة بسرعتها الفائقة وسهولة استخدامها. يتطلب فقط وقت استجابة أقل من ثانية لإرجاع نتائج الاستعلام تحت البيانات الضخمة ويمكنه دعم ليس فقط سيناريوهات استعلام النقطة عالية التزامن ولكن أيضًا سيناريوهات التحليل المعقدة عالية الإنتاجية.

كل هذا يجعل Apache Doris أداة مثالية للسيناريوهات التي تشمل تحليل التقارير، والاستعلامات المخصصة، ومستودع البيانات الموحد، وتسريع استعلام بحيرة البيانات. على Apache Doris، يمكن للمستخدمين بناء تطبيقات متنوعة، مثل تحليل سلوك المستخدم، ومنصة اختبار AB، وتحليل استرجاع السجلات، وتحليل ملف المستخدم، وتحليل الطلبات.

🎉 تحقق من 🔗[جميع الإصدارات](https://doris.apache.org/docs/releasenotes/all-release)، حيث ستجد ملخصًا زمنيًا لإصدارات Apache Doris التي تم إصدارها خلال العام الماضي.

👀 استكشف 🔗[الموقع الرسمي](https://doris.apache.org/) لاكتشاف الميزات الأساسية والمدونات وحالات الاستخدام لـ Apache Doris بالتفصيل.

## 📈 سيناريوهات الاستخدام

كما هو موضح في الشكل أدناه، بعد تكاملات ومعالجات البيانات المختلفة، عادة ما يتم تخزين مصادر البيانات في مستودع البيانات في الوقت الفعلي Apache Doris وبحيرة البيانات غير المتصلة أو مستودع البيانات (في Apache Hive أو Apache Iceberg أو Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


يُستخدم Apache Doris على نطاق واسع في السيناريوهات التالية:

- **تحليل البيانات في الوقت الفعلي**:

  - **التقارير واتخاذ القرارات في الوقت الفعلي**: يوفر Doris تقارير ولوحات معلومات محدثة في الوقت الفعلي للاستخدام المؤسسي الداخلي والخارجي، مما يدعم اتخاذ القرارات في الوقت الفعلي في العمليات الآلية.
  
  - **التحليل المخصص**: يقدم Doris قدرات تحليل بيانات متعددة الأبعاد، مما يتيح تحليلات ذكاء الأعمال السريعة والاستعلامات المخصصة لمساعدة المستخدمين على اكتشاف الرؤى بسرعة من البيانات المعقدة.
  
  - **إنشاء ملف المستخدم وتحليل السلوك**: يمكن لـ Doris تحليل سلوكيات المستخدمين مثل المشاركة والاحتفاظ والتحويل، بينما يدعم أيضًا سيناريوهات مثل رؤى السكان واختيار الجمهور لتحليل السلوك.

- **تحليلات بحيرة البيانات**:

  - **تسريع استعلام بحيرة البيانات**: يسرع Doris استعلامات بيانات بحيرة البيانات بمحرك الاستعلام الفعال.
  
  - **التحليلات الموحدة**: يدعم Doris استعلامات موحدة عبر مصادر بيانات متعددة، مما يبسط البنية ويزيل صوامع البيانات.
  
  - **معالجة البيانات في الوقت الفعلي**: يجمع Doris قدرات معالجة تدفقات البيانات في الوقت الفعلي ومعالجة البيانات الدفعية لتلبية احتياجات التزامن العالي وزمن الاستجابة المنخفض للمتطلبات التجارية المعقدة.

- **القدرة على المراقبة القائمة على SQL**:

  - **تحليل السجلات والأحداث**: يتيح Doris تحليل السجلات والأحداث في الوقت الفعلي أو الدفعي في الأنظمة الموزعة، مما يساعد في تحديد المشكلات وتحسين الأداء.


## البنية العامة

يستخدم Apache Doris بروتوكول MySQL، وهو متوافق بشكل كبير مع بناء جملة MySQL ويدعم SQL القياسي. يمكن للمستخدمين الوصول إلى Apache Doris من خلال أدوات العميل المختلفة، ويتكامل بسلاسة مع أدوات BI.

### بنية التخزين والحوسبة المتكاملة

بنية التخزين والحوسبة المتكاملة لـ Apache Doris مبسطة وسهلة الصيانة. كما هو موضح في الشكل أدناه، تتكون من نوعين فقط من العمليات:

- **Frontend (FE):** مسؤول بشكل أساسي عن معالجة طلبات المستخدم، وتحليل الاستعلام والتخطيط، وإدارة البيانات الوصفية، ومهام إدارة العقد.

- **Backend (BE):** مسؤول بشكل أساسي عن تخزين البيانات وتنفيذ الاستعلام. يتم تقسيم البيانات إلى أجزاء وتخزينها مع نسخ متعددة عبر عقد BE.

![البنية العامة لـ Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

في بيئة الإنتاج، يمكن نشر عدة عقد FE للتعافي من الكوارث. تحافظ كل عقدة FE على نسخة كاملة من البيانات الوصفية. تنقسم عقد FE إلى ثلاثة أدوار:

| الدور      | الوظيفة                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | عقدة FE Master مسؤولة عن عمليات قراءة وكتابة البيانات الوصفية. عندما تحدث تغييرات في البيانات الوصفية في Master، يتم مزامنتها مع عقد Follower أو Observer عبر بروتوكول BDB JE. |
| Follower  | عقدة Follower مسؤولة عن قراءة البيانات الوصفية. إذا فشلت عقدة Master، يمكن اختيار عقدة Follower كـ Master جديد. |
| Observer  | عقدة Observer مسؤولة عن قراءة البيانات الوصفية وتستخدم بشكل أساسي لزيادة التزامن في الاستعلام. لا تشارك في انتخابات قيادة المجموعة. |

كل من عمليات FE وBE قابلة للتوسع أفقيًا، مما يتيح لمجموعة واحدة دعم مئات الآلات وعشرات البيتابايت من سعة التخزين. تستخدم عمليات FE وBE بروتوكول الاتساق لضمان توفر الخدمات العالي وموثوقية البيانات العالية. البنية المتكاملة للتخزين والحوسبة متكاملة بشكل كبير، مما يقلل بشكل كبير من تعقيد تشغيل الأنظمة الموزعة.


## الميزات الأساسية لـ Apache Doris

- **التوفر العالي**: في Apache Doris، يتم تخزين كل من البيانات الوصفية والبيانات مع نسخ متعددة، ومزامنة سجلات البيانات عبر بروتوكول quorum. يعتبر كتابة البيانات ناجحة بمجرد أن تكمل غالبية النسخ الكتابة، مما يضمن أن المجموعة تبقى متاحة حتى إذا فشلت بعض العقد. يدعم Apache Doris التعافي من الكوارث في نفس المدينة وعبر المناطق، مما يتيح أوضاع master-slave للمجموعة المزدوجة. عندما تواجه بعض العقد أعطالًا، يمكن للمجموعة عزل العقد المعطلة تلقائيًا، مما يمنع تأثر التوفر العام للمجموعة.

- **التوافق العالي**: Apache Doris متوافق بشكل كبير مع بروتوكول MySQL ويدعم بناء جملة SQL القياسي، ويغطي معظم وظائف MySQL و Hive. يتيح هذا التوافق العالي للمستخدمين نقل وتكامل التطبيقات والأدوات الموجودة بسلاسة. يدعم Apache Doris نظام MySQL البيئي، مما يتيح للمستخدمين الاتصال بـ Doris باستخدام أدوات MySQL Client لعمليات وصيانة أكثر ملاءمة. كما يدعم أيضًا توافق بروتوكول MySQL لأدوات إعداد التقارير BI وأدوات نقل البيانات، مما يضمن الكفاءة والاستقرار في عمليات تحليل البيانات ونقل البيانات.

- **مستودع البيانات في الوقت الفعلي**: بناءً على Apache Doris، يمكن بناء خدمة مستودع بيانات في الوقت الفعلي. يقدم Apache Doris قدرات استيعاب البيانات على مستوى الثانية، والتقاط التغييرات التدريجية من قواعد البيانات المعاملية عبر الإنترنت في المنبع إلى Doris في غضون ثوانٍ. من خلال الاستفادة من المحركات المتجهة، وبنية MPP، ومحركات تنفيذ Pipeline، يوفر Doris قدرات استعلام بيانات أقل من ثانية، وبالتالي بناء منصة مستودع بيانات في الوقت الفعلي عالية الأداء ومنخفضة زمن الاستجابة.

- **بحيرة البيانات الموحدة**: يمكن لـ Apache Doris بناء بنية بحيرة بيانات موحدة بناءً على مصادر بيانات خارجية مثل بحيرات البيانات أو قواعد البيانات العلائقية. يتيح حل بحيرة البيانات الموحدة لـ Doris التكامل السلس وتدفق البيانات الحر بين بحيرات البيانات ومستودعات البيانات، مما يساعد المستخدمين على استخدام قدرات مستودع البيانات مباشرة لحل مشاكل تحليل البيانات في بحيرات البيانات مع الاستفادة الكاملة من قدرات إدارة بيانات بحيرة البيانات لتعزيز قيمة البيانات.

- **النمذجة المرنة**: يقدم Apache Doris أساليب نمذجة متنوعة، مثل نماذج الجدول العريض، ونماذج ما قبل التجميع، ومخططات النجمة/الثلج، إلخ. أثناء استيراد البيانات، يمكن تسطيح البيانات إلى جداول عريضة وكتابتها في Doris من خلال محركات الحوسبة مثل Flink أو Spark، أو يمكن استيراد البيانات مباشرة إلى Doris، وإجراء عمليات نمذجة البيانات من خلال المشاهدات، والمشاهدات المادية، أو عمليات الانضمام متعددة الجداول في الوقت الفعلي.

## نظرة عامة تقنية

يوفر Doris واجهة SQL فعالة وهو متوافق بالكامل مع بروتوكول MySQL. يعتمد محرك الاستعلام الخاص به على بنية MPP (المعالجة المتوازية الضخمة)، قادرة على تنفيذ استعلامات تحليلية معقدة بكفاءة وتحقيق استعلامات في الوقت الفعلي منخفضة زمن الاستجابة. من خلال تقنية التخزين العمودي لتشفير وضغط البيانات، فإنه يحسن بشكل كبير أداء الاستعلام ونسبة ضغط التخزين.

### الواجهة

يتبنى Apache Doris بروتوكول MySQL، ويدعم SQL القياسي، وهو متوافق بشكل كبير مع بناء جملة MySQL. يمكن للمستخدمين الوصول إلى Apache Doris من خلال أدوات العميل المختلفة وتكاملها بسلاسة مع أدوات BI، بما في ذلك ولكن لا يقتصر على Smartbi و DataEase و FineBI و Tableau و Power BI و Apache Superset. يمكن لـ Apache Doris العمل كمصدر بيانات لأي أدوات BI تدعم بروتوكول MySQL.

### محرك التخزين

Apache Doris لديه محرك تخزين عمودي، والذي يشفر ويضغط ويقرأ البيانات حسب العمود. هذا يتيح نسبة ضغط بيانات عالية جدًا ويقلل إلى حد كبير من فحص البيانات غير الضروري، وبالتالي يجعل استخدام موارد IO و CPU أكثر كفاءة.

يدعم Apache Doris هياكل فهرس متنوعة لتقليل فحوصات البيانات:

- **فهرس المفتاح المركب المرتب**: يمكن للمستخدمين تحديد ثلاثة أعمدة كحد أقصى لتشكيل مفتاح ترتيب مركب. يمكن أن يقطع هذا البيانات بشكل فعال لدعم أفضل لسيناريوهات إعداد التقارير عالية التزامن.

- **فهرس Min/Max**: يتيح هذا تصفية بيانات فعالة في استعلامات التكافؤ والنطاق للأنواع الرقمية.

- **فهرس BloomFilter**: هذا فعال جدًا في تصفية التكافؤ وتقليم الأعمدة عالية الكاردينالية.

- **الفهرس المقلوب**: يتيح هذا البحث السريع لأي حقل.

يدعم Apache Doris مجموعة متنوعة من نماذج البيانات وقام بتحسينها لسيناريوهات مختلفة:

- **نموذج التفاصيل (نموذج المفتاح المكرر):** نموذج بيانات تفاصيل مصمم لتلبية متطلبات التخزين التفصيلية لجداول الحقائق.

- **نموذج المفتاح الأساسي (نموذج المفتاح الفريد):** يضمن مفاتيح فريدة؛ يتم الكتابة فوق البيانات بنفس المفتاح، مما يتيح تحديثات البيانات على مستوى الصف.

- **نموذج التجميع (نموذج مفتاح التجميع):** يدمج أعمدة القيم بنفس المفتاح، مما يحسن الأداء بشكل كبير من خلال التجميع المسبق.

يدعم Apache Doris أيضًا المشاهدات المادية لجدول واحد متسقة بقوة والمشاهدات المادية متعددة الجداول المحدثة بشكل غير متزامن. يتم تحديث المشاهدات المادية لجدول واحد وصيانتها تلقائيًا بواسطة النظام، ولا تتطلب تدخلًا يدويًا من المستخدمين. يمكن تحديث المشاهدات المادية متعددة الجداول بشكل دوري باستخدام الجدولة داخل المجموعة أو أدوات الجدولة الخارجية، مما يقلل من تعقيد نمذجة البيانات.

### 🔍 محرك الاستعلام

Apache Doris لديه محرك استعلام يعتمد على MPP للتنفيذ المتوازي بين العقد وداخل العقد. يدعم الانضمام shuffle الموزع للجداول الكبيرة للتعامل بشكل أفضل مع الاستعلامات المعقدة.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

محرك الاستعلام لـ Apache Doris متجه بالكامل، مع جميع هياكل الذاكرة موضوعة بتنسيق عمودي. يمكن أن يقلل هذا بشكل كبير من استدعاءات الوظائف الافتراضية، ويزيد من معدلات ضربات التخزين المؤقت، ويجعل استخدام تعليمات SIMD فعالًا. يوفر Apache Doris أداءً أعلى من 5 إلى 10 مرات في سيناريوهات تجميع الجدول العريض من المحركات غير المتجهة.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

يستخدم Apache Doris تقنية تنفيذ الاستعلام التكيفية لضبط خطة التنفيذ ديناميكيًا بناءً على إحصائيات وقت التشغيل. على سبيل المثال، يمكنه إنشاء مرشح وقت التشغيل ودفعه إلى جانب المسبار. على وجه التحديد، يدفع المرشحات إلى عقدة المسح ذات المستوى الأدنى على جانب المسبار، مما يقلل بشكل كبير من كمية البيانات المراد معالجتها ويزيد من أداء الانضمام. يدعم مرشح وقت التشغيل لـ Apache Doris In/Min/Max/Bloom Filter.

يستخدم Apache Doris محرك تنفيذ Pipeline الذي يقسم الاستعلامات إلى مهام فرعية متعددة للتنفيذ المتوازي، مستفيدًا بالكامل من قدرات CPU متعددة النوى. يعالج في نفس الوقت مشكلة انفجار الخيوط عن طريق الحد من عدد خيوط الاستعلام. يقلل محرك تنفيذ Pipeline من نسخ البيانات ومشاركتها، ويحسن عمليات الفرز والتجميع، وبالتالي يحسن بشكل كبير كفاءة الاستعلام والإنتاجية.

من حيث المحسن، يستخدم Apache Doris استراتيجية تحسين مجمعة لـ CBO (المحسن المستند إلى التكلفة)، و RBO (المحسن المستند إلى القواعد)، و HBO (المحسن المستند إلى التاريخ). يدعم RBO طي الثوابت، وإعادة كتابة الاستعلام الفرعي، ودفع المسند، والمزيد. يدعم CBO إعادة ترتيب الانضمام وتحسينات أخرى. يوصي HBO بخطة التنفيذ المثلى بناءً على معلومات الاستعلام التاريخية. تضمن هذه التدابير المتعددة للتحسين أن Doris يمكنه تعداد خطط الاستعلام عالية الأداء لأنواع مختلفة من الاستعلامات.


## 🎆 لماذا تختار Apache Doris?

- 🎯 **سهل الاستخدام**: عمليتان، لا توجد تبعيات أخرى؛ توسيع المجموعة عبر الإنترنت، استعادة النسخ التلقائية؛ متوافق مع بروتوكول MySQL، واستخدام SQL القياسي.

- 🚀 **أداء عالي**: أداء سريع للغاية لاستعلامات زمن استجابة منخفض وإنتاجية عالية مع محرك تخزين عمودي، وبنية MPP حديثة، ومحرك استعلام متجه، ومشهد مادي مجمع مسبقًا وفهرس بيانات.

- 🖥️ **موحد واحد**: يمكن لنظام واحد دعم سيناريوهات خدمة البيانات في الوقت الفعلي، وتحليل البيانات التفاعلي، ومعالجة البيانات غير المتصلة.

- ⚛️ **الاستعلام الموحد**: يدعم الاستعلام الموحد لبحيرات البيانات مثل Hive و Iceberg و Hudi وقواعد البيانات مثل MySQL و Elasticsearch.

- ⏩ **طرق استيراد بيانات متنوعة**: يدعم الاستيراد الدفعي من HDFS/S3 والاستيراد المتدفق من MySQL Binlog/Kafka؛ يدعم الكتابة الدقيقة الدفعية من خلال واجهة HTTP والكتابة في الوقت الفعلي باستخدام Insert في JDBC.

- 🚙 **نظام بيئي غني**: يستخدم Spark Spark-Doris-Connector لقراءة وكتابة Doris؛ يتيح Flink-Doris-Connector لـ Flink CDC تنفيذ كتابة بيانات exactly-once إلى Doris؛ يتم توفير DBT Doris Adapter لتحويل البيانات في Doris باستخدام DBT.

## 🙌 المساهمون

**تخرج Apache Doris بنجاح من حاضنة Apache وأصبح مشروعًا من المستوى الأعلى في يونيو 2022**.

نقدر بعمق 🔗[مساهمي المجتمع](https://github.com/apache/doris/graphs/contributors) لمساهمتهم في Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 المستخدمون

Apache Doris لديه الآن قاعدة مستخدمين واسعة في الصين وحول العالم، واعتبارًا من اليوم، **يُستخدم Apache Doris في بيئات الإنتاج في آلاف الشركات حول العالم.** أكثر من 80% من أفضل 50 شركة إنترنت في الصين من حيث القيمة السوقية أو التقييم تستخدم Apache Doris منذ فترة طويلة، بما في ذلك Baidu و Meituan و Xiaomi و Jingdong و Bytedance و Tencent و NetEase و Kwai و Sina و 360 و Mihoyo و Ke Holdings. كما يُستخدم على نطاق واسع في بعض الصناعات التقليدية مثل المالية والطاقة والتصنيع والاتصالات.

مستخدمو Apache Doris: 🔗[المستخدمون](https://doris.apache.org/users)

أضف شعار شركتك في موقع Apache Doris: 🔗[أضف شركتك](https://github.com/apache/doris/discussions/27683)
 
## 👣 ابدأ

### 📚 الوثائق

جميع الوثائق   🔗[الوثائق](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ التنزيل 

جميع إصدارات الإصدار والإصدارات الثنائية 🔗[التنزيل](https://doris.apache.org/download) 

### 🗄️ التجميع

انظر كيفية التجميع  🔗[التجميع](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 التثبيت

انظر كيفية التثبيت والنشر 🔗[التثبيت والنشر](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 المكونات

### 📝 Doris Connector

يوفر Doris دعمًا لـ Spark/Flink لقراءة البيانات المخزنة في Doris من خلال Connector، ويدعم أيضًا كتابة البيانات إلى Doris من خلال Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 المجتمع والدعم

### 📤 الاشتراك في قوائم البريد

قائمة البريد هي الشكل الأكثر اعترافًا للتواصل في مجتمع Apache. انظر كيفية 🔗[الاشتراك في قوائم البريد](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 الإبلاغ عن المشكلات أو إرسال Pull Request

إذا واجهت أي أسئلة، لا تتردد في تقديم 🔗[GitHub Issue](https://github.com/apache/doris/issues) أو نشره في 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) وإصلاحه عن طريق إرسال 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 كيفية المساهمة

نرحب باقتراحاتك وتعليقاتك (بما في ذلك الانتقادات) والتعليقات والمساهمات. انظر 🔗[كيفية المساهمة](https://doris.apache.org/community/how-to-contribute/) و 🔗[دليل إرسال الكود](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ مقترحات تحسين Doris (DSIP)

🔗[مقترح تحسين Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) يمكن اعتباره **مجموعة من مستندات التصميم لجميع التحديثات أو التحسينات الرئيسية للميزات**.

### 🔑 مواصفات ترميز Backend C++
🔗 [مواصفات ترميز Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) يجب اتباعها بدقة، مما سيساعدنا على تحقيق جودة كود أفضل.

## 💬 اتصل بنا

اتصل بنا من خلال قائمة البريد التالية.

| الاسم                                                                          | النطاق                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | المناقشات المتعلقة بالتطوير | [الاشتراك](mailto:dev-subscribe@doris.apache.org)   | [إلغاء الاشتراك](mailto:dev-unsubscribe@doris.apache.org)   | [الأرشيفات](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 الروابط

* الموقع الرسمي لـ Apache Doris - [الموقع](https://doris.apache.org)
* قائمة بريد المطورين - <dev@doris.apache.org>. أرسل بريدًا إلكترونيًا إلى <dev-subscribe@doris.apache.org>، اتبع الرد للاشتراك في قائمة البريد.
* قناة Slack - [انضم إلى Slack](https://doris.apache.org/slack)
* Twitter - [تابع @doris_apache](https://twitter.com/doris_apache)


## 📜 الترخيص

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **ملاحظة**
> بعض تراخيص تبعيات الطرف الثالث غير متوافقة مع ترخيص Apache 2.0. لذلك تحتاج إلى تعطيل
بعض ميزات Doris للامتثال لترخيص Apache 2.0. للتفاصيل، راجع الملف `thirdparty/LICENSE.txt`


