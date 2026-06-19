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

## 🌍 In anderen Sprachen lesen

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris ist eine einfach zu verwendende, leistungsstarke und Echtzeit-Analysedatenbank basierend auf MPP-Architektur, bekannt für ihre extreme Geschwindigkeit und Benutzerfreundlichkeit. Sie benötigt nur eine subsekundäre Antwortzeit, um Abfrageergebnisse unter massiven Daten zurückzugeben, und kann nicht nur Szenarien mit hoher Parallelität für Punktabfragen, sondern auch komplexe Analyseszenarien mit hohem Durchsatz unterstützen.

All dies macht Apache Doris zu einem idealen Werkzeug für Szenarien wie Berichtsanalyse, Ad-hoc-Abfragen, einheitliches Data Warehouse und Data-Lake-Abfragebeschleunigung. Auf Apache Doris können Benutzer verschiedene Anwendungen erstellen, wie z. B. Benutzerverhaltensanalyse, AB-Testplattform, Protokollabrufanalyse, Benutzerprofilanalyse und Bestellanalyse.

🎉 Schauen Sie sich 🔗[Alle Versionen](https://doris.apache.org/docs/releasenotes/all-release) an, wo Sie eine chronologische Zusammenfassung der Apache Doris-Versionen finden, die im vergangenen Jahr veröffentlicht wurden.

👀 Erkunden Sie die 🔗[Offizielle Website](https://doris.apache.org/), um die Kernfunktionen, Blogs und Anwendungsfälle von Apache Doris im Detail zu entdecken.

## 📈 Anwendungsszenarien

Wie in der folgenden Abbildung dargestellt, werden die Datenquellen nach verschiedenen Datenintegrationen und -verarbeitungen normalerweise im Echtzeit-Data Warehouse Apache Doris und im Offline-Data Lake oder Data Warehouse (in Apache Hive, Apache Iceberg oder Apache Hudi) gespeichert.

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris wird in den folgenden Szenarien weit verbreitet verwendet:

- **Echtzeit-Datenanalyse**:

  - **Echtzeit-Berichterstattung und Entscheidungsfindung**: Doris bietet Echtzeit-aktualisierte Berichte und Dashboards für interne und externe Unternehmensnutzung und unterstützt die Echtzeit-Entscheidungsfindung in automatisierten Prozessen.
  
  - **Ad-hoc-Analyse**: Doris bietet mehrdimensionale Datenanalysefunktionen und ermöglicht schnelle Business-Intelligence-Analysen und Ad-hoc-Abfragen, um Benutzern zu helfen, schnell Erkenntnisse aus komplexen Daten zu gewinnen.
  
  - **Benutzerprofilierung und Verhaltensanalyse**: Doris kann Benutzerverhalten wie Teilnahme, Retention und Konversion analysieren und unterstützt auch Szenarien wie Bevölkerungsinsights und Gruppenselektion für Verhaltensanalysen.

- **Data-Lake-Analyse**:

  - **Data-Lake-Abfragebeschleunigung**: Doris beschleunigt Data-Lake-Datenabfragen mit seiner effizienten Abfrage-Engine.
  
  - **Föderierte Analyse**: Doris unterstützt föderierte Abfragen über mehrere Datenquellen hinweg, vereinfacht die Architektur und eliminiert Datensilos.
  
  - **Echtzeit-Datenverarbeitung**: Doris kombiniert Echtzeit-Datenströme und Batch-Datenverarbeitungsfunktionen, um den Anforderungen hoher Parallelität und niedriger Latenz komplexer Geschäftsanforderungen gerecht zu werden.

- **SQL-basierte Beobachtbarkeit**:

  - **Protokoll- und Ereignisanalyse**: Doris ermöglicht Echtzeit- oder Batch-Analysen von Protokollen und Ereignissen in verteilten Systemen und hilft dabei, Probleme zu identifizieren und die Leistung zu optimieren.


## Gesamtarchitektur

Apache Doris verwendet das MySQL-Protokoll, ist hochgradig kompatibel mit MySQL-Syntax und unterstützt Standard-SQL. Benutzer können über verschiedene Client-Tools auf Apache Doris zugreifen, und es integriert sich nahtlos mit BI-Tools.

### Speicher-Computing-integrierte Architektur

Die Speicher-Computing-integrierte Architektur von Apache Doris ist rationalisiert und einfach zu warten. Wie in der folgenden Abbildung dargestellt, besteht sie nur aus zwei Arten von Prozessen:

- **Frontend (FE):** Hauptsächlich verantwortlich für die Behandlung von Benutzeranfragen, Abfrageparsing und -planung, Metadatenverwaltung und Knotenverwaltungsaufgaben.

- **Backend (BE):** Hauptsächlich verantwortlich für Datenspeicherung und Abfrageausführung. Daten werden in Shards partitioniert und mit mehreren Replikaten über BE-Knoten hinweg gespeichert.

![Die Gesamtarchitektur von Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

In einer Produktionsumgebung können mehrere FE-Knoten für die Notfallwiederherstellung bereitgestellt werden. Jeder FE-Knoten verwaltet eine vollständige Kopie der Metadaten. Die FE-Knoten sind in drei Rollen unterteilt:

| Rolle      | Funktion                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Der FE Master-Knoten ist für Metadaten-Lese- und -Schreibvorgänge verantwortlich. Wenn Metadatenänderungen im Master auftreten, werden sie über das BDB JE-Protokoll an Follower- oder Observer-Knoten synchronisiert. |
| Follower  | Der Follower-Knoten ist für das Lesen von Metadaten verantwortlich. Wenn der Master-Knoten ausfällt, kann ein Follower-Knoten als neuer Master ausgewählt werden. |
| Observer  | Der Observer-Knoten ist für das Lesen von Metadaten verantwortlich und wird hauptsächlich verwendet, um die Abfrageparallelität zu erhöhen. Er nimmt nicht an Cluster-Führungswahlen teil. |

Sowohl FE- als auch BE-Prozesse sind horizontal skalierbar und ermöglichen es einem einzelnen Cluster, Hunderte von Maschinen und Dutzende von Petabytes Speicherkapazität zu unterstützen. Die FE- und BE-Prozesse verwenden ein Konsistenzprotokoll, um hohe Verfügbarkeit von Diensten und hohe Zuverlässigkeit von Daten sicherzustellen. Die Speicher-Computing-integrierte Architektur ist hochgradig integriert und reduziert die Betriebskomplexität verteilter Systeme erheblich.


## Kernfunktionen von Apache Doris

- **Hohe Verfügbarkeit**: In Apache Doris werden sowohl Metadaten als auch Daten mit mehreren Replikaten gespeichert, wobei Datenprotokolle über das Quorum-Protokoll synchronisiert werden. Der Datenschreibvorgang wird als erfolgreich angesehen, sobald eine Mehrheit der Replikate den Schreibvorgang abgeschlossen hat, wodurch sichergestellt wird, dass der Cluster verfügbar bleibt, auch wenn einige Knoten ausfallen. Apache Doris unterstützt sowohl Notfallwiederherstellung innerhalb derselben Stadt als auch regionsübergreifend und ermöglicht Dual-Cluster-Master-Slave-Modi. Wenn einige Knoten Ausfälle erleiden, kann der Cluster die fehlerhaften Knoten automatisch isolieren und verhindern, dass die Gesamtverfügbarkeit des Clusters beeinträchtigt wird.

- **Hohe Kompatibilität**: Apache Doris ist hochgradig kompatibel mit dem MySQL-Protokoll und unterstützt Standard-SQL-Syntax, die die meisten MySQL- und Hive-Funktionen abdeckt. Diese hohe Kompatibilität ermöglicht es Benutzern, bestehende Anwendungen und Tools nahtlos zu migrieren und zu integrieren. Apache Doris unterstützt das MySQL-Ökosystem und ermöglicht es Benutzern, Doris mit MySQL-Client-Tools zu verbinden, um bequemere Betriebs- und Wartungsvorgänge durchzuführen. Es unterstützt auch MySQL-Protokollkompatibilität für BI-Berichtstools und Datenübertragungstools und gewährleistet Effizienz und Stabilität in Datenanalyse- und Datenübertragungsprozessen.

- **Echtzeit-Data Warehouse**: Basierend auf Apache Doris kann ein Echtzeit-Data-Warehouse-Service erstellt werden. Apache Doris bietet Funktionen zur Datenerfassung auf Sekundenebene und erfasst inkrementelle Änderungen von Upstream-Online-Transaktionsdatenbanken innerhalb von Sekunden in Doris. Durch die Nutzung von vektorisierten Engines, MPP-Architektur und Pipeline-Ausführungs-Engines bietet Doris Funktionen zur Datenabfrage unter einer Sekunde und konstruiert damit eine hochleistungsfähige, niedriglatente Echtzeit-Data-Warehouse-Plattform.

- **Einheitlicher Data Lake**: Apache Doris kann eine einheitliche Data-Lake-Architektur basierend auf externen Datenquellen wie Data Lakes oder relationalen Datenbanken erstellen. Die einheitliche Data-Lake-Lösung von Doris ermöglicht nahtlose Integration und freien Datenfluss zwischen Data Lakes und Data Warehouses und hilft Benutzern, Data-Warehouse-Funktionen direkt zu nutzen, um Datenanalyseprobleme in Data Lakes zu lösen, während gleichzeitig die Datenverwaltungsfunktionen von Data Lakes voll ausgeschöpft werden, um den Datenwert zu steigern.

- **Flexibles Modellieren**: Apache Doris bietet verschiedene Modellierungsansätze wie Wide-Table-Modelle, Voraggregationsmodelle, Stern-/Schneeflockenschemata usw. Während des Datenimports können Daten in Wide-Tabellen abgeflacht und über Compute-Engines wie Flink oder Spark in Doris geschrieben werden, oder Daten können direkt in Doris importiert werden, wobei Datenmodellierungsvorgänge über Ansichten, materialisierte Ansichten oder Echtzeit-Mehrtabellen-Joins durchgeführt werden.

## Technische Übersicht

Doris bietet eine effiziente SQL-Schnittstelle und ist vollständig kompatibel mit dem MySQL-Protokoll. Seine Abfrage-Engine basiert auf einer MPP-Architektur (Massively Parallel Processing) und kann komplexe analytische Abfragen effizient ausführen und niedriglatente Echtzeitabfragen erreichen. Durch Spaltenspeichertechnologie für Datenkodierung und -kompression optimiert es die Abfrageleistung und das Speicherkompressionsverhältnis erheblich.

### Schnittstelle

Apache Doris übernimmt das MySQL-Protokoll, unterstützt Standard-SQL und ist hochgradig kompatibel mit MySQL-Syntax. Benutzer können über verschiedene Client-Tools auf Apache Doris zugreifen und es nahtlos mit BI-Tools integrieren, einschließlich, aber nicht beschränkt auf Smartbi, DataEase, FineBI, Tableau, Power BI und Apache Superset. Apache Doris kann als Datenquelle für alle BI-Tools fungieren, die das MySQL-Protokoll unterstützen.

### Speicher-Engine

Apache Doris verfügt über eine Spaltenspeicher-Engine, die Daten spaltenweise kodiert, komprimiert und liest. Dies ermöglicht ein sehr hohes Datenkompressionsverhältnis und reduziert unnötige Datenscans erheblich, wodurch IO- und CPU-Ressourcen effizienter genutzt werden.

Apache Doris unterstützt verschiedene Indexstrukturen, um Datenscans zu minimieren:

- **Sortierter zusammengesetzter Schlüsselindex**: Benutzer können höchstens drei Spalten angeben, um einen zusammengesetzten Sortierschlüssel zu bilden. Dies kann Daten effektiv beschneiden, um hochgradig parallele Berichtsszenarien besser zu unterstützen.

- **Min/Max-Index**: Dies ermöglicht effektive Datenfilterung bei Äquivalenz- und Bereichsabfragen numerischer Typen.

- **BloomFilter-Index**: Dies ist sehr effektiv bei Äquivalenzfilterung und Beschneidung von Spalten mit hoher Kardinalität.

- **Invertierter Index**: Dies ermöglicht schnelles Suchen nach jedem Feld.

Apache Doris unterstützt eine Vielzahl von Datenmodellen und hat sie für verschiedene Szenarien optimiert:

- **Detailmodell (Duplicate Key Model):** Ein Detaildatenmodell, das entwickelt wurde, um die detaillierten Speicheranforderungen von Faktentabellen zu erfüllen.

- **Primärschlüsselmodell (Unique Key Model):** Stellt eindeutige Schlüssel sicher; Daten mit demselben Schlüssel werden überschrieben, wodurch zeilenbasierte Datenaktualisierungen ermöglicht werden.

- **Aggregationsmodell (Aggregate Key Model):** Führt Wertspalten mit demselben Schlüssel zusammen und verbessert die Leistung erheblich durch Voraggregation.

Apache Doris unterstützt auch stark konsistente materialisierte Ansichten mit einer einzelnen Tabelle und asynchron aktualisierte materialisierte Ansichten mit mehreren Tabellen. Materialisierte Ansichten mit einer einzelnen Tabelle werden automatisch vom System aktualisiert und gewartet und erfordern keine manuelle Intervention der Benutzer. Materialisierte Ansichten mit mehreren Tabellen können periodisch mit In-Cluster-Planung oder externen Planungstools aktualisiert werden, wodurch die Komplexität der Datenmodellierung reduziert wird.

### 🔍 Abfrage-Engine

Apache Doris verfügt über eine MPP-basierte Abfrage-Engine für parallele Ausführung zwischen und innerhalb von Knoten. Es unterstützt verteilte Shuffle-Joins für große Tabellen, um komplizierte Abfragen besser zu handhaben.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Die Abfrage-Engine von Apache Doris ist vollständig vektorisiert, wobei alle Speicherstrukturen in einem Spaltenformat angeordnet sind. Dies kann virtuelle Funktionsaufrufe erheblich reduzieren, Cache-Trefferraten erhöhen und SIMD-Anweisungen effizient nutzen. Apache Doris liefert eine 5-10 mal höhere Leistung in Wide-Table-Aggregationsszenarien als nicht-vektorisierte Engines.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris verwendet adaptive Abfrageausführungstechnologie, um den Ausführungsplan dynamisch basierend auf Laufzeitstatistiken anzupassen. Beispielsweise kann es einen Laufzeitfilter generieren und ihn zur Prüfseite pushen. Insbesondere pusht es die Filter zum Scan-Knoten der niedrigsten Ebene auf der Prüfseite, was die zu verarbeitende Datenmenge erheblich reduziert und die Join-Leistung erhöht. Der Laufzeitfilter von Apache Doris unterstützt In/Min/Max/Bloom Filter.

Apache Doris verwendet eine Pipeline-Ausführungs-Engine, die Abfragen in mehrere Unteraufgaben für parallele Ausführung aufteilt und Multi-Core-CPU-Funktionen voll ausnutzt. Es behebt gleichzeitig das Thread-Explosionsproblem, indem es die Anzahl der Abfragethreads begrenzt. Die Pipeline-Ausführungs-Engine reduziert Datenkopierung und -freigabe, optimiert Sortier- und Aggregationsvorgänge und verbessert dadurch die Abfrageeffizienz und den Durchsatz erheblich.

In Bezug auf den Optimierer verwendet Apache Doris eine kombinierte Optimierungsstrategie von CBO (Cost-Based Optimizer), RBO (Rule-Based Optimizer) und HBO (History-Based Optimizer). RBO unterstützt Konstantenfaltung, Unterabfrageumschreibung, Prädikat-Pushdown und mehr. CBO unterstützt Join-Neuordnung und andere Optimierungen. HBO empfiehlt den optimalen Ausführungsplan basierend auf historischen Abfrageinformationen. Diese mehrfachen Optimierungsmaßnahmen stellen sicher, dass Doris leistungsstarke Abfragepläne für verschiedene Arten von Abfragen aufzählen kann.


## 🎆 Warum Apache Doris wählen?

- 🎯 **Einfach zu verwenden**: Zwei Prozesse, keine anderen Abhängigkeiten; Online-Cluster-Skalierung, automatische Replikatwiederherstellung; kompatibel mit MySQL-Protokoll und Verwendung von Standard-SQL.

- 🚀 **Hohe Leistung**: Extrem schnelle Leistung für niedriglatente und hochdurchsatzfähige Abfragen mit Spaltenspeicher-Engine, moderner MPP-Architektur, vektorisierter Abfrage-Engine, voraggregierter materialisierter Ansicht und Datenindex.

- 🖥️ **Einheitlich**: Ein einzelnes System kann Echtzeit-Datenservice, interaktive Datenanalyse und Offline-Datenverarbeitungsszenarien unterstützen.

- ⚛️ **Föderierte Abfrage**: Unterstützt föderierte Abfragen von Data Lakes wie Hive, Iceberg, Hudi und Datenbanken wie MySQL und Elasticsearch.

- ⏩ **Verschiedene Datenimportmethoden**: Unterstützt Batch-Import von HDFS/S3 und Stream-Import von MySQL Binlog/Kafka; unterstützt Micro-Batch-Schreiben über HTTP-Schnittstelle und Echtzeit-Schreiben mit Insert in JDBC.

- 🚙 **Reiche Ökologie**: Spark verwendet Spark-Doris-Connector, um Doris zu lesen und zu schreiben; Flink-Doris-Connector ermöglicht es Flink CDC, genau einmaliges Datenschreiben in Doris zu implementieren; DBT Doris Adapter wird bereitgestellt, um Daten in Doris mit DBT zu transformieren.

## 🙌 Mitwirkende

**Apache Doris hat erfolgreich den Apache-Inkubator abgeschlossen und wurde im Juni 2022 zu einem Top-Level-Projekt**.

Wir schätzen die 🔗[Community-Mitwirkenden](https://github.com/apache/doris/graphs/contributors) sehr für ihren Beitrag zu Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Benutzer

Apache Doris hat jetzt eine breite Benutzerbasis in China und auf der ganzen Welt, und ab heute wird **Apache Doris in Produktionsumgebungen in Tausenden von Unternehmen weltweit verwendet.** Mehr als 80% der Top-50-Internetunternehmen in China in Bezug auf Marktkapitalisierung oder Bewertung verwenden Apache Doris seit langem, einschließlich Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo und Ke Holdings. Es wird auch in einigen traditionellen Branchen wie Finanzen, Energie, Fertigung und Telekommunikation weit verbreitet verwendet.

Die Benutzer von Apache Doris: 🔗[Benutzer](https://doris.apache.org/users)

Fügen Sie Ihr Firmenlogo auf der Apache Doris-Website hinzu: 🔗[Fügen Sie Ihr Unternehmen hinzu](https://github.com/apache/doris/discussions/27683)
 
## 👣 Erste Schritte

### 📚 Dokumentation

Alle Dokumentation   🔗[Dokumentation](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Download 

Alle Release- und Binärversionen 🔗[Download](https://doris.apache.org/download) 

### 🗄️ Kompilieren

Sehen Sie, wie man kompiliert  🔗[Kompilierung](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Installieren

Sehen Sie, wie man installiert und bereitstellt 🔗[Installation und Bereitstellung](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Komponenten

### 📝 Doris Connector

Doris bietet Unterstützung für Spark/Flink, um über Connector in Doris gespeicherte Daten zu lesen, und unterstützt auch das Schreiben von Daten in Doris über Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Community und Support

### 📤 Mailinglisten abonnieren

Die Mailingliste ist die am meisten anerkannte Kommunikationsform in der Apache-Community. Sehen Sie, wie man 🔗[Mailinglisten abonniert](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Probleme melden oder Pull Request einreichen

Wenn Sie Fragen haben, können Sie gerne ein 🔗[GitHub Issue](https://github.com/apache/doris/issues) einreichen oder es in 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) veröffentlichen und es durch Einreichen eines 🔗[Pull Request](https://github.com/apache/doris/pulls) beheben

### 🍻 Wie man beiträgt

Wir begrüßen Ihre Vorschläge, Kommentare (einschließlich Kritik), Kommentare und Beiträge. Sehen Sie 🔗[Wie man beiträgt](https://doris.apache.org/community/how-to-contribute/) und 🔗[Code-Einreichungsleitfaden](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Doris-Verbesserungsvorschläge (DSIP)

🔗[Doris-Verbesserungsvorschlag (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) kann als **Eine Sammlung von Designdokumenten für alle wichtigen Funktionsaktualisierungen oder -verbesserungen** betrachtet werden.

### 🔑 Backend C++ Codierungsrichtlinie
🔗 [Backend C++ Codierungsrichtlinie](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) sollte strikt befolgt werden, was uns helfen wird, bessere Codequalität zu erreichen.

## 💬 Kontaktieren Sie uns

Kontaktieren Sie uns über die folgende Mailingliste.

| Name                                                                          | Bereich                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Entwicklungsbezogene Diskussionen | [Abonnieren](mailto:dev-subscribe@doris.apache.org)   | [Abonnement kündigen](mailto:dev-unsubscribe@doris.apache.org)   | [Archive](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Links

* Apache Doris Offizielle Website - [Website](https://doris.apache.org)
* Entwickler-Mailingliste - <dev@doris.apache.org>. Senden Sie eine E-Mail an <dev-subscribe@doris.apache.org>, folgen Sie der Antwort, um die Mailingliste zu abonnieren.
* Slack-Kanal - [Slack beitreten](https://doris.apache.org/slack)
* Twitter - [Folgen Sie @doris_apache](https://twitter.com/doris_apache)


## 📜 Lizenz

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Hinweis**
> Einige Lizenzen der Drittanbieterabhängigkeiten sind nicht mit der Apache 2.0-Lizenz kompatibel. Sie müssen daher einige
Doris-Funktionen deaktivieren, um der Apache 2.0-Lizenz zu entsprechen. Einzelheiten finden Sie in der Datei `thirdparty/LICENSE.txt`


