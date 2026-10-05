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

## 🌍 Lire ce document dans d'autres langues

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris est une base de données open source d'analyse et de recherche en temps réel, conçue sur une architecture MPP. Il fournit des analyses SQL rapides, accélère les requêtes lakehouse et permet une recherche hybride sur des données structurées, textuelles et vectorielles.

Consultez le [site officiel](https://doris.apache.org/) pour découvrir la dernière présentation du produit, les cas d'utilisation, les actualités de l'écosystème, les articles de blog et les témoignages d'utilisateurs. Pour les mises à jour de version, consultez [toutes les notes de version](https://doris.apache.org/releases/all-release).

## 📈 Cas d’utilisation

| Cas d'utilisation | Ce qu'il fournit |
| -------- | ---------------- |
| [Analyses orientées client](https://doris.apache.org/use-cases/customer-facing-analytics) | Fournissez des analyses interactives en moins d'une seconde à des utilisateurs externes. |
| [Entrepôt de données](https://doris.apache.org/use-cases/data-warehousing) | Créez un entrepôt temps réel unique couvrant tous les domaines métier. |
| [Observabilité](https://doris.apache.org/use-cases/observability) | Analysez des journaux, événements et métriques à haut débit avec SQL. |
| [Doris pour l'AI](https://doris.apache.org/use-cases/ai) | Utilisez la recherche vectorielle, textuelle, JSON et structurée dans un même moteur SQL. |

## 🚀 Capacités principales

Apache Doris repose sur trois capacités principales. Le site Web fait référence pour les descriptions détaillées du produit et les exemples.

| Capacité | Ce qu'elle fournit |
| ---------- | ---------------- |
| [Analyses en temps réel](https://doris.apache.org/#real-time-analytics) | Ingestion en streaming, transformation incrémentale et requêtes en moins d'une seconde sous forte concurrence. |
| [Analyses lakehouse](https://doris.apache.org/#lakehouse-analytics) | Analyses SQL rapides sur des formats de tables ouverts tels que Iceberg, Delta Lake et Hudi. |
| [Recherche hybride](https://doris.apache.org/#hybrid-search) | Analyses SQL natives sur des données JSON, plein texte et vectorielles pour les charges de travail d'AI et de recherche. |

## 🔌 Écosystème

Doris se trouve au centre de la pile de données moderne. Il relie les bases de données amont, les systèmes de streaming et le stockage lakehouse aux outils aval de BI, d'AI, d'analytique et d'observabilité.

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Écosystème Apache Doris" width="850" />
</p>

Pour les informations les plus récentes sur l'écosystème, consultez le [site officiel](https://doris.apache.org/) et la [documentation sur les connexions et les intégrations](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro).

## 👣 Bien démarrer

- [Qu'est-ce qu'Apache Doris](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [Démarrage rapide](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [Télécharger](https://doris.apache.org/download)
- [Installation](https://doris.apache.org/docs/dev/install/intro)
- [Compiler à partir des sources](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 Architecture

Apache Doris prend en charge les déploiements où le calcul et le stockage sont couplés, ainsi que ceux où ils sont découplés. En mode découplé, des groupes de calcul sans état s'exécutent sur un stockage objet partagé, ce qui permet de faire évoluer le calcul à la demande et d'isoler les charges de travail.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Architecture découplée calcul-stockage d'Apache Doris" width="850" />
</p>

Pour en savoir plus, consultez le [guide de déploiement](https://doris.apache.org/docs/dev/install/intro) et le [guide des modes de déploiement](https://doris.apache.org/docs/dev/install/choosing-deployment-mode).

## 📣 Actualités du projet

| Ressource | Ce qu'elle fournit |
| -------- | ---------------- |
| [Rapport de la communauté](https://doris.apache.org/community-report/) | Mises à jour hebdomadaires sur l'activité de la communauté, les PR fusionnées, les contributeurs et l'avancement des fonctionnalités. |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | Discussion de planification 2026 pour les travaux sur l'AI et la recherche hybride, le moteur de requêtes, le stockage et le lac de données. |

## 🧩 Composants

Doris fournit des connecteurs et des outils pour les workflows courants d'ingénierie des données.

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 Utilisateurs

Apache Doris est utilisé en production par des milliers d'entreprises dans le monde entier, dans les services Internet, la finance, la distribution, la logistique, l'industrie, l'énergie, les télécommunications, l'AI et d'autres secteurs.

- [Utilisateurs d'Apache Doris](https://doris.apache.org/why-doris/users/)
- [Ajoutez votre entreprise](https://github.com/apache/doris/discussions/27683)

## 🙌 Contributeurs

Apache Doris est sorti de l'Apache Incubator et est devenu un Apache Top-Level Project en juin 2022. Merci à tous les [contributeurs de la communauté](https://github.com/apache/doris/graphs/contributors) qui participent au développement de Doris.

[![graphique des contributions](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 Communauté et support

- [Problèmes GitHub](https://github.com/apache/doris/issues)
- [Discussions sur GitHub](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [Comment contribuer](https://doris.apache.org/community/how-to-contribute/)
- [Guide de soumission de code](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 Nous contacter

- [Rejoignez Slack](https://doris.apache.org/slack)
- [Abonnez-vous à la liste de diffusion des développeurs](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 Liens

- Site officiel d'Apache Doris : [doris.apache.org](https://doris.apache.org)
- X : [@doris_apache](https://twitter.com/doris_apache)
- Medium : [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 Licence

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Remarque**
> Certaines licences de dépendances tierces ne sont pas compatibles avec l'Apache 2.0 License. Vous devez donc désactiver
certaines fonctionnalités de Doris pour respecter l'Apache 2.0 License. Pour plus de détails, consultez `thirdparty/LICENSE.txt`
