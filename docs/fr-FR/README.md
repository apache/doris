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

## 🌍 Lire dans d'autres langues

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris est une base de données analytique facile à utiliser, haute performance et en temps réel basée sur l'architecture MPP, connue pour sa vitesse extrême et sa facilité d'utilisation. Il ne nécessite qu'un temps de réponse inférieur à la seconde pour renvoyer les résultats de requête sous des données massives et peut prendre en charge non seulement les scénarios de requête ponctuelle à haute concurrence mais aussi les scénarios d'analyse complexe à haut débit.

Tout cela fait d'Apache Doris un outil idéal pour les scénarios incluant l'analyse de rapports, les requêtes ad-hoc, l'entrepôt de données unifié et l'accélération des requêtes de lac de données. Sur Apache Doris, les utilisateurs peuvent construire diverses applications, telles que l'analyse du comportement des utilisateurs, la plateforme de test AB, l'analyse de récupération de journaux, l'analyse de profil utilisateur et l'analyse des commandes.

🎉 Consultez 🔗[Toutes les versions](https://doris.apache.org/docs/releasenotes/all-release), où vous trouverez un résumé chronologique des versions d'Apache Doris publiées au cours de l'année écoulée.

👀 Explorez le 🔗[Site officiel](https://doris.apache.org/) pour découvrir en détail les fonctionnalités principales d'Apache Doris, les blogs et les cas d'utilisation.

## 📈 Scénarios d'utilisation

Comme le montre la figure ci-dessous, après diverses intégrations et traitements de données, les sources de données sont généralement stockées dans l'entrepôt de données en temps réel Apache Doris et le lac de données hors ligne ou l'entrepôt de données (dans Apache Hive, Apache Iceberg ou Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris est largement utilisé dans les scénarios suivants :

- **Analyse de données en temps réel** :

  - **Rapports et prise de décision en temps réel** : Doris fournit des rapports et tableaux de bord mis à jour en temps réel pour un usage interne et externe des entreprises, soutenant la prise de décision en temps réel dans les processus automatisés.
  
  - **Analyse ad-hoc** : Doris offre des capacités d'analyse de données multidimensionnelles, permettant une analyse rapide de l'intelligence d'affaires et des requêtes ad-hoc pour aider les utilisateurs à découvrir rapidement des insights à partir de données complexes.
  
  - **Profilage utilisateur et analyse comportementale** : Doris peut analyser les comportements des utilisateurs tels que la participation, la rétention et la conversion, tout en soutenant également des scénarios comme les insights démographiques et la sélection de groupes pour l'analyse comportementale.

- **Analyse de lac de données** :

  - **Accélération des requêtes de lac de données** : Doris accélère les requêtes de données du lac de données avec son moteur de requête efficace.
  
  - **Analyse fédérée** : Doris prend en charge les requêtes fédérées sur plusieurs sources de données, simplifiant l'architecture et éliminant les silos de données.
  
  - **Traitement de données en temps réel** : Doris combine les capacités de traitement des flux de données en temps réel et par lots pour répondre aux besoins de haute concurrence et de faible latence des exigences métier complexes.

- **Observabilité basée sur SQL** :

  - **Analyse des journaux et événements** : Doris permet l'analyse en temps réel ou par lots des journaux et événements dans les systèmes distribués, aidant à identifier les problèmes et à optimiser les performances.


## Architecture globale

Apache Doris utilise le protocole MySQL, est hautement compatible avec la syntaxe MySQL et prend en charge SQL standard. Les utilisateurs peuvent accéder à Apache Doris via divers outils clients, et il s'intègre de manière transparente avec les outils BI.

### Architecture intégrée stockage-calcul

L'architecture intégrée stockage-calcul d'Apache Doris est rationalisée et facile à maintenir. Comme le montre la figure ci-dessous, elle se compose de seulement deux types de processus :

- **Frontend (FE) :** Principalement responsable de la gestion des requêtes utilisateur, de l'analyse et de la planification des requêtes, de la gestion des métadonnées et des tâches de gestion des nœuds.

- **Backend (BE) :** Principalement responsable du stockage des données et de l'exécution des requêtes. Les données sont partitionnées en fragments et stockées avec plusieurs répliques sur les nœuds BE.

![L'architecture globale d'Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

Dans un environnement de production, plusieurs nœuds FE peuvent être déployés pour la récupération après sinistre. Chaque nœud FE maintient une copie complète des métadonnées. Les nœuds FE sont divisés en trois rôles :

| Rôle      | Fonction                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Le nœud FE Master est responsable des opérations de lecture et d'écriture des métadonnées. Lorsque des changements de métadonnées se produisent dans le Master, ils sont synchronisés vers les nœuds Follower ou Observer via le protocole BDB JE. |
| Follower  | Le nœud Follower est responsable de la lecture des métadonnées. Si le nœud Master échoue, un nœud Follower peut être sélectionné comme nouveau Master. |
| Observer  | Le nœud Observer est responsable de la lecture des métadonnées et est principalement utilisé pour augmenter la concurrence des requêtes. Il ne participe pas aux élections de leadership du cluster. |

Les processus FE et BE sont tous deux extensibles horizontalement, permettant à un seul cluster de supporter des centaines de machines et des dizaines de pétaoctets de capacité de stockage. Les processus FE et BE utilisent un protocole de cohérence pour assurer la haute disponibilité des services et la haute fiabilité des données. L'architecture intégrée stockage-calcul est hautement intégrée, réduisant considérablement la complexité opérationnelle des systèmes distribués.


## Fonctionnalités principales d'Apache Doris

- **Haute disponibilité** : Dans Apache Doris, les métadonnées et les données sont stockées avec plusieurs répliques, synchronisant les journaux de données via le protocole quorum. L'écriture de données est considérée comme réussie une fois qu'une majorité de répliques ont terminé l'écriture, garantissant que le cluster reste disponible même si quelques nœuds échouent. Apache Doris prend en charge la récupération après sinistre dans la même ville et entre régions, permettant des modes maître-esclave à double cluster. Lorsque certains nœuds subissent des défaillances, le cluster peut automatiquement isoler les nœuds défectueux, empêchant la disponibilité globale du cluster d'être affectée.

- **Haute compatibilité** : Apache Doris est hautement compatible avec le protocole MySQL et prend en charge la syntaxe SQL standard, couvrant la plupart des fonctions MySQL et Hive. Cette haute compatibilité permet aux utilisateurs de migrer et d'intégrer de manière transparente les applications et outils existants. Apache Doris prend en charge l'écosystème MySQL, permettant aux utilisateurs de connecter Doris en utilisant les outils client MySQL pour des opérations et une maintenance plus pratiques. Il prend également en charge la compatibilité du protocole MySQL pour les outils de reporting BI et les outils de transmission de données, garantissant l'efficacité et la stabilité dans les processus d'analyse de données et de transmission de données.

- **Entrepôt de données en temps réel** : Basé sur Apache Doris, un service d'entrepôt de données en temps réel peut être construit. Apache Doris offre des capacités d'ingestion de données au niveau de la seconde, capturant les changements incrémentiels des bases de données transactionnelles en ligne en amont dans Doris en quelques secondes. En tirant parti des moteurs vectorisés, de l'architecture MPP et des moteurs d'exécution Pipeline, Doris fournit des capacités de requête de données inférieures à la seconde, construisant ainsi une plateforme d'entrepôt de données en temps réel haute performance et à faible latence.

- **Lac de données unifié** : Apache Doris peut construire une architecture de lac de données unifié basée sur des sources de données externes telles que les lacs de données ou les bases de données relationnelles. La solution de lac de données unifié Doris permet une intégration transparente et un flux de données libre entre les lacs de données et les entrepôts de données, aidant les utilisateurs à utiliser directement les capacités de l'entrepôt de données pour résoudre les problèmes d'analyse de données dans les lacs de données tout en tirant pleinement parti des capacités de gestion des données du lac de données pour améliorer la valeur des données.

- **Modélisation flexible** : Apache Doris offre diverses approches de modélisation, telles que les modèles de table large, les modèles de pré-agrégation, les schémas étoile/flocon de neige, etc. Pendant l'importation de données, les données peuvent être aplaties en tables larges et écrites dans Doris via des moteurs de calcul comme Flink ou Spark, ou les données peuvent être directement importées dans Doris, effectuant des opérations de modélisation de données via des vues, des vues matérialisées ou des jointures multi-tables en temps réel.

## Aperçu technique

Doris fournit une interface SQL efficace et est entièrement compatible avec le protocole MySQL. Son moteur de requête est basé sur une architecture MPP (traitement massivement parallèle), capable d'exécuter efficacement des requêtes analytiques complexes et d'atteindre des requêtes en temps réel à faible latence. Grâce à la technologie de stockage en colonnes pour l'encodage et la compression des données, il optimise considérablement les performances des requêtes et le ratio de compression du stockage.

### Interface

Apache Doris adopte le protocole MySQL, prend en charge SQL standard et est hautement compatible avec la syntaxe MySQL. Les utilisateurs peuvent accéder à Apache Doris via divers outils clients et l'intégrer de manière transparente avec les outils BI, y compris mais sans s'y limiter Smartbi, DataEase, FineBI, Tableau, Power BI et Apache Superset. Apache Doris peut fonctionner comme source de données pour tout outil BI qui prend en charge le protocole MySQL.

### Moteur de stockage

Apache Doris dispose d'un moteur de stockage en colonnes, qui encode, compresse et lit les données par colonne. Cela permet un ratio de compression de données très élevé et réduit considérablement le balayage inutile des données, utilisant ainsi plus efficacement les ressources IO et CPU.

Apache Doris prend en charge diverses structures d'index pour minimiser les balayages de données :

- **Index de clé composée triée** : Les utilisateurs peuvent spécifier au maximum trois colonnes pour former une clé de tri composée. Cela peut efficacement élaguer les données pour mieux prendre en charge les scénarios de reporting hautement concurrents.

- **Index Min/Max** : Cela permet un filtrage efficace des données dans les requêtes d'équivalence et de plage de types numériques.

- **Index BloomFilter** : C'est très efficace dans le filtrage d'équivalence et l'élagage des colonnes à cardinalité élevée.

- **Index inversé** : Cela permet une recherche rapide pour n'importe quel champ.

Apache Doris prend en charge une variété de modèles de données et les a optimisés pour différents scénarios :

- **Modèle de détail (Modèle de clé dupliquée) :** Un modèle de données de détail conçu pour répondre aux exigences de stockage détaillé des tables de faits.

- **Modèle de clé primaire (Modèle de clé unique) :** Assure des clés uniques ; les données avec la même clé sont écrasées, permettant des mises à jour de données au niveau des lignes.

- **Modèle d'agrégation (Modèle de clé d'agrégation) :** Fusionne les colonnes de valeurs avec la même clé, améliorant considérablement les performances grâce à la pré-agrégation.

Apache Doris prend également en charge les vues matérialisées à table unique fortement cohérentes et les vues matérialisées multi-tables rafraîchies de manière asynchrone. Les vues matérialisées à table unique sont automatiquement rafraîchies et maintenues par le système, ne nécessitant aucune intervention manuelle des utilisateurs. Les vues matérialisées multi-tables peuvent être rafraîchies périodiquement en utilisant la planification dans le cluster ou des outils de planification externes, réduisant la complexité de la modélisation des données.

### 🔍 Moteur de requête

Apache Doris dispose d'un moteur de requête basé sur MPP pour l'exécution parallèle entre et dans les nœuds. Il prend en charge la jointure shuffle distribuée pour les grandes tables pour mieux gérer les requêtes compliquées.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Le moteur de requête d'Apache Doris est entièrement vectorisé, avec toutes les structures mémoire disposées dans un format en colonnes. Cela peut considérablement réduire les appels de fonction virtuels, augmenter les taux de réussite du cache et utiliser efficacement les instructions SIMD. Apache Doris offre des performances 5 à 10 fois supérieures dans les scénarios d'agrégation de table large par rapport aux moteurs non vectorisés.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris utilise la technologie d'exécution de requête adaptative pour ajuster dynamiquement le plan d'exécution en fonction des statistiques d'exécution. Par exemple, il peut générer un filtre d'exécution et le pousser vers le côté de la sonde. Spécifiquement, il pousse les filtres vers le nœud de balayage de niveau le plus bas du côté de la sonde, ce qui réduit considérablement la quantité de données à traiter et augmente les performances de jointure. Le filtre d'exécution d'Apache Doris prend en charge In/Min/Max/Bloom Filter.

Apache Doris utilise un moteur d'exécution Pipeline qui décompose les requêtes en plusieurs sous-tâches pour une exécution parallèle, tirant pleinement parti des capacités CPU multi-cœurs. Il résout simultanément le problème d'explosion des threads en limitant le nombre de threads de requête. Le moteur d'exécution Pipeline réduit la copie et le partage des données, optimise les opérations de tri et d'agrégation, améliorant ainsi considérablement l'efficacité et le débit des requêtes.

En termes d'optimiseur, Apache Doris emploie une stratégie d'optimisation combinée de CBO (Optimiseur basé sur les coûts), RBO (Optimiseur basé sur les règles) et HBO (Optimiseur basé sur l'historique). RBO prend en charge le pliage constant, la réécriture de sous-requête, la poussée de prédicat, et plus encore. CBO prend en charge le réordonnancement des jointures et d'autres optimisations. HBO recommande le plan d'exécution optimal basé sur les informations de requête historiques. Ces multiples mesures d'optimisation garantissent que Doris peut énumérer des plans de requête haute performance pour divers types de requêtes.


## 🎆 Pourquoi choisir Apache Doris ?

- 🎯 **Facile à utiliser** : Deux processus, aucune autre dépendance ; mise à l'échelle du cluster en ligne, récupération automatique des répliques ; compatible avec le protocole MySQL et utilisant SQL standard.

- 🚀 **Haute performance** : Performances extrêmement rapides pour les requêtes à faible latence et haut débit avec moteur de stockage en colonnes, architecture MPP moderne, moteur de requête vectorisé, vue matérialisée pré-agrégée et index de données.

- 🖥️ **Unifié unique** : Un seul système peut prendre en charge les scénarios de service de données en temps réel, d'analyse de données interactive et de traitement de données hors ligne.

- ⚛️ **Requête fédérée** : Prend en charge la requête fédérée des lacs de données tels que Hive, Iceberg, Hudi, et des bases de données telles que MySQL et Elasticsearch.

- ⏩ **Diverses méthodes d'importation de données** : Prend en charge l'importation par lots depuis HDFS/S3 et l'importation de flux depuis MySQL Binlog/Kafka ; prend en charge l'écriture par micro-lots via l'interface HTTP et l'écriture en temps réel en utilisant Insert dans JDBC.

- 🚙 **Écosystème riche** : Spark utilise Spark-Doris-Connector pour lire et écrire Doris ; Flink-Doris-Connector permet à Flink CDC d'implémenter une écriture de données exactement une fois vers Doris ; DBT Doris Adapter est fourni pour transformer les données dans Doris avec DBT.

## 🙌 Contributeurs

**Apache Doris a obtenu son diplôme de l'incubateur Apache avec succès et est devenu un projet de niveau supérieur en juin 2022**.

Nous apprécions profondément les 🔗[contributeurs de la communauté](https://github.com/apache/doris/graphs/contributors) pour leur contribution à Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Utilisateurs

Apache Doris a maintenant une large base d'utilisateurs en Chine et dans le monde entier, et à ce jour, **Apache Doris est utilisé dans des environnements de production dans des milliers d'entreprises dans le monde entier.** Plus de 80% des 50 premières entreprises Internet en Chine en termes de capitalisation boursière ou d'évaluation utilisent Apache Doris depuis longtemps, notamment Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo et Ke Holdings. Il est également largement utilisé dans certaines industries traditionnelles telles que la finance, l'énergie, la fabrication et les télécommunications.

Les utilisateurs d'Apache Doris : 🔗[Utilisateurs](https://doris.apache.org/users)

Ajoutez le logo de votre entreprise sur le site Web d'Apache Doris : 🔗[Ajouter votre entreprise](https://github.com/apache/doris/discussions/27683)
 
## 👣 Pour commencer

### 📚 Documentation

Toute la documentation   🔗[Documentation](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Téléchargement 

Toutes les versions de release et binaires 🔗[Téléchargement](https://doris.apache.org/download) 

### 🗄️ Compilation

Voir comment compiler  🔗[Compilation](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Installation

Voir comment installer et déployer 🔗[Installation et déploiement](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Composants

### 📝 Doris Connector

Doris fournit un support pour Spark/Flink pour lire les données stockées dans Doris via Connector, et prend également en charge l'écriture de données dans Doris via Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Communauté et support

### 📤 S'abonner aux listes de diffusion

La liste de diffusion est la forme de communication la plus reconnue dans la communauté Apache. Voir comment 🔗[S'abonner aux listes de diffusion](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Signaler des problèmes ou soumettre une Pull Request

Si vous rencontrez des questions, n'hésitez pas à déposer une 🔗[GitHub Issue](https://github.com/apache/doris/issues) ou à la publier dans 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) et à la corriger en soumettant une 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Comment contribuer

Nous accueillons vos suggestions, commentaires (y compris les critiques), commentaires et contributions. Voir 🔗[Comment contribuer](https://doris.apache.org/community/how-to-contribute/) et 🔗[Guide de soumission de code](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Propositions d'amélioration Doris (DSIP)

🔗[Proposition d'amélioration Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) peut être considérée comme **Une collection de documents de conception pour toutes les mises à jour ou améliorations majeures de fonctionnalités**.

### 🔑 Spécification de codage Backend C++
🔗 [Spécification de codage Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) doit être strictement suivie, ce qui nous aidera à atteindre une meilleure qualité de code.

## 💬 Contactez-nous

Contactez-nous via la liste de diffusion suivante.

| Nom                                                                          | Portée                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Discussions liées au développement | [S'abonner](mailto:dev-subscribe@doris.apache.org)   | [Se désabonner](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Liens

* Site Web officiel d'Apache Doris - [Site](https://doris.apache.org)
* Liste de diffusion des développeurs - <dev@doris.apache.org>. Envoyez un e-mail à <dev-subscribe@doris.apache.org>, suivez la réponse pour vous abonner à la liste de diffusion.
* Canal Slack - [Rejoindre Slack](https://doris.apache.org/slack)
* Twitter - [Suivre @doris_apache](https://twitter.com/doris_apache)


## 📜 Licence

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Note**
> Certaines licences des dépendances tierces ne sont pas compatibles avec la licence Apache 2.0. Vous devez donc désactiver
certaines fonctionnalités Doris pour être conforme à la licence Apache 2.0. Pour plus de détails, reportez-vous au fichier `thirdparty/LICENSE.txt`



