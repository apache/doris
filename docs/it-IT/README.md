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

## 🌍 Leggi in altre lingue

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris è un database analitico facile da usare, ad alte prestazioni e in tempo reale basato sull'architettura MPP, noto per la sua velocità estrema e facilità d'uso. Richiede solo un tempo di risposta inferiore al secondo per restituire risultati di query con dati massicci e può supportare non solo scenari di query puntuali ad alta concorrenza ma anche scenari di analisi complessa ad alto throughput.

Tutto ciò rende Apache Doris uno strumento ideale per scenari che includono analisi dei report, query ad-hoc, data warehouse unificato e accelerazione delle query del data lake. Su Apache Doris, gli utenti possono costruire varie applicazioni, come analisi del comportamento degli utenti, piattaforma di test AB, analisi di recupero dei log, analisi del profilo utente e analisi degli ordini.

🎉 Controlla 🔗[Tutte le versioni](https://doris.apache.org/docs/releasenotes/all-release), dove troverai un riepilogo cronologico delle versioni di Apache Doris rilasciate nell'ultimo anno.

👀 Esplora il 🔗[Sito web ufficiale](https://doris.apache.org/) per scoprire in dettaglio le funzionalità principali, i blog e i casi d'uso di Apache Doris.

## 📈 Scenari d'uso

Come mostrato nella figura seguente, dopo varie integrazioni e elaborazioni dei dati, le sorgenti di dati sono generalmente memorizzate nel data warehouse in tempo reale Apache Doris e nel data lake offline o nel data warehouse (in Apache Hive, Apache Iceberg o Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris è ampiamente utilizzato nei seguenti scenari:

- **Analisi dei dati in tempo reale**:

  - **Reporting e decision making in tempo reale**: Doris fornisce report e dashboard aggiornati in tempo reale per uso aziendale sia interno che esterno, supportando il processo decisionale in tempo reale nei processi automatizzati.
  
  - **Analisi ad-hoc**: Doris offre capacità di analisi dei dati multidimensionali, consentendo analisi rapide di business intelligence e query ad-hoc per aiutare gli utenti a scoprire rapidamente informazioni da dati complessi.
  
  - **Profilazione utente e analisi del comportamento**: Doris può analizzare comportamenti degli utenti come partecipazione, retention e conversione, supportando anche scenari come informazioni demografiche e selezione di gruppi per l'analisi del comportamento.

- **Analisi del data lake**:

  - **Accelerazione delle query del data lake**: Doris accelera le query dei dati del data lake con il suo motore di query efficiente.
  
  - **Analisi federata**: Doris supporta query federate su più sorgenti di dati, semplificando l'architettura ed eliminando i silos di dati.
  
  - **Elaborazione dei dati in tempo reale**: Doris combina capacità di elaborazione di flussi di dati in tempo reale e batch per soddisfare le esigenze di alta concorrenza e bassa latenza di requisiti aziendali complessi.

- **Osservabilità basata su SQL**:

  - **Analisi di log ed eventi**: Doris consente l'analisi in tempo reale o batch di log ed eventi nei sistemi distribuiti, aiutando a identificare problemi e ottimizzare le prestazioni.


## Architettura complessiva

Apache Doris utilizza il protocollo MySQL, è altamente compatibile con la sintassi MySQL e supporta SQL standard. Gli utenti possono accedere ad Apache Doris tramite vari strumenti client e si integra perfettamente con gli strumenti BI.

### Architettura integrata storage-computing

L'architettura integrata storage-computing di Apache Doris è semplificata e facile da mantenere. Come mostrato nella figura seguente, consiste solo di due tipi di processi:

- **Frontend (FE):** Principalmente responsabile della gestione delle richieste degli utenti, del parsing e della pianificazione delle query, della gestione dei metadati e delle attività di gestione dei nodi.

- **Backend (BE):** Principalmente responsabile dello storage dei dati e dell'esecuzione delle query. I dati sono partizionati in shard e memorizzati con più repliche attraverso i nodi BE.

![L'architettura complessiva di Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

In un ambiente di produzione, possono essere distribuiti più nodi FE per il disaster recovery. Ogni nodo FE mantiene una copia completa dei metadati. I nodi FE sono divisi in tre ruoli:

| Ruolo      | Funzione                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Il nodo FE Master è responsabile delle operazioni di lettura e scrittura dei metadati. Quando si verificano modifiche ai metadati nel Master, vengono sincronizzate ai nodi Follower o Observer tramite il protocollo BDB JE. |
| Follower  | Il nodo Follower è responsabile della lettura dei metadati. Se il nodo Master fallisce, un nodo Follower può essere selezionato come nuovo Master. |
| Observer  | Il nodo Observer è responsabile della lettura dei metadati ed è principalmente utilizzato per aumentare la concorrenza delle query. Non partecipa alle elezioni di leadership del cluster. |

Sia i processi FE che BE sono scalabili orizzontalmente, consentendo a un singolo cluster di supportare centinaia di macchine e decine di petabyte di capacità di storage. I processi FE e BE utilizzano un protocollo di consistenza per garantire l'alta disponibilità dei servizi e l'alta affidabilità dei dati. L'architettura integrata storage-computing è altamente integrata, riducendo significativamente la complessità operativa dei sistemi distribuiti.


## Caratteristiche principali di Apache Doris

- **Alta disponibilità**: In Apache Doris, sia i metadati che i dati sono memorizzati con più repliche, sincronizzando i log dei dati tramite il protocollo quorum. La scrittura dei dati è considerata riuscita una volta che la maggioranza delle repliche ha completato la scrittura, garantendo che il cluster rimanga disponibile anche se alcuni nodi falliscono. Apache Doris supporta sia il disaster recovery nella stessa città che tra regioni, abilitando modalità master-slave a doppio cluster. Quando alcuni nodi subiscono guasti, il cluster può isolare automaticamente i nodi difettosi, impedendo che la disponibilità complessiva del cluster venga influenzata.

- **Alta compatibilità**: Apache Doris è altamente compatibile con il protocollo MySQL e supporta la sintassi SQL standard, coprendo la maggior parte delle funzioni MySQL e Hive. Questa alta compatibilità consente agli utenti di migrare e integrare facilmente applicazioni e strumenti esistenti. Apache Doris supporta l'ecosistema MySQL, consentendo agli utenti di connettersi a Doris utilizzando strumenti client MySQL per operazioni e manutenzione più convenienti. Supporta anche la compatibilità del protocollo MySQL per strumenti di reporting BI e strumenti di trasmissione dati, garantendo efficienza e stabilità nei processi di analisi dei dati e trasmissione dei dati.

- **Data warehouse in tempo reale**: Basato su Apache Doris, può essere costruito un servizio di data warehouse in tempo reale. Apache Doris offre capacità di acquisizione dati a livello di secondo, catturando cambiamenti incrementali da database transazionali online upstream in Doris entro secondi. Sfruttando motori vettorizzati, architettura MPP e motori di esecuzione Pipeline, Doris fornisce capacità di query dei dati inferiori al secondo, costruendo così una piattaforma di data warehouse in tempo reale ad alte prestazioni e bassa latenza.

- **Data lake unificato**: Apache Doris può costruire un'architettura di data lake unificata basata su sorgenti di dati esterne come data lake o database relazionali. La soluzione di data lake unificata di Doris consente un'integrazione perfetta e un flusso libero di dati tra data lake e data warehouse, aiutando gli utenti a utilizzare direttamente le capacità del data warehouse per risolvere problemi di analisi dei dati nei data lake mentre sfruttano appieno le capacità di gestione dei dati del data lake per migliorare il valore dei dati.

- **Modellazione flessibile**: Apache Doris offre vari approcci di modellazione, come modelli di tabelle larghe, modelli di pre-aggregazione, schemi stella/fiocco di neve, ecc. Durante l'importazione dei dati, i dati possono essere appiattiti in tabelle larghe e scritti in Doris tramite motori di calcolo come Flink o Spark, oppure i dati possono essere importati direttamente in Doris, eseguendo operazioni di modellazione dei dati tramite viste, viste materializzate o join multi-tabella in tempo reale.

## Panoramica tecnica

Doris fornisce un'interfaccia SQL efficiente ed è completamente compatibile con il protocollo MySQL. Il suo motore di query è basato su un'architettura MPP (elaborazione massivamente parallela), capace di eseguire efficientemente query analitiche complesse e raggiungere query in tempo reale a bassa latenza. Attraverso la tecnologia di storage colonnare per la codifica e compressione dei dati, ottimizza significativamente le prestazioni delle query e il rapporto di compressione dello storage.

### Interfaccia

Apache Doris adotta il protocollo MySQL, supporta SQL standard ed è altamente compatibile con la sintassi MySQL. Gli utenti possono accedere ad Apache Doris tramite vari strumenti client e integrarlo perfettamente con strumenti BI, inclusi ma non limitati a Smartbi, DataEase, FineBI, Tableau, Power BI e Apache Superset. Apache Doris può funzionare come sorgente di dati per qualsiasi strumento BI che supporta il protocollo MySQL.

### Motore di storage

Apache Doris ha un motore di storage colonnare, che codifica, comprime e legge i dati per colonna. Ciò consente un rapporto di compressione dei dati molto elevato e riduce notevolmente la scansione non necessaria dei dati, utilizzando così in modo più efficiente le risorse IO e CPU.

Apache Doris supporta varie strutture di indice per minimizzare le scansioni dei dati:

- **Indice chiave composta ordinata**: Gli utenti possono specificare al massimo tre colonne per formare una chiave di ordinamento composta. Ciò può potare efficacemente i dati per supportare meglio scenari di reporting altamente concorrenti.

- **Indice Min/Max**: Ciò consente un filtraggio efficace dei dati nelle query di equivalenza e intervallo di tipi numerici.

- **Indice BloomFilter**: Ciò è molto efficace nel filtraggio di equivalenza e nella potatura di colonne ad alta cardinalità.

- **Indice invertito**: Ciò consente ricerche rapide per qualsiasi campo.

Apache Doris supporta una varietà di modelli di dati e li ha ottimizzati per diversi scenari:

- **Modello dettaglio (Modello chiave duplicata):** Un modello di dati dettaglio progettato per soddisfare i requisiti di storage dettagliati delle tabelle dei fatti.

- **Modello chiave primaria (Modello chiave unica):** Garantisce chiavi univoche; i dati con la stessa chiave vengono sovrascritti, consentendo aggiornamenti dei dati a livello di riga.

- **Modello aggregazione (Modello chiave aggregazione):** Unisce colonne di valori con la stessa chiave, migliorando significativamente le prestazioni attraverso la pre-aggregazione.

Apache Doris supporta anche viste materializzate a tabella singola fortemente consistenti e viste materializzate multi-tabella aggiornate in modo asincrono. Le viste materializzate a tabella singola vengono aggiornate e mantenute automaticamente dal sistema, senza richiedere intervento manuale degli utenti. Le viste materializzate multi-tabella possono essere aggiornate periodicamente utilizzando la pianificazione all'interno del cluster o strumenti di pianificazione esterni, riducendo la complessità della modellazione dei dati.

### 🔍 Motore di query

Apache Doris ha un motore di query basato su MPP per l'esecuzione parallela tra e all'interno dei nodi. Supporta join shuffle distribuito per tabelle grandi per gestire meglio query complicate.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Il motore di query di Apache Doris è completamente vettorizzato, con tutte le strutture di memoria disposte in un formato colonnare. Ciò può ridurre notevolmente le chiamate di funzione virtuale, aumentare i tassi di hit della cache e fare un uso efficiente delle istruzioni SIMD. Apache Doris offre prestazioni da 5 a 10 volte superiori negli scenari di aggregazione di tabelle larghe rispetto ai motori non vettorizzati.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris utilizza la tecnologia di esecuzione query adattiva per regolare dinamicamente il piano di esecuzione basato su statistiche di runtime. Ad esempio, può generare un filtro runtime e spingerlo sul lato della sonda. Specificamente, spinge i filtri al nodo di scansione di livello più basso sul lato della sonda, il che riduce notevolmente la quantità di dati da elaborare e aumenta le prestazioni del join. Il filtro runtime di Apache Doris supporta In/Min/Max/Bloom Filter.

Apache Doris utilizza un motore di esecuzione Pipeline che scompone le query in più sotto-attività per l'esecuzione parallela, sfruttando appieno le capacità della CPU multi-core. Affronta simultaneamente il problema dell'esplosione dei thread limitando il numero di thread di query. Il motore di esecuzione Pipeline riduce la copia e la condivisione dei dati, ottimizza le operazioni di ordinamento e aggregazione, migliorando così significativamente l'efficienza e il throughput delle query.

In termini di ottimizzatore, Apache Doris impiega una strategia di ottimizzazione combinata di CBO (ottimizzatore basato sui costi), RBO (ottimizzatore basato su regole) e HBO (ottimizzatore basato sulla storia). RBO supporta il folding costante, la riscrittura delle sottoquery, il pushdown dei predicati e altro ancora. CBO supporta il riordino dei join e altre ottimizzazioni. HBO raccomanda il piano di esecuzione ottimale basato su informazioni di query storiche. Queste multiple misure di ottimizzazione assicurano che Doris possa enumerare piani di query ad alte prestazioni per vari tipi di query.


## 🎆 Perché scegliere Apache Doris?

- 🎯 **Facile da usare**: Due processi, nessun'altra dipendenza; scalabilità del cluster online, recupero automatico delle repliche; compatibile con il protocollo MySQL e utilizzando SQL standard.

- 🚀 **Alte prestazioni**: Prestazioni estremamente veloci per query a bassa latenza e alto throughput con motore di storage colonnare, architettura MPP moderna, motore di query vettorizzato, vista materializzata pre-aggregata e indice dei dati.

- 🖥️ **Unificato singolo**: Un singolo sistema può supportare scenari di servizio dati in tempo reale, analisi dati interattiva ed elaborazione dati offline.

- ⚛️ **Query federate**: Supporta query federate di data lake come Hive, Iceberg, Hudi e database come MySQL ed Elasticsearch.

- ⏩ **Vari metodi di importazione dati**: Supporta l'importazione batch da HDFS/S3 e l'importazione stream da MySQL Binlog/Kafka; supporta la scrittura micro-batch tramite interfaccia HTTP e la scrittura in tempo reale utilizzando Insert in JDBC.

- 🚙 **Ecologia ricca**: Spark utilizza Spark-Doris-Connector per leggere e scrivere Doris; Flink-Doris-Connector consente a Flink CDC di implementare la scrittura dei dati esattamente una volta in Doris; DBT Doris Adapter è fornito per trasformare i dati in Doris con DBT.

## 🙌 Contributori

**Apache Doris si è diplomato con successo dall'incubatore Apache ed è diventato un progetto di livello superiore nel giugno 2022**.

Apprezziamo profondamente i 🔗[contributori della comunità](https://github.com/apache/doris/graphs/contributors) per il loro contributo ad Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Utenti

Apache Doris ora ha una vasta base di utenti in Cina e in tutto il mondo, e ad oggi, **Apache Doris è utilizzato in ambienti di produzione in migliaia di aziende in tutto il mondo.** Più dell'80% delle prime 50 aziende Internet in Cina in termini di capitalizzazione di mercato o valutazione utilizzano Apache Doris da molto tempo, inclusi Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo e Ke Holdings. È anche ampiamente utilizzato in alcune industrie tradizionali come finanza, energia, manifattura e telecomunicazioni.

Gli utenti di Apache Doris: 🔗[Utenti](https://doris.apache.org/users)

Aggiungi il logo della tua azienda sul sito web di Apache Doris: 🔗[Aggiungi la tua azienda](https://github.com/apache/doris/discussions/27683)
 
## 👣 Inizia

### 📚 Documentazione

Tutta la documentazione   🔗[Documentazione](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Download 

Tutte le versioni di release e binarie 🔗[Download](https://doris.apache.org/download) 

### 🗄️ Compilazione

Vedi come compilare  🔗[Compilazione](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Installazione

Vedi come installare e distribuire 🔗[Installazione e distribuzione](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Componenti

### 📝 Doris Connector

Doris fornisce supporto per Spark/Flink per leggere dati memorizzati in Doris tramite Connector e supporta anche la scrittura di dati in Doris tramite Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Comunità e supporto

### 📤 Iscriviti alle mailing list

La mailing list è la forma di comunicazione più riconosciuta nella comunità Apache. Vedi come 🔗[Iscriviti alle mailing list](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Segnala problemi o invia Pull Request

Se hai domande, sentiti libero di presentare un 🔗[GitHub Issue](https://github.com/apache/doris/issues) o pubblicarlo in 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) e correggerlo inviando un 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Come contribuire

Accogliamo con favore i tuoi suggerimenti, commenti (inclusi critiche), commenti e contributi. Vedi 🔗[Come contribuire](https://doris.apache.org/community/how-to-contribute/) e 🔗[Guida all'invio del codice](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Proposte di miglioramento Doris (DSIP)

🔗[Proposta di miglioramento Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) può essere pensata come **Una collezione di documenti di progettazione per tutti gli aggiornamenti o miglioramenti principali delle funzionalità**.

### 🔑 Specifica di codifica Backend C++
🔗 [Specifica di codifica Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) dovrebbe essere rigorosamente seguita, il che ci aiuterà a raggiungere una migliore qualità del codice.

## 💬 Contattaci

Contattaci tramite la seguente mailing list.

| Nome                                                                          | Ambito                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Discussioni relative allo sviluppo | [Iscriviti](mailto:dev-subscribe@doris.apache.org)   | [Annulla iscrizione](mailto:dev-unsubscribe@doris.apache.org)   | [Archivi](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Link

* Sito web ufficiale di Apache Doris - [Sito](https://doris.apache.org)
* Mailing list degli sviluppatori - <dev@doris.apache.org>. Invia un'email a <dev-subscribe@doris.apache.org>, segui la risposta per iscriverti alla mailing list.
* Canale Slack - [Unisciti a Slack](https://doris.apache.org/slack)
* Twitter - [Segui @doris_apache](https://twitter.com/doris_apache)


## 📜 Licenza

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Nota**
> Alcune licenze delle dipendenze di terze parti non sono compatibili con la licenza Apache 2.0. Quindi è necessario disabilitare
alcune funzionalità di Doris per essere conformi alla licenza Apache 2.0. Per i dettagli, fare riferimento al file `thirdparty/LICENSE.txt`


