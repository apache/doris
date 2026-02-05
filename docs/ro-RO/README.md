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

## 🌍 Citește în alte limbi

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris este o bază de date analitică ușor de utilizat, de înaltă performanță și în timp real bazată pe arhitectura MPP, cunoscută pentru viteza sa extremă și ușurința de utilizare. Necesită doar un timp de răspuns sub secundă pentru a returna rezultatele interogărilor sub date masive și poate suporta nu doar scenarii de interogare punctuală de concurență ridicată, ci și scenarii de analiză complexă de throughput ridicat.

Toate acestea fac Apache Doris un instrument ideal pentru scenarii care includ analiza rapoartelor, interogarea ad-hoc, depozitul de date unificat și accelerarea interogărilor lacului de date. Pe Apache Doris, utilizatorii pot construi diverse aplicații, cum ar fi analiza comportamentului utilizatorilor, platforma de testare AB, analiza recuperării jurnalelor, analiza profilului utilizatorului și analiza comenzilor.

🎉 Consultați 🔗[Toate versiunile](https://doris.apache.org/docs/releasenotes/all-release), unde veți găsi un rezumat cronologic al versiunilor Apache Doris lansate în ultimul an.

👀 Explorați 🔗[Site-ul oficial](https://doris.apache.org/) pentru a descoperi în detaliu caracteristicile principale, blogurile și cazurile de utilizare ale Apache Doris.

## 📈 Scenarii de utilizare

După cum se arată în figura de mai jos, după diverse integrări și procesări de date, sursele de date sunt de obicei stocate în depozitul de date în timp real Apache Doris și lacul de date offline sau depozitul de date (în Apache Hive, Apache Iceberg sau Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris este utilizat pe scară largă în următoarele scenarii:

- **Analiza datelor în timp real**:

  - **Raportare și luare de decizii în timp real**: Doris oferă rapoarte și panouri actualizate în timp real pentru utilizarea întreprinderilor atât interne, cât și externe, susținând luarea de decizii în timp real în procesele automatizate.
  
  - **Analiza ad-hoc**: Doris oferă capacități de analiză multidimensională a datelor, permițând analize rapide de business intelligence și interogări ad-hoc pentru a ajuta utilizatorii să descopere rapid informații din date complexe.
  
  - **Profilarea utilizatorilor și analiza comportamentului**: Doris poate analiza comportamentele utilizatorilor, cum ar fi participarea, retenția și conversia, în timp ce sprijină, de asemenea, scenarii precum informațiile despre populație și selecția grupului pentru analiza comportamentului.

- **Analiza lacului de date**:

  - **Accelerarea interogărilor lacului de date**: Doris accelerează interogările datelor lacului de date cu motorul său eficient de interogare.
  
  - **Analiza federată**: Doris suportă interogări federate pe mai multe surse de date, simplificând arhitectura și eliminând silozurile de date.
  
  - **Procesarea datelor în timp real**: Doris combină capacitățile de procesare a fluxurilor de date în timp real și procesarea în loturi pentru a satisface nevoile de concurență ridicată și latență scăzută ale cerințelor de afaceri complexe.

- **Observabilitate bazată pe SQL**:

  - **Analiza jurnalelor și evenimentelor**: Doris permite analiza în timp real sau în loturi a jurnalelor și evenimentelor în sistemele distribuite, ajutând la identificarea problemelor și optimizarea performanței.


## Arhitectura generală

Apache Doris folosește protocolul MySQL, este foarte compatibil cu sintaxa MySQL și suportă SQL standard. Utilizatorii pot accesa Apache Doris prin diverse instrumente client și se integrează perfect cu instrumentele BI.

### Arhitectura integrată de stocare și calcul

Arhitectura integrată de stocare și calcul a Apache Doris este simplificată și ușor de întreținut. După cum se arată în figura de mai jos, constă doar din două tipuri de procese:

- **Frontend (FE):** Responsabil în principal pentru gestionarea cererilor utilizatorilor, parsarea și planificarea interogărilor, gestionarea metadatelor și sarcinile de gestionare a nodurilor.

- **Backend (BE):** Responsabil în principal pentru stocarea datelor și execuția interogărilor. Datele sunt partiționate în fragmente și stocate cu mai multe replici pe nodurile BE.

![Arhitectura generală a Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

Într-un mediu de producție, pot fi implementate mai multe noduri FE pentru recuperarea în caz de dezastru. Fiecare nod FE menține o copie completă a metadatelor. Nodurile FE sunt împărțite în trei roluri:

| Rol      | Funcție                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Nodul FE Master este responsabil pentru operațiunile de citire și scriere a metadatelor. Când apar modificări ale metadatelor în Master, acestea sunt sincronizate cu nodurile Follower sau Observer prin protocolul BDB JE. |
| Follower  | Nodul Follower este responsabil pentru citirea metadatelor. Dacă nodul Master eșuează, un nod Follower poate fi selectat ca noul Master. |
| Observer  | Nodul Observer este responsabil pentru citirea metadatelor și este folosit în principal pentru a crește concurența interogărilor. Nu participă la alegerile de conducere a clusterului. |

Atât procesele FE, cât și BE sunt scalabile orizontal, permițând unui singur cluster să suporte sute de mașini și zeci de petabaiți de capacitate de stocare. Procesele FE și BE folosesc un protocol de consistență pentru a asigura disponibilitatea ridicată a serviciilor și fiabilitatea ridicată a datelor. Arhitectura integrată de stocare și calcul este foarte integrată, reducând semnificativ complexitatea operațională a sistemelor distribuite.


## Caracteristicile principale ale Apache Doris

- **Disponibilitate ridicată**: În Apache Doris, atât metadatele, cât și datele sunt stocate cu mai multe replici, sincronizând jurnalele de date prin protocolul quorum. Scrierea datelor este considerată reușită odată ce majoritatea replicilor au finalizat scrierea, asigurând că clusterul rămâne disponibil chiar dacă câteva noduri eșuează. Apache Doris suportă recuperarea în caz de dezastru atât în același oraș, cât și între regiuni, permițând moduri master-slave de cluster dublu. Când unele noduri întâmpină defecțiuni, clusterul poate izola automat nodurile defecte, prevenind ca disponibilitatea generală a clusterului să fie afectată.

- **Compatibilitate ridicată**: Apache Doris este foarte compatibil cu protocolul MySQL și suportă sintaxa SQL standard, acoperind majoritatea funcțiilor MySQL și Hive. Această compatibilitate ridicată permite utilizatorilor să migreze și să integreze fără probleme aplicațiile și instrumentele existente. Apache Doris suportă ecosistemul MySQL, permițând utilizatorilor să se conecteze la Doris folosind instrumente client MySQL pentru operațiuni și întreținere mai convenabile. De asemenea, suportă compatibilitatea protocolului MySQL pentru instrumentele de raportare BI și instrumentele de transmisie a datelor, asigurând eficiență și stabilitate în procesele de analiză a datelor și transmisie a datelor.

- **Depozit de date în timp real**: Pe baza Apache Doris, poate fi construit un serviciu de depozit de date în timp real. Apache Doris oferă capacități de ingestie a datelor la nivel de secundă, capturând modificări incrementale din bazele de date transacționale online upstream în Doris în câteva secunde. Folosind motoare vectorizate, arhitectura MPP și motoarele de execuție Pipeline, Doris oferă capacități de interogare a datelor sub secundă, construind astfel o platformă de depozit de date în timp real de înaltă performanță și latență scăzută.

- **Lac de date unificat**: Apache Doris poate construi o arhitectură de lac de date unificată bazată pe surse de date externe, cum ar fi lacuri de date sau baze de date relaționale. Soluția de lac de date unificată Doris permite integrare perfectă și flux liber de date între lacuri de date și depozite de date, ajutând utilizatorii să utilizeze direct capacitățile depozitului de date pentru a rezolva problemele de analiză a datelor în lacurile de date, în timp ce valorifică pe deplin capacitățile de management al datelor lacului de date pentru a spori valoarea datelor.

- **Modelare flexibilă**: Apache Doris oferă diverse abordări de modelare, cum ar fi modelele de tabel larg, modelele de pre-agregare, schemele stea/fulg de zăpadă etc. În timpul importului de date, datele pot fi aplatizate în tabele largi și scrise în Doris prin motoare de calcul precum Flink sau Spark, sau datele pot fi importate direct în Doris, efectuând operațiuni de modelare a datelor prin vizualizări, vizualizări materializate sau îmbinări multi-tabel în timp real.

## Prezentare generală tehnică

Doris oferă o interfață SQL eficientă și este complet compatibil cu protocolul MySQL. Motorul său de interogare se bazează pe o arhitectură MPP (procesare paralelă masivă), capabilă să execute eficient interogări analitice complexe și să realizeze interogări în timp real cu latență scăzută. Prin tehnologia de stocare pe coloane pentru codificarea și comprimarea datelor, optimizează semnificativ performanța interogărilor și raportul de compresie a stocării.

### Interfață

Apache Doris adoptă protocolul MySQL, suportă SQL standard și este foarte compatibil cu sintaxa MySQL. Utilizatorii pot accesa Apache Doris prin diverse instrumente client și îl pot integra perfect cu instrumentele BI, inclusiv, dar fără a se limita la Smartbi, DataEase, FineBI, Tableau, Power BI și Apache Superset. Apache Doris poate funcționa ca sursă de date pentru orice instrumente BI care suportă protocolul MySQL.

### Motor de stocare

Apache Doris are un motor de stocare pe coloane, care codifică, comprimă și citește datele pe coloană. Acest lucru permite un raport de compresie a datelor foarte ridicat și reduce în mare măsură scanarea inutilă a datelor, făcând astfel un utilizare mai eficientă a resurselor IO și CPU.

Apache Doris suportă diverse structuri de index pentru a minimiza scanările de date:

- **Index de cheie compusă sortată**: Utilizatorii pot specifica cel mult trei coloane pentru a forma o cheie de sortare compusă. Acest lucru poate tăia eficient datele pentru a sprijini mai bine scenariile de raportare foarte concurente.

- **Index Min/Max**: Acest lucru permite filtrarea eficientă a datelor în interogările de echivalență și interval de tipuri numerice.

- **Index BloomFilter**: Acest lucru este foarte eficient în filtrarea de echivalență și tăierea coloanelor de cardinalitate ridicată.

- **Index inversat**: Acest lucru permite căutări rapide pentru orice câmp.

Apache Doris suportă o varietate de modele de date și le-a optimizat pentru diferite scenarii:

- **Model de detaliu (Model de cheie duplicată):** Un model de date de detaliu proiectat pentru a satisface cerințele detaliate de stocare ale tabelelor de fapte.

- **Model de cheie primară (Model de cheie unică):** Asigură chei unice; datele cu aceeași cheie sunt suprascrise, permițând actualizări ale datelor la nivel de rând.

- **Model de agregare (Model de cheie de agregare):** Fuzionează coloanele de valori cu aceeași cheie, îmbunătățind semnificativ performanța prin pre-agregare.

Apache Doris suportă, de asemenea, vizualizări materializate de tabel unic puternic consistente și vizualizări materializate multi-tabel actualizate asincron. Vizualizările materializate de tabel unic sunt actualizate și întreținute automat de sistem, fără a necesita intervenție manuală de la utilizatori. Vizualizările materializate multi-tabel pot fi actualizate periodic folosind programarea în cluster sau instrumente de programare externe, reducând complexitatea modelării datelor.

### 🔍 Motor de interogare

Apache Doris are un motor de interogare bazat pe MPP pentru execuție paralelă între noduri și în interiorul nodurilor. Suportă îmbinarea shuffle distribuită pentru tabele mari pentru a gestiona mai bine interogările complicate.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Motorul de interogare al Apache Doris este complet vectorizat, cu toate structurile de memorie așezate într-un format pe coloane. Acest lucru poate reduce în mare măsură apelurile de funcții virtuale, crește ratele de lovire a cache-ului și face un utilizare eficientă a instrucțiunilor SIMD. Apache Doris oferă o performanță de 5-10 ori mai mare în scenariile de agregare a tabelelor largi decât motoarele nevectorizate.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris folosește tehnologia de execuție adaptivă a interogărilor pentru a ajusta dinamic planul de execuție pe baza statisticilor de timp de execuție. De exemplu, poate genera un filtru de timp de execuție și îl poate împinge pe partea probei. Specific, împinge filtrele către nodul de scanare de cel mai mic nivel pe partea probei, ceea ce reduce în mare măsură cantitatea de date de procesat și crește performanța îmbinării. Filtru de timp de execuție al Apache Doris suportă In/Min/Max/Bloom Filter.

Apache Doris folosește un motor de execuție Pipeline care descompune interogările în mai multe sub-sarcini pentru execuție paralelă, valorificând pe deplin capacitățile CPU multi-core. Abordează simultan problema exploziei de fire limitând numărul de fire de interogare. Motorul de execuție Pipeline reduce copierea și partajarea datelor, optimizează operațiunile de sortare și agregare, îmbunătățind astfel semnificativ eficiența și throughput-ul interogărilor.

În ceea ce privește optimizatorul, Apache Doris folosește o strategie de optimizare combinată a CBO (optimizator bazat pe cost), RBO (optimizator bazat pe reguli) și HBO (optimizator bazat pe istoric). RBO suportă plierea constantă, rescrierea subinterogărilor, împingerea predicatelor și multe altele. CBO suportă reordonarea îmbinărilor și alte optimizări. HBO recomandă planul de execuție optim pe baza informațiilor istorice despre interogări. Aceste măsuri multiple de optimizare asigură că Doris poate enumera planuri de interogare de înaltă performanță pentru diferite tipuri de interogări.


## 🎆 De ce să alegi Apache Doris?

- 🎯 **Ușor de utilizat**: Două procese, fără alte dependențe; scalare cluster online, recuperare automată a replicilor; compatibil cu protocolul MySQL și folosind SQL standard.

- 🚀 **Performanță ridicată**: Performanță extrem de rapidă pentru interogări cu latență scăzută și throughput ridicat cu motor de stocare pe coloane, arhitectură MPP modernă, motor de interogare vectorizat, vizualizare materializată pre-agregată și index de date.

- 🖥️ **Unificat unic**: Un singur sistem poate suporta scenarii de servire a datelor în timp real, analiză interactivă a datelor și procesare offline a datelor.

- ⚛️ **Interogare federată**: Suportă interogarea federată a lacurilor de date precum Hive, Iceberg, Hudi și baze de date precum MySQL și Elasticsearch.

- ⏩ **Diverse metode de importare a datelor**: Suportă importul în loturi din HDFS/S3 și importul de flux din MySQL Binlog/Kafka; suportă scrierea micro-lot prin interfața HTTP și scrierea în timp real folosind Insert în JDBC.

- 🚙 **Ecologie bogată**: Spark folosește Spark-Doris-Connector pentru a citi și scrie Doris; Flink-Doris-Connector permite Flink CDC să implementeze scrierea datelor exact o dată în Doris; DBT Doris Adapter este furnizat pentru a transforma datele în Doris cu DBT.

## 🙌 Contribuitori

**Apache Doris a absolvit cu succes incubatorul Apache și a devenit un proiect de nivel superior în iunie 2022**.

Apreciem profund 🔗[contribuitorii comunității](https://github.com/apache/doris/graphs/contributors) pentru contribuția lor la Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Utilizatori

Apache Doris are acum o bază largă de utilizatori în China și în întreaga lume, iar până astăzi, **Apache Doris este utilizat în medii de producție în mii de companii din întreaga lume.** Peste 80% din primele 50 de companii de internet din China în ceea ce privește capitalizarea pieței sau evaluarea au folosit Apache Doris de mult timp, inclusiv Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo și Ke Holdings. Este, de asemenea, utilizat pe scară largă în unele industrii tradiționale, cum ar fi finanțe, energie, producție și telecomunicații.

Utilizatorii Apache Doris: 🔗[Utilizatori](https://doris.apache.org/users)

Adăugați logo-ul companiei dvs. pe site-ul web Apache Doris: 🔗[Adăugați compania dvs.](https://github.com/apache/doris/discussions/27683)
 
## 👣 Începe

### 📚 Documentație

Toată documentația   🔗[Documentație](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Descărcare 

Toate versiunile de lansare și binare 🔗[Descărcare](https://doris.apache.org/download) 

### 🗄️ Compilare

Vedeți cum să compilați  🔗[Compilare](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Instalare

Vedeți cum să instalați și să implementați 🔗[Instalare și implementare](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Componente

### 📝 Doris Connector

Doris oferă suport pentru Spark/Flink pentru a citi datele stocate în Doris prin Connector și, de asemenea, suportă scrierea datelor în Doris prin Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Comunitate și suport

### 📤 Abonare la liste de corespondență

Lista de corespondență este cea mai recunoscută formă de comunicare în comunitatea Apache. Vedeți cum să 🔗[Vă abonați la liste de corespondență](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Raportarea problemelor sau trimiterea Pull Request

Dacă aveți întrebări, nu ezitați să depuneți un 🔗[GitHub Issue](https://github.com/apache/doris/issues) sau să îl postați în 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) și să îl reparați trimitând un 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Cum să contribuiți

Bun venit sugestiile, comentariile (inclusiv critici), comentariile și contribuțiile dvs. Vedeți 🔗[Cum să contribuiți](https://doris.apache.org/community/how-to-contribute/) și 🔗[Ghidul de trimitere a codului](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Propuneri de îmbunătățire Doris (DSIP)

🔗[Propunerea de îmbunătățire Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) poate fi considerată ca **O colecție de documente de proiectare pentru toate actualizările sau îmbunătățirile majore ale funcționalităților**.

### 🔑 Specificația de codare Backend C++
🔗 [Specificația de codare Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) ar trebui urmată strict, ceea ce ne va ajuta să obținem o calitate mai bună a codului.

## 💬 Contactați-ne

Contactați-ne prin următoarea listă de corespondență.

| Nume                                                                          | Domeniu                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Discuții legate de dezvoltare | [Abonare](mailto:dev-subscribe@doris.apache.org)   | [Dezabonare](mailto:dev-unsubscribe@doris.apache.org)   | [Arhive](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Link-uri

* Site-ul web oficial Apache Doris - [Site](https://doris.apache.org)
* Lista de corespondență pentru dezvoltatori - <dev@doris.apache.org>. Trimiteți un e-mail la <dev-subscribe@doris.apache.org>, urmați răspunsul pentru a vă abona la lista de corespondență.
* Canalul Slack - [Alăturați-vă la Slack](https://doris.apache.org/slack)
* Twitter - [Urmăriți @doris_apache](https://twitter.com/doris_apache)


## 📜 Licență

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Notă**
> Unele licențe ale dependențelor terților nu sunt compatibile cu licența Apache 2.0. Prin urmare, trebuie să dezactivați
unele funcții Doris pentru a fi conform cu licența Apache 2.0. Pentru detalii, consultați fișierul `thirdparty/LICENSE.txt`


