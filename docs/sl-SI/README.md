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

## 🌍 Preberite v drugih jezikih

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris je enostavna za uporabo, visokozmogljiva in v realnem času analitična baza podatkov, ki temelji na arhitekturi MPP, znana po svoji ekstremni hitrosti in enostavnosti uporabe. Za vračanje rezultatov poizvedb pod masivnimi podatki potrebuje le čas odziva pod sekundo in lahko podpira ne le scenarije točkovnih poizvedb z visoko sočasnostjo, temveč tudi scenarije kompleksne analize z visokim prepustom.

Vse to naredi Apache Doris idealno orodje za scenarije, ki vključujejo analizo poročil, ad-hoc poizvedbe, enotno podatkovno skladišče in pospešitev poizvedb podatkovnega jezera. Na Apache Doris lahko uporabniki gradijo različne aplikacije, kot so analiza vedenja uporabnikov, platforma za testiranje AB, analiza iskanja dnevnikov, analiza profila uporabnika in analiza naročil.

🎉 Oglejte si 🔗[Vse izdaje](https://doris.apache.org/docs/releasenotes/all-release), kjer boste našli kronološki povzetek različic Apache Doris, izdanih v preteklem letu.

👀 Raziskajte 🔗[Uradno spletno mesto](https://doris.apache.org/), da podrobno odkrijete glavne funkcije, bloge in primere uporabe Apache Doris.

## 📈 Scenariji uporabe

Kot je prikazano na spodnji sliki, se po različnih integracijah in obdelavi podatkov viri podatkov običajno shranijo v podatkovno skladišče v realnem času Apache Doris in offline podatkovno jezero ali podatkovno skladišče (v Apache Hive, Apache Iceberg ali Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris se pogosto uporablja v naslednjih scenarijih:

- **Analiza podatkov v realnem času**:

  - **Poročanje in odločanje v realnem času**: Doris zagotavlja poročila in nadzorne plošče, posodobljene v realnem času, tako za interno kot zunanjo uporabo v podjetjih, podpira odločanje v realnem času v avtomatiziranih procesih.
  
  - **Ad-hoc analiza**: Doris ponuja zmogljivosti večdimenzionalne analize podatkov, omogoča hitro analizo poslovne inteligence in ad-hoc poizvedbe, da pomaga uporabnikom hitro odkriti vpoglede iz kompleksnih podatkov.
  
  - **Profiliranje uporabnikov in analiza vedenja**: Doris lahko analizira vedenja uporabnikov, kot so sodelovanje, zadrževanje in konverzija, hkrati pa podpira tudi scenarije, kot so vpogledi v populacijo in izbira skupin za analizo vedenja.

- **Analitika podatkovnega jezera**:

  - **Pospešitev poizvedb podatkovnega jezera**: Doris pospešuje poizvedbe podatkov podatkovnega jezera s svojim učinkovitim mehanizmom poizvedb.
  
  - **Federativna analitika**: Doris podpira federativne poizvedbe v več virih podatkov, poenostavlja arhitekturo in odpravlja podatkovne silose.
  
  - **Obdelava podatkov v realnem času**: Doris združuje zmogljivosti obdelave tokov podatkov v realnem času in paketne obdelave podatkov za izpolnjevanje potreb visoke sočasnosti in nizke zakasnitve kompleksnih poslovnih zahtev.

- **Opazovanje na osnovi SQL**:

  - **Analiza dnevnikov in dogodkov**: Doris omogoča analizo dnevnikov in dogodkov v realnem času ali paketno analizo v porazdeljenih sistemih, pomaga pri identifikaciji težav in optimizaciji zmogljivosti.


## Splošna arhitektura

Apache Doris uporablja protokol MySQL, je visoko združljiv s sintakso MySQL in podpira standardni SQL. Uporabniki lahko dostopajo do Apache Doris preko različnih odjemalskih orodij in se brezhibno integrira z orodji BI.

### Integrirana arhitektura shranjevanja in računanja

Integrirana arhitektura shranjevanja in računanja Apache Doris je poenostavljena in enostavna za vzdrževanje. Kot je prikazano na spodnji sliki, je sestavljena le iz dveh vrst procesov:

- **Frontend (FE):** Odgovoren predvsem za obravnavo zahtev uporabnikov, razčlenjevanje in načrtovanje poizvedb, upravljanje metapodatkov in naloge upravljanja vozlišč.

- **Backend (BE):** Odgovoren predvsem za shranjevanje podatkov in izvajanje poizvedb. Podatki so razdeljeni na dele in shranjeni z več replikami na vozliščih BE.

![Splošna arhitektura Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

V produkcijskem okolju je mogoče razporediti več vozlišč FE za obnovo po nesreči. Vsako vozlišče FE vzdržuje popolno kopijo metapodatkov. Vozlišča FE so razdeljena na tri vloge:

| Vloga      | Funkcija                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Vozlišče FE Master je odgovorno za operacije branja in pisanja metapodatkov. Ko pride do sprememb metapodatkov v Masterju, se sinhronizirajo z vozlišči Follower ali Observer preko protokola BDB JE. |
| Follower  | Vozlišče Follower je odgovorno za branje metapodatkov. Če vozlišče Master ne uspe, je mogoče izbrati vozlišče Follower kot novega Masterja. |
| Observer  | Vozlišče Observer je odgovorno za branje metapodatkov in se uporablja predvsem za povečanje sočasnosti poizvedb. Ne sodeluje pri volitvah vodstva grozda. |

Tako procesi FE kot BE so horizontalno razširljivi, kar omogoča enemu grozdu podporo za stotine strojev in desetine petabajtov zmogljivosti shranjevanja. Procesi FE in BE uporabljajo protokol konsistence za zagotavljanje visoke razpoložljivosti storitev in visoke zanesljivosti podatkov. Integrirana arhitektura shranjevanja in računanja je visoko integrirana, kar znatno zmanjšuje operativno kompleksnost porazdeljenih sistemov.


## Glavne funkcije Apache Doris

- **Visoka razpoložljivost**: V Apache Doris so tako metapodatki kot podatki shranjeni z več replikami, sinhronizirajo dnevnike podatkov preko protokola quorum. Pisanje podatkov velja za uspešno, ko večina replik zaključi pisanje, kar zagotavlja, da grozd ostane na voljo tudi če nekaj vozlišč ne uspe. Apache Doris podpira obnovo po nesreči v istem mestu in med regijami, omogoča načine master-slave dvojnega grozda. Ko nekatera vozlišča doživijo napake, lahko grozd samodejno izolira okvarjena vozlišča, kar preprečuje vpliv na splošno razpoložljivost grozda.

- **Visoka združljivost**: Apache Doris je visoko združljiv s protokolom MySQL in podpira standardno sintakso SQL, pokriva večino funkcij MySQL in Hive. Ta visoka združljivost omogoča uporabnikom brezhibno migracijo in integracijo obstoječih aplikacij in orodij. Apache Doris podpira ekosistem MySQL, kar omogoča uporabnikom povezovanje z Doris z uporabo orodij MySQL Client za bolj priročne operacije in vzdrževanje. Podpira tudi združljivost protokola MySQL za orodja za poročanje BI in orodja za prenos podatkov, kar zagotavlja učinkovitost in stabilnost v procesih analize podatkov in prenosa podatkov.

- **Podatkovno skladišče v realnem času**: Na osnovi Apache Doris je mogoče zgraditi storitev podatkovnega skladišča v realnem času. Apache Doris ponuja zmogljivosti zajemanja podatkov na ravni sekund, zajema inkrementalne spremembe iz nadrejenih spletnih transakcijskih baz podatkov v Doris v nekaj sekundah. Z izkoriščanjem vektorskih mehanizmov, arhitekture MPP in mehanizmov izvajanja Pipeline, Doris zagotavlja zmogljivosti poizvedb podatkov pod sekundo, s čimer gradi visokozmogljivo platformo podatkovnega skladišča v realnem času z nizko zakasnitvijo.

- **Enotno podatkovno jezero**: Apache Doris lahko zgradi enotno arhitekturo podatkovnega jezera na osnovi zunanjih virov podatkov, kot so podatkovna jezera ali relacijske baze podatkov. Rešitev enotnega podatkovnega jezera Doris omogoča brezhibno integracijo in prost pretok podatkov med podatkovnimi jezeri in podatkovnimi skladišči, pomaga uporabnikom neposredno uporabljati zmogljivosti podatkovnega skladišča za reševanje problemov analize podatkov v podatkovnih jezerih, hkrati pa v celoti izkorišča zmogljivosti upravljanja podatkov podatkovnega jezera za povečanje vrednosti podatkov.

- **Prilagodljivo modeliranje**: Apache Doris ponuja različne pristope k modeliranju, kot so modeli širokih tabel, modeli predhodne agregacije, sheme zvezde/snežinke itd. Med uvozom podatkov je mogoče podatke zravnati v široke tabele in zapisati v Doris preko računskih mehanizmov, kot sta Flink ali Spark, ali pa podatke neposredno uvoziti v Doris, izvajati operacije modeliranja podatkov preko pogledov, materializiranih pogledov ali povezav več tabel v realnem času.

## Tehnični pregled

Doris zagotavlja učinkovit SQL vmesnik in je popolnoma združljiv s protokolom MySQL. Njegov mehanizem poizvedb temelji na arhitekturi MPP (masivno vzporedna obdelava), ki lahko učinkovito izvaja kompleksne analitične poizvedbe in dosega poizvedbe v realnem času z nizko zakasnitvijo. Preko tehnologije shranjevanja po stolpcih za kodiranje in stiskanje podatkov znatno optimizira zmogljivost poizvedb in razmerje stiskanja shranjevanja.

### Vmesnik

Apache Doris sprejme protokol MySQL, podpira standardni SQL in je visoko združljiv s sintakso MySQL. Uporabniki lahko dostopajo do Apache Doris preko različnih odjemalskih orodij in ga brezhibno integrirajo z orodji BI, vključno, vendar ne omejeno na Smartbi, DataEase, FineBI, Tableau, Power BI in Apache Superset. Apache Doris lahko deluje kot vir podatkov za katera koli orodja BI, ki podpirajo protokol MySQL.

### Mehanizem shranjevanja

Apache Doris ima mehanizem shranjevanja po stolpcih, ki kodira, stiska in bere podatke po stolpcih. To omogoča zelo visoko razmerje stiskanja podatkov in v veliki meri zmanjšuje nepotrebno skeniranje podatkov, s čimer učinkoviteje izkorišča vire IO in CPU.

Apache Doris podpira različne strukture indeksov za zmanjšanje skeniranja podatkov:

- **Indeks razvrščenega sestavljenega ključa**: Uporabniki lahko določijo največ tri stolpce za oblikovanje sestavljenega razvrstitvenega ključa. To lahko učinkovito obreže podatke za boljšo podporo scenarijem poročanja z visoko sočasnostjo.

- **Indeks Min/Max**: To omogoča učinkovito filtriranje podatkov v poizvedbah enakovrednosti in obsega številskih tipov.

- **Indeks BloomFilter**: To je zelo učinkovito pri filtriranju enakovrednosti in obrezovanju stolpcev z visoko kardinalnostjo.

- **Obrnjeni indeks**: To omogoča hitro iskanje za poljubno polje.

Apache Doris podpira različne modele podatkov in jih je optimiziral za različne scenarije:

- **Model podrobnosti (Model podvojenega ključa):** Model podatkov podrobnosti, zasnovan za izpolnjevanje podrobnih zahtev shranjevanja tabel dejstev.

- **Model primarnega ključa (Model edinstvenega ključa):** Zagotavlja edinstvene ključe; podatki z istim ključem se prepišejo, kar omogoča posodobitve podatkov na ravni vrstic.

- **Model agregacije (Model agregacijskega ključa):** Združuje stolpce vrednosti z istim ključem, kar znatno izboljša zmogljivost preko predhodne agregacije.

Apache Doris podpira tudi močno konsistentne materializirane poglede ene tabele in asinhrono osvežene materializirane poglede več tabel. Materializirani pogledi ene tabele se samodejno osvežijo in vzdržujejo s strani sistema, ne zahtevajo ročnega posredovanja uporabnikov. Materializirani pogledi več tabel se lahko periodično osvežijo z uporabo razporejanja znotraj grozda ali zunanjih orodij za razporejanje, kar zmanjšuje kompleksnost modeliranja podatkov.

### 🔍 Mehanizem poizvedb

Apache Doris ima mehanizem poizvedb na osnovi MPP za vzporedno izvajanje med vozlišči in znotraj vozlišč. Podpira porazdeljeno mešanje povezav za velike tabele za boljšo obravnavo zapletenih poizvedb.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Mehanizem poizvedb Apache Doris je popolnoma vektorski, z vsemi strukturami pomnilnika razporejenimi v formatu stolpcev. To lahko v veliki meri zmanjša virtualne klice funkcij, poveča stopnje zadetkov predpomnilnika in učinkovito izkorišča SIMD navodila. Apache Doris zagotavlja zmogljivost 5-10 krat višjo v scenarijih agregacije širokih tabel kot nevektorski mehanizmi.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris uporablja tehnologijo prilagodljivega izvajanja poizvedb za dinamično prilagajanje načrta izvajanja na osnovi statistik časa izvajanja. Na primer, lahko ustvari filter časa izvajanja in ga potisne na stran sonda. Specifično, potisne filtre na vozlišče skeniranja najnižje ravni na strani sonda, kar v veliki meri zmanjšuje količino podatkov za obdelavo in povečuje zmogljivost povezave. Filter časa izvajanja Apache Doris podpira In/Min/Max/Bloom Filter.

Apache Doris uporablja mehanizem izvajanja Pipeline, ki razbije poizvedbe na več podnalog za vzporedno izvajanje, v celoti izkorišča zmogljivosti večjedrnega procesorja. Hkrati rešuje problem eksplozije niti z omejevanjem števila niti poizvedb. Mehanizem izvajanja Pipeline zmanjšuje kopiranje in deljenje podatkov, optimizira operacije razvrščanja in agregacije, s čimer znatno izboljšuje učinkovitost in prepust poizvedb.

Glede optimizatorja Apache Doris uporablja kombinirano strategijo optimizacije CBO (optimizator na osnovi stroškov), RBO (optimizator na osnovi pravil) in HBO (optimizator na osnovi zgodovine). RBO podpira zlaganje konstant, prepisovanje podpoizvedb, potiskanje predikatov in več. CBO podpira preurejanje povezav in druge optimizacije. HBO priporoča optimalen načrt izvajanja na osnovi zgodovinskih informacij o poizvedbah. Te večkratne ukrepe optimizacije zagotavljajo, da Doris lahko našteje načrte poizvedb visoke zmogljivosti za različne vrste poizvedb.


## 🎆 Zakaj izbrati Apache Doris?

- 🎯 **Enostavno za uporabo**: Dva procesa, brez drugih odvisnosti; spletno skaliranje grozda, samodejna obnova replik; združljivo s protokolom MySQL in uporaba standardnega SQL.

- 🚀 **Visoka zmogljivost**: Izjemno hitra zmogljivost za poizvedbe z nizko zakasnitvijo in visokim prepustom z mehanizmom shranjevanja po stolpcih, sodobno arhitekturo MPP, vektorskim mehanizmom poizvedb, predhodno agregiranim materializiranim pogledom in indeksom podatkov.

- 🖥️ **Enotno**: En sam sistem lahko podpira scenarije serviranja podatkov v realnem času, interaktivne analize podatkov in offline obdelave podatkov.

- ⚛️ **Federativne poizvedbe**: Podpira federativne poizvedbe podatkovnih jezer, kot so Hive, Iceberg, Hudi, in baz podatkov, kot sta MySQL in Elasticsearch.

- ⏩ **Različne metode uvoza podatkov**: Podpira paketni uvoz iz HDFS/S3 in tokovni uvoz iz MySQL Binlog/Kafka; podpira mikro-paketno pisanje preko HTTP vmesnika in pisanje v realnem času z uporabo Insert v JDBC.

- 🚙 **Bogat ekosistem**: Spark uporablja Spark-Doris-Connector za branje in pisanje Doris; Flink-Doris-Connector omogoča Flink CDC implementacijo točno enkratnega pisanja podatkov v Doris; DBT Doris Adapter je na voljo za transformacijo podatkov v Doris z DBT.

## 🙌 Sodelavci

**Apache Doris je uspešno končal Apache inkubator in postal projekt najvišje ravni junija 2022**.

Globoko cenimo 🔗[sodelavce skupnosti](https://github.com/apache/doris/graphs/contributors) za njihov prispevek k Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Uporabniki

Apache Doris ima zdaj široko bazo uporabnikov na Kitajskem in po vsem svetu, in do danes **Apache Doris uporabljajo v produkcijskih okoljih v tisočih podjetjih po vsem svetu.** Več kot 80% izmed 50 največjih internetnih podjetij na Kitajskem glede na tržno kapitalizacijo ali vrednotenje že dolgo uporablja Apache Doris, vključno z Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo in Ke Holdings. Pogosto se uporablja tudi v nekaterih tradicionalnih industrijah, kot so finance, energija, proizvodnja in telekomunikacije.

Uporabniki Apache Doris: 🔗[Uporabniki](https://doris.apache.org/users)

Dodajte logotip vašega podjetja na spletno mesto Apache Doris: 🔗[Dodajte vaše podjetje](https://github.com/apache/doris/discussions/27683)
 
## 👣 Začni

### 📚 Dokumentacija

Vsa dokumentacija   🔗[Dokumentacija](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Prenos 

Vse izdaje in binarne različice 🔗[Prenos](https://doris.apache.org/download) 

### 🗄️ Kompilacija

Oglejte si, kako kompilirati  🔗[Kompilacija](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Namestitev

Oglejte si, kako namestiti in razporediti 🔗[Namestitev in razporeditev](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Komponente

### 📝 Doris Connector

Doris zagotavlja podporo za Spark/Flink za branje podatkov, shranjenih v Doris, preko Connector, in tudi podpira pisanje podatkov v Doris preko Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Skupnost in podpora

### 📤 Naročite se na poštne sezname

Poštni seznam je najbolj priznana oblika komunikacije v skupnosti Apache. Oglejte si, kako se 🔗[Naročiti na poštne sezname](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Prijavite težave ali pošljite Pull Request

Če imate vprašanja, lahko brez zadržkov prijavite 🔗[GitHub Issue](https://github.com/apache/doris/issues) ali ga objavite v 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) in ga popravite z oddajo 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Kako prispevati

Veselimo se vaših predlogov, komentarjev (vključno s kritikami), komentarjev in prispevkov. Oglejte si 🔗[Kako prispevati](https://doris.apache.org/community/how-to-contribute/) in 🔗[Vodnik za oddajo kode](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Predlogi za izboljšanje Doris (DSIP)

🔗[Predlog za izboljšanje Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) lahko razumemo kot **Zbirko dokumentov oblikovanja za vse glavne posodobitve ali izboljšave funkcij**.

### 🔑 Specifikacija kodiranja Backend C++
🔗 [Specifikacija kodiranja Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) mora biti strogo upoštevana, kar nam bo pomagalo doseči boljšo kakovost kode.

## 💬 Kontaktirajte nas

Kontaktirajte nas preko naslednjega poštnega seznama.

| Ime                                                                          | Obseg                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Razprave v zvezi z razvojem | [Naročite se](mailto:dev-subscribe@doris.apache.org)   | [Odjavite se](mailto:dev-unsubscribe@doris.apache.org)   | [Arhivi](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Povezave

* Uradno spletno mesto Apache Doris - [Spletno mesto](https://doris.apache.org)
* Poštni seznam razvijalcev - <dev@doris.apache.org>. Pošljite e-pošto na <dev-subscribe@doris.apache.org>, sledite odgovoru za naročanje na poštni seznam.
* Slack kanal - [Pridružite se Slack](https://doris.apache.org/slack)
* Twitter - [Sledite @doris_apache](https://twitter.com/doris_apache)


## 📜 Licenca

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Opomba**
> Nekatere licence odvisnosti tretjih oseb niso združljive z licenco Apache 2.0. Zato morate onemogočiti
nekatere funkcije Doris, da bi bili v skladu z licenco Apache 2.0. Za podrobnosti glejte datoteko `thirdparty/LICENSE.txt`


