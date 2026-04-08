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

## 🌍 Przeczytaj w innych językach

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris to łatwa w użyciu, wysokowydajna i działająca w czasie rzeczywistym baza danych analityczna oparta na architekturze MPP, znana ze swojej ekstremalnej prędkości i łatwości użytkowania. Wymaga tylko czasu odpowiedzi poniżej sekundy, aby zwrócić wyniki zapytań przy masywnych danych i może obsługiwać nie tylko scenariusze zapytań punktowych o wysokiej współbieżności, ale także scenariusze złożonej analizy o wysokiej przepustowości.

Wszystko to sprawia, że Apache Doris jest idealnym narzędziem dla scenariuszy obejmujących analizę raportów, zapytania ad-hoc, ujednolicony magazyn danych i przyspieszenie zapytań do jeziora danych. Na Apache Doris użytkownicy mogą budować różne aplikacje, takie jak analiza zachowań użytkowników, platforma testów AB, analiza wyszukiwania logów, analiza profilu użytkownika i analiza zamówień.

🎉 Sprawdź 🔗[Wszystkie wersje](https://doris.apache.org/docs/releasenotes/all-release), gdzie znajdziesz chronologiczne podsumowanie wersji Apache Doris wydanych w ciągu ostatniego roku.

👀 Odkryj 🔗[Oficjalną stronę internetową](https://doris.apache.org/), aby szczegółowo poznać główne funkcje, blogi i przypadki użycia Apache Doris.

## 📈 Scenariusze użycia

Jak pokazano na poniższym rysunku, po różnych integracjach i przetwarzaniu danych źródła danych są zwykle przechowywane w magazynie danych w czasie rzeczywistym Apache Doris oraz w jeziorze danych offline lub magazynie danych (w Apache Hive, Apache Iceberg lub Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris jest szeroko używany w następujących scenariuszach:

- **Analiza danych w czasie rzeczywistym**:

  - **Raportowanie i podejmowanie decyzji w czasie rzeczywistym**: Doris zapewnia raporty i pulpity nawigacyjne aktualizowane w czasie rzeczywistym zarówno do użytku wewnętrznego, jak i zewnętrznego w przedsiębiorstwach, wspierając podejmowanie decyzji w czasie rzeczywistym w zautomatyzowanych procesach.
  
  - **Analiza ad-hoc**: Doris oferuje możliwości analizy danych wielowymiarowych, umożliwiając szybką analizę business intelligence i zapytania ad-hoc, aby pomóc użytkownikom szybko odkrywać wnioski z złożonych danych.
  
  - **Profilowanie użytkowników i analiza zachowań**: Doris może analizować zachowania użytkowników, takie jak uczestnictwo, retencja i konwersja, jednocześnie wspierając scenariusze takie jak wnioski demograficzne i wybór grup do analizy zachowań.

- **Analityka jeziora danych**:

  - **Przyspieszenie zapytań do jeziora danych**: Doris przyspiesza zapytania do danych jeziora danych dzięki wydajnemu silnikowi zapytań.
  
  - **Analityka federacyjna**: Doris obsługuje zapytania federacyjne w wielu źródłach danych, upraszczając architekturę i eliminując silosy danych.
  
  - **Przetwarzanie danych w czasie rzeczywistym**: Doris łączy możliwości przetwarzania strumieni danych w czasie rzeczywistym i przetwarzania wsadowego, aby sprostać potrzebom wysokiej współbieżności i niskiego opóźnienia złożonych wymagań biznesowych.

- **Obserwowalność oparta na SQL**:

  - **Analiza logów i zdarzeń**: Doris umożliwia analizę logów i zdarzeń w czasie rzeczywistym lub wsadową w systemach rozproszonych, pomagając identyfikować problemy i optymalizować wydajność.


## Ogólna architektura

Apache Doris używa protokołu MySQL, jest wysoce zgodny z składnią MySQL i obsługuje standardowy SQL. Użytkownicy mogą uzyskać dostęp do Apache Doris za pomocą różnych narzędzi klienckich i bezproblemowo integruje się z narzędziami BI.

### Zintegrowana architektura przechowywania i obliczeń

Zintegrowana architektura przechowywania i obliczeń Apache Doris jest uproszczona i łatwa w utrzymaniu. Jak pokazano na poniższym rysunku, składa się tylko z dwóch typów procesów:

- **Frontend (FE):** Odpowiedzialny głównie za obsługę żądań użytkowników, parsowanie i planowanie zapytań, zarządzanie metadanymi oraz zadania zarządzania węzłami.

- **Backend (BE):** Odpowiedzialny głównie za przechowywanie danych i wykonywanie zapytań. Dane są partycjonowane na fragmenty i przechowywane z wieloma replikami w węzłach BE.

![Ogólna architektura Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

W środowisku produkcyjnym można wdrożyć wiele węzłów FE w celu odzyskania po awarii. Każdy węzeł FE utrzymuje pełną kopię metadanych. Węzły FE są podzielone na trzy role:

| Rola      | Funkcja                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Węzeł FE Master jest odpowiedzialny za operacje odczytu i zapisu metadanych. Gdy zmiany metadanych występują w Master, są synchronizowane do węzłów Follower lub Observer za pośrednictwem protokołu BDB JE. |
| Follower  | Węzeł Follower jest odpowiedzialny za odczyt metadanych. Jeśli węzeł Master ulegnie awarii, węzeł Follower może zostać wybrany jako nowy Master. |
| Observer  | Węzeł Observer jest odpowiedzialny za odczyt metadanych i jest głównie używany do zwiększenia współbieżności zapytań. Nie uczestniczy w wyborach przywództwa klastra. |

Zarówno procesy FE, jak i BE są skalowalne poziomo, umożliwiając pojedynczemu klastrowi obsługę setek maszyn i dziesiątek petabajtów pojemności przechowywania. Procesy FE i BE używają protokołu spójności, aby zapewnić wysoką dostępność usług i wysoką niezawodność danych. Zintegrowana architektura przechowywania i obliczeń jest wysoce zintegrowana, znacznie zmniejszając złożoność operacyjną systemów rozproszonych.


## Główne funkcje Apache Doris

- **Wysoka dostępność**: W Apache Doris zarówno metadane, jak i dane są przechowywane z wieloma replikami, synchronizując dzienniki danych za pośrednictwem protokołu quorum. Zapis danych jest uważany za udany, gdy większość replik zakończy zapis, zapewniając, że klaster pozostaje dostępny nawet jeśli kilka węzłów ulegnie awarii. Apache Doris obsługuje zarówno odzyskiwanie po awarii w tym samym mieście, jak i między regionami, umożliwiając tryby master-slave podwójnego klastra. Gdy niektóre węzły doświadczają awarii, klaster może automatycznie izolować wadliwe węzły, zapobiegając wpływowi na ogólną dostępność klastra.

- **Wysoka zgodność**: Apache Doris jest wysoce zgodny z protokołem MySQL i obsługuje standardową składnię SQL, obejmując większość funkcji MySQL i Hive. Ta wysoka zgodność pozwala użytkownikom bezproblemowo migrować i integrować istniejące aplikacje i narzędzia. Apache Doris obsługuje ekosystem MySQL, umożliwiając użytkownikom łączenie się z Doris za pomocą narzędzi klienckich MySQL w celu wygodniejszych operacji i konserwacji. Obsługuje również zgodność protokołu MySQL dla narzędzi raportowania BI i narzędzi transmisji danych, zapewniając wydajność i stabilność w procesach analizy danych i transmisji danych.

- **Magazyn danych w czasie rzeczywistym**: Na podstawie Apache Doris można zbudować usługę magazynu danych w czasie rzeczywistym. Apache Doris oferuje możliwości pozyskiwania danych na poziomie sekund, przechwytując przyrostowe zmiany z upstreamowych baz danych transakcyjnych online do Doris w ciągu sekund. Wykorzystując zwektoryzowane silniki, architekturę MPP i silniki wykonawcze Pipeline, Doris zapewnia możliwości zapytań danych poniżej sekundy, budując w ten sposób platformę magazynu danych w czasie rzeczywistym o wysokiej wydajności i niskim opóźnieniu.

- **Ujednolicone jezioro danych**: Apache Doris może zbudować ujednoliconą architekturę jeziora danych opartą na zewnętrznych źródłach danych, takich jak jeziora danych lub bazy danych relacyjne. Rozwiązanie ujednoliconego jeziora danych Doris umożliwia bezproblemową integrację i swobodny przepływ danych między jeziorami danych a magazynami danych, pomagając użytkownikom bezpośrednio wykorzystywać możliwości magazynu danych do rozwiązywania problemów analizy danych w jeziorach danych, jednocześnie w pełni wykorzystując możliwości zarządzania danymi jeziora danych w celu zwiększenia wartości danych.

- **Elastyczne modelowanie**: Apache Doris oferuje różne podejścia do modelowania, takie jak modele szerokich tabel, modele pre-agregacji, schematy gwiazdy/płatka śniegu itp. Podczas importu danych dane mogą być spłaszczone do szerokich tabel i zapisane do Doris za pośrednictwem silników obliczeniowych takich jak Flink lub Spark, lub dane mogą być bezpośrednio importowane do Doris, wykonując operacje modelowania danych za pośrednictwem widoków, widoków zmaterializowanych lub połączeń wielu tabel w czasie rzeczywistym.

## Przegląd techniczny

Doris zapewnia wydajny interfejs SQL i jest w pełni zgodny z protokołem MySQL. Jego silnik zapytań jest oparty na architekturze MPP (masowo równoległe przetwarzanie), zdolnej do wydajnego wykonywania złożonych zapytań analitycznych i osiągania zapytań w czasie rzeczywistym o niskim opóźnieniu. Dzięki technologii przechowywania kolumnowego do kodowania i kompresji danych znacznie optymalizuje wydajność zapytań i współczynnik kompresji przechowywania.

### Interfejs

Apache Doris przyjmuje protokół MySQL, obsługuje standardowy SQL i jest wysoce zgodny z składnią MySQL. Użytkownicy mogą uzyskać dostęp do Apache Doris za pomocą różnych narzędzi klienckich i bezproblemowo integrować go z narzędziami BI, w tym, ale nie ograniczając się do Smartbi, DataEase, FineBI, Tableau, Power BI i Apache Superset. Apache Doris może działać jako źródło danych dla dowolnych narzędzi BI, które obsługują protokół MySQL.

### Silnik przechowywania

Apache Doris ma silnik przechowywania kolumnowego, który koduje, kompresuje i odczytuje dane kolumnami. Umożliwia to bardzo wysoki współczynnik kompresji danych i znacznie zmniejsza niepotrzebne skanowanie danych, dzięki czemu bardziej efektywnie wykorzystuje zasoby IO i CPU.

Apache Doris obsługuje różne struktury indeksów, aby zminimalizować skanowanie danych:

- **Indeks złożonego klucza posortowanego**: Użytkownicy mogą określić maksymalnie trzy kolumny, aby utworzyć złożony klucz sortowania. Może to skutecznie przycinać dane, aby lepiej obsługiwać scenariusze raportowania o wysokiej współbieżności.

- **Indeks Min/Max**: Umożliwia to skuteczne filtrowanie danych w zapytaniach równoważności i zakresu typów numerycznych.

- **Indeks BloomFilter**: Jest bardzo skuteczny w filtrowaniu równoważności i przycinaniu kolumn o wysokiej kardynalności.

- **Indeks odwrócony**: Umożliwia szybkie wyszukiwanie dla dowolnego pola.

Apache Doris obsługuje różnorodne modele danych i zoptymalizował je dla różnych scenariuszy:

- **Model szczegółów (Model zduplikowanego klucza):** Model danych szczegółowych zaprojektowany do spełnienia szczegółowych wymagań przechowywania tabel faktów.

- **Model klucza podstawowego (Model unikalnego klucza):** Zapewnia unikalne klucze; dane z tym samym kluczem są nadpisywane, umożliwiając aktualizacje danych na poziomie wiersza.

- **Model agregacji (Model klucza agregacji):** Łączy kolumny wartości z tym samym kluczem, znacznie poprawiając wydajność poprzez pre-agregację.

Apache Doris obsługuje również silnie spójne zmaterializowane widoki pojedynczej tabeli i asynchronicznie odświeżane zmaterializowane widoki wielu tabel. Zmaterializowane widoki pojedynczej tabeli są automatycznie odświeżane i utrzymywane przez system, nie wymagając ręcznej interwencji użytkowników. Zmaterializowane widoki wielu tabel mogą być okresowo odświeżane za pomocą harmonogramowania w klastrze lub zewnętrznych narzędzi harmonogramowania, zmniejszając złożoność modelowania danych.

### 🔍 Silnik zapytań

Apache Doris ma silnik zapytań oparty na MPP do równoległego wykonywania między węzłami i wewnątrz węzłów. Obsługuje rozproszone połączenie shuffle dla dużych tabel, aby lepiej obsługiwać skomplikowane zapytania.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Silnik zapytań Apache Doris jest w pełni zwektoryzowany, ze wszystkimi strukturami pamięci ułożonymi w formacie kolumnowym. Może to znacznie zmniejszyć wywołania funkcji wirtualnych, zwiększyć wskaźniki trafień pamięci podręcznej i efektywnie wykorzystywać instrukcje SIMD. Apache Doris zapewnia wydajność 5-10 razy wyższą w scenariuszach agregacji szerokich tabel niż silniki niezwektoryzowane.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris wykorzystuje technologię adaptacyjnego wykonywania zapytań do dynamicznego dostosowywania planu wykonania na podstawie statystyk czasu wykonania. Na przykład może generować filtr czasu wykonania i wypychać go na stronę sondy. Konkretnie, wypycha filtry do węzła skanowania najniższego poziomu po stronie sondy, co znacznie zmniejsza ilość danych do przetworzenia i zwiększa wydajność połączenia. Filtr czasu wykonania Apache Doris obsługuje In/Min/Max/Bloom Filter.

Apache Doris wykorzystuje silnik wykonawczy Pipeline, który rozbija zapytania na wiele podzadań do równoległego wykonywania, w pełni wykorzystując możliwości procesora wielordzeniowego. Jednocześnie rozwiązuje problem eksplozji wątków, ograniczając liczbę wątków zapytań. Silnik wykonawczy Pipeline zmniejsza kopiowanie i udostępnianie danych, optymalizuje operacje sortowania i agregacji, znacznie poprawiając wydajność i przepustowość zapytań.

Pod względem optymalizatora Apache Doris stosuje połączoną strategię optymalizacji CBO (optymalizator oparty na kosztach), RBO (optymalizator oparty na regułach) i HBO (optymalizator oparty na historii). RBO obsługuje składanie stałych, przepisywanie podzapytań, wypychanie predykatów i więcej. CBO obsługuje zmianę kolejności połączeń i inne optymalizacje. HBO rekomenduje optymalny plan wykonania na podstawie historycznych informacji o zapytaniach. Te wielokrotne środki optymalizacji zapewniają, że Doris może wyliczyć plany zapytań o wysokiej wydajności dla różnych typów zapytań.


## 🎆 Dlaczego wybrać Apache Doris?

- 🎯 **Łatwy w użyciu**: Dwa procesy, brak innych zależności; skalowanie klastra online, automatyczne odzyskiwanie replik; zgodny z protokołem MySQL i używający standardowego SQL.

- 🚀 **Wysoka wydajność**: Niezwykle szybka wydajność dla zapytań o niskim opóźnieniu i wysokiej przepustowości z silnikiem przechowywania kolumnowego, nowoczesną architekturą MPP, zwektoryzowanym silnikiem zapytań, pre-agregowanym zmaterializowanym widokiem i indeksem danych.

- 🖥️ **Pojedynczy ujednolicony**: Pojedynczy system może obsługiwać scenariusze serwowania danych w czasie rzeczywistym, interaktywnej analizy danych i przetwarzania danych offline.

- ⚛️ **Zapytania federacyjne**: Obsługuje zapytania federacyjne jezior danych takich jak Hive, Iceberg, Hudi oraz baz danych takich jak MySQL i Elasticsearch.

- ⏩ **Różne metody importu danych**: Obsługuje import wsadowy z HDFS/S3 i import strumieniowy z MySQL Binlog/Kafka; obsługuje zapisywanie mikro-wsadowe przez interfejs HTTP i zapisywanie w czasie rzeczywistym przy użyciu Insert w JDBC.

- 🚙 **Bogata ekosystem**: Spark używa Spark-Doris-Connector do odczytu i zapisu Doris; Flink-Doris-Connector umożliwia Flink CDC zaimplementowanie dokładnie jednokrotnego zapisu danych do Doris; DBT Doris Adapter jest dostarczany do przekształcania danych w Doris za pomocą DBT.

## 🙌 Współtwórcy

**Apache Doris pomyślnie ukończył inkubator Apache i stał się projektem najwyższego poziomu w czerwcu 2022 roku**.

Głęboko doceniamy 🔗[współtwórców społeczności](https://github.com/apache/doris/graphs/contributors) za ich wkład w Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Użytkownicy

Apache Doris ma teraz szeroką bazę użytkowników w Chinach i na całym świecie, a do dziś **Apache Doris jest używany w środowiskach produkcyjnych w tysiącach firm na całym świecie.** Ponad 80% z 50 najlepszych firm internetowych w Chinach pod względem kapitalizacji rynkowej lub wyceny używa Apache Doris od dawna, w tym Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo i Ke Holdings. Jest również szeroko używany w niektórych tradycyjnych branżach, takich jak finanse, energia, produkcja i telekomunikacja.

Użytkownicy Apache Doris: 🔗[Użytkownicy](https://doris.apache.org/users)

Dodaj logo swojej firmy na stronie Apache Doris: 🔗[Dodaj swoją firmę](https://github.com/apache/doris/discussions/27683)
 
## 👣 Rozpocznij

### 📚 Dokumentacja

Wszystka dokumentacja   🔗[Dokumentacja](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Pobierz 

Wszystkie wersje wydań i binarne 🔗[Pobierz](https://doris.apache.org/download) 

### 🗄️ Kompiluj

Zobacz jak skompilować  🔗[Kompilacja](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Zainstaluj

Zobacz jak zainstalować i wdrożyć 🔗[Instalacja i wdrożenie](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Komponenty

### 📝 Doris Connector

Doris zapewnia wsparcie dla Spark/Flink do odczytu danych przechowywanych w Doris za pośrednictwem Connector, a także obsługuje zapisywanie danych do Doris za pośrednictwem Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Społeczność i wsparcie

### 📤 Subskrybuj listy mailingowe

Lista mailingowa jest najbardziej uznaną formą komunikacji w społeczności Apache. Zobacz jak 🔗[Subskrybować listy mailingowe](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Zgłoś problemy lub prześlij Pull Request

Jeśli masz pytania, możesz zgłosić 🔗[GitHub Issue](https://github.com/apache/doris/issues) lub opublikować go w 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) i naprawić go, przesyłając 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Jak przyczynić się

Witamy Twoje sugestie, komentarze (w tym krytyki), komentarze i wkład. Zobacz 🔗[Jak przyczynić się](https://doris.apache.org/community/how-to-contribute/) i 🔗[Przewodnik po przesyłaniu kodu](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Propozycje ulepszeń Doris (DSIP)

🔗[Propozycja ulepszenia Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) może być uważana za **Kolekcję dokumentów projektowych dla wszystkich głównych aktualizacji lub ulepszeń funkcji**.

### 🔑 Specyfikacja kodowania Backend C++
🔗 [Specyfikacja kodowania Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) powinna być ściśle przestrzegana, co pomoże nam osiągnąć lepszą jakość kodu.

## 💬 Skontaktuj się z nami

Skontaktuj się z nami za pośrednictwem następującej listy mailingowej.

| Nazwa                                                                          | Zakres                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Dyskusje związane z rozwojem | [Subskrybuj](mailto:dev-subscribe@doris.apache.org)   | [Anuluj subskrypcję](mailto:dev-unsubscribe@doris.apache.org)   | [Archiwa](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Linki

* Oficjalna strona internetowa Apache Doris - [Strona](https://doris.apache.org)
* Lista mailingowa deweloperów - <dev@doris.apache.org>. Wyślij wiadomość e-mail na <dev-subscribe@doris.apache.org>, postępuj zgodnie z odpowiedzią, aby subskrybować listę mailingową.
* Kanał Slack - [Dołącz do Slack](https://doris.apache.org/slack)
* Twitter - [Śledź @doris_apache](https://twitter.com/doris_apache)


## 📜 Licencja

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Uwaga**
> Niektóre licencje zależności stron trzecich nie są zgodne z licencją Apache 2.0. Dlatego musisz wyłączyć
niektóre funkcje Doris, aby być zgodnym z licencją Apache 2.0. Aby uzyskać szczegóły, zapoznaj się z plikiem `thirdparty/LICENSE.txt`


