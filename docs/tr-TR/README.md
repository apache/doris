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

## 🌍 Diğer dillerde okuyun

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris, MPP mimarisine dayalı, kullanımı kolay, yüksek performanslı ve gerçek zamanlı bir analitik veritabanıdır ve aşırı hızı ve kullanım kolaylığı ile bilinir. Büyük veriler altında sorgu sonuçlarını döndürmek için yalnızca saniyenin altında bir yanıt süresi gerektirir ve yalnızca yüksek eşzamanlılık nokta sorgu senaryolarını değil, aynı zamanda yüksek verimli karmaşık analiz senaryolarını da destekleyebilir.

Tüm bunlar Apache Doris'i rapor analizi, ad-hoc sorgu, birleşik veri ambarı ve veri gölü sorgu hızlandırma dahil senaryolar için ideal bir araç haline getirir. Apache Doris üzerinde kullanıcılar, kullanıcı davranış analizi, AB test platformu, log alma analizi, kullanıcı profil analizi ve sipariş analizi gibi çeşitli uygulamalar oluşturabilir.

🎉 🔗[Tüm sürümler](https://doris.apache.org/docs/releasenotes/all-release)'i kontrol edin, burada geçen yıl yayınlanan Apache Doris sürümlerinin kronolojik bir özetini bulacaksınız.

👀 Apache Doris'in temel özelliklerini, bloglarını ve kullanıcı örneklerini detaylı olarak keşfetmek için 🔗[Resmi Web Sitesi](https://doris.apache.org/)'ni keşfedin.

## 📈 Kullanım Senaryoları

Aşağıdaki şekilde gösterildiği gibi, çeşitli veri entegrasyonu ve işlemeden sonra, veri kaynakları genellikle gerçek zamanlı veri ambarı Apache Doris ve çevrimdışı veri gölü veya veri ambarında (Apache Hive, Apache Iceberg veya Apache Hudi'de) saklanır.

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris aşağıdaki senaryolarda yaygın olarak kullanılır:

- **Gerçek zamanlı veri analizi**:

  - **Gerçek zamanlı raporlama ve karar verme**: Doris, hem iç hem de dış kurumsal kullanım için gerçek zamanlı güncellenen raporlar ve paneller sağlar ve otomatikleştirilmiş süreçlerde gerçek zamanlı karar vermeyi destekler.
  
  - **Ad-hoc analiz**: Doris, çok boyutlu veri analizi yetenekleri sunar ve kullanıcıların karmaşık verilerden hızlı bir şekilde içgörüler keşfetmesine yardımcı olmak için hızlı iş zekası analizi ve ad-hoc sorguları mümkün kılar.
  
  - **Kullanıcı profilleme ve davranış analizi**: Doris, katılım, tutma ve dönüşüm gibi kullanıcı davranışlarını analiz edebilir ve aynı zamanda davranış analizi için nüfus içgörüleri ve grup seçimi gibi senaryoları da destekler.

- **Veri gölü analitiği**:

  - **Veri gölü sorgu hızlandırma**: Doris, verimli sorgu motoru ile veri gölü veri sorgularını hızlandırır.
  
  - **Federe analitik**: Doris, birden fazla veri kaynağında federe sorguları destekler, mimariyi basitleştirir ve veri silolarını ortadan kaldırır.
  
  - **Gerçek zamanlı veri işleme**: Doris, yüksek eşzamanlılık ve düşük gecikme süresi karmaşık iş gereksinimlerinin ihtiyaçlarını karşılamak için gerçek zamanlı veri akışları ve toplu veri işleme yeteneklerini birleştirir.

- **SQL tabanlı gözlemlenebilirlik**:

  - **Log ve olay analizi**: Doris, dağıtılmış sistemlerdeki logların ve olayların gerçek zamanlı veya toplu analizini mümkün kılar, sorunları belirlemeye ve performansı optimize etmeye yardımcı olur.


## Genel Mimari

Apache Doris MySQL protokolünü kullanır, MySQL sözdizimi ile yüksek düzeyde uyumludur ve standart SQL'i destekler. Kullanıcılar çeşitli istemci araçları aracılığıyla Apache Doris'e erişebilir ve BI araçlarıyla sorunsuz bir şekilde entegre olur.

### Depolama-Hesaplama Entegre Mimarisi

Apache Doris'in depolama-hesaplama entegre mimarisi sadeleştirilmiş ve bakımı kolaydır. Aşağıdaki şekilde gösterildiği gibi, yalnızca iki tür süreçten oluşur:

- **Frontend (FE):** Öncelikle kullanıcı isteklerini işleme, sorgu ayrıştırma ve planlama, meta veri yönetimi ve düğüm yönetimi görevlerinden sorumludur.

- **Backend (BE):** Öncelikle veri depolama ve sorgu yürütmeden sorumludur. Veriler parçalara bölünür ve BE düğümleri arasında birden fazla kopya ile saklanır.

![Apache Doris'in genel mimarisi](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

Bir üretim ortamında, olağanüstü durum kurtarma için birden fazla FE düğümü dağıtılabilir. Her FE düğümü meta verilerin tam bir kopyasını tutar. FE düğümleri üç role ayrılır:

| Rol      | İşlev                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | FE Master düğümü meta veri okuma ve yazma işlemlerinden sorumludur. Master'da meta veri değişiklikleri meydana geldiğinde, BDB JE protokolü aracılığıyla Follower veya Observer düğümlerine senkronize edilir. |
| Follower  | Follower düğümü meta verileri okumaktan sorumludur. Master düğümü başarısız olursa, bir Follower düğümü yeni Master olarak seçilebilir. |
| Observer  | Observer düğümü meta verileri okumaktan sorumludur ve esas olarak sorgu eşzamanlılığını artırmak için kullanılır. Küme liderlik seçimlerine katılmaz. |

Hem FE hem de BE süreçleri yatay olarak ölçeklenebilir, tek bir kümenin yüzlerce makineyi ve onlarca petabayt depolama kapasitesini desteklemesini sağlar. FE ve BE süreçleri, hizmetlerin yüksek kullanılabilirliğini ve verilerin yüksek güvenilirliğini sağlamak için bir tutarlılık protokolü kullanır. Depolama-hesaplama entegre mimarisi yüksek düzeyde entegre edilmiştir, dağıtılmış sistemlerin operasyonel karmaşıklığını önemli ölçüde azaltır.


## Apache Doris'in Temel Özellikleri

- **Yüksek kullanılabilirlik**: Apache Doris'te hem meta veriler hem de veriler birden fazla kopya ile saklanır, veri logları quorum protokolü aracılığıyla senkronize edilir. Veri yazma, çoğunluk kopyaları yazmayı tamamladığında başarılı kabul edilir, birkaç düğüm başarısız olsa bile kümenin kullanılabilir kalmasını sağlar. Apache Doris hem aynı şehir hem de bölgeler arası olağanüstü durum kurtarmayı destekler, çift küme ana-yedek modlarını mümkün kılar. Bazı düğümler arıza yaşadığında, küme hatalı düğümleri otomatik olarak izole edebilir, genel küme kullanılabilirliğinin etkilenmesini önler.

- **Yüksek uyumluluk**: Apache Doris MySQL protokolü ile yüksek düzeyde uyumludur ve standart SQL sözdizimini destekler, çoğu MySQL ve Hive işlevini kapsar. Bu yüksek uyumluluk, kullanıcıların mevcut uygulamaları ve araçları sorunsuz bir şekilde geçirmesine ve entegre etmesine olanak tanır. Apache Doris MySQL ekosistemini destekler, kullanıcıların daha uygun işlemler ve bakım için MySQL İstemci araçlarını kullanarak Doris'e bağlanmasını mümkün kılar. Ayrıca BI raporlama araçları ve veri iletim araçları için MySQL protokol uyumluluğunu destekler, veri analizi ve veri iletim süreçlerinde verimlilik ve istikrarı sağlar.

- **Gerçek zamanlı veri ambarı**: Apache Doris'e dayalı olarak, gerçek zamanlı bir veri ambarı hizmeti oluşturulabilir. Apache Doris saniye düzeyinde veri alma yetenekleri sunar, yukarı akış çevrimiçi işlem veritabanlarından artımlı değişiklikleri saniyeler içinde Doris'e yakalar. Vektörleştirilmiş motorlar, MPP mimarisi ve Pipeline yürütme motorlarını kullanarak, Doris saniyenin altında veri sorgu yetenekleri sağlar, böylece yüksek performanslı, düşük gecikme süreli gerçek zamanlı veri ambarı platformu oluşturur.

- **Birleşik veri gölü**: Apache Doris, veri gölleri veya ilişkisel veritabanları gibi harici veri kaynaklarına dayalı birleşik bir veri gölü mimarisi oluşturabilir. Doris birleşik veri gölü çözümü, veri gölleri ve veri ambarları arasında sorunsuz entegrasyon ve serbest veri akışı sağlar, kullanıcıların veri göllerindeki veri analizi sorunlarını çözmek için veri ambarı yeteneklerini doğrudan kullanmasına yardımcı olurken, veri değerini artırmak için veri gölü veri yönetim yeteneklerinden tam olarak yararlanır.

- **Esnek modelleme**: Apache Doris, geniş tablo modelleri, ön toplama modelleri, yıldız/kartanesi şemaları gibi çeşitli modelleme yaklaşımları sunar. Veri içe aktarma sırasında, veriler geniş tablolara düzleştirilebilir ve Flink veya Spark gibi hesaplama motorları aracılığıyla Doris'e yazılabilir veya veriler doğrudan Doris'e aktarılabilir, görünümler, materyalize edilmiş görünümler veya gerçek zamanlı çoklu tablo birleştirmeleri aracılığıyla veri modelleme işlemleri gerçekleştirilir.

## Teknik genel bakış

Doris verimli bir SQL arayüzü sağlar ve MySQL protokolü ile tamamen uyumludur. Sorgu motoru MPP (kitlesel paralel işleme) mimarisine dayanır, karmaşık analitik sorguları verimli bir şekilde yürütebilir ve düşük gecikme süreli gerçek zamanlı sorgulara ulaşabilir. Veri kodlama ve sıkıştırma için sütun depolama teknolojisi aracılığıyla, sorgu performansını ve depolama sıkıştırma oranını önemli ölçüde optimize eder.

### Arayüz

Apache Doris MySQL protokolünü benimser, standart SQL'i destekler ve MySQL sözdizimi ile yüksek düzeyde uyumludur. Kullanıcılar çeşitli istemci araçları aracılığıyla Apache Doris'e erişebilir ve Smartbi, DataEase, FineBI, Tableau, Power BI ve Apache Superset dahil ancak bunlarla sınırlı olmamak üzere BI araçlarıyla sorunsuz bir şekilde entegre edebilir. Apache Doris, MySQL protokolünü destekleyen herhangi bir BI aracı için veri kaynağı olarak çalışabilir.

### Depolama motoru

Apache Doris'in sütun depolama motoru vardır, verileri sütun bazında kodlar, sıkıştırır ve okur. Bu, çok yüksek bir veri sıkıştırma oranı sağlar ve gereksiz veri taramasını büyük ölçüde azaltır, böylece IO ve CPU kaynaklarının daha verimli kullanılmasını sağlar.

Apache Doris veri taramalarını en aza indirmek için çeşitli indeks yapılarını destekler:

- **Sıralı bileşik anahtar indeksi**: Kullanıcılar en fazla üç sütun belirterek bileşik bir sıralama anahtarı oluşturabilir. Bu, yüksek eşzamanlı raporlama senaryolarını daha iyi desteklemek için verileri etkili bir şekilde budayabilir.

- **Min/Max indeksi**: Bu, sayısal türlerin eşdeğerlik ve aralık sorgularında etkili veri filtreleme sağlar.

- **BloomFilter indeksi**: Bu, yüksek kardinaliteli sütunların eşdeğerlik filtrelemesi ve budamasında çok etkilidir.

- **Ters indeks**: Bu, herhangi bir alan için hızlı arama sağlar.

Apache Doris çeşitli veri modellerini destekler ve bunları farklı senaryolar için optimize etmiştir:

- **Detay modeli (Yinelenen anahtar modeli):** Gerçek tabloların detaylı depolama gereksinimlerini karşılamak için tasarlanmış bir detay veri modeli.

- **Birincil anahtar modeli (Benzersiz anahtar modeli):** Benzersiz anahtarları garanti eder; aynı anahtara sahip veriler üzerine yazılır, satır düzeyinde veri güncellemelerini mümkün kılar.

- **Toplama modeli (Toplama anahtarı modeli):** Aynı anahtara sahip değer sütunlarını birleştirir, ön toplama yoluyla performansı önemli ölçüde artırır.

Apache Doris ayrıca güçlü tutarlı tek tablo materyalize edilmiş görünümleri ve asenkron olarak yenilenen çoklu tablo materyalize edilmiş görünümleri destekler. Tek tablo materyalize edilmiş görünümler sistem tarafından otomatik olarak yenilenir ve korunur, kullanıcılardan manuel müdahale gerektirmez. Çoklu tablo materyalize edilmiş görünümler küme içi zamanlama veya harici zamanlama araçları kullanılarak periyodik olarak yenilenebilir, veri modellemenin karmaşıklığını azaltır.

### 🔍 Sorgu motoru

Apache Doris'in düğümler arasında ve düğümler içinde paralel yürütme için MPP tabanlı bir sorgu motoru vardır. Karmaşık sorguları daha iyi işlemek için büyük tablolar için dağıtılmış shuffle birleştirmesini destekler.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Apache Doris'in sorgu motoru tamamen vektörleştirilmiştir, tüm bellek yapıları sütun formatında düzenlenmiştir. Bu, sanal fonksiyon çağrılarını büyük ölçüde azaltabilir, önbellek isabet oranlarını artırabilir ve SIMD talimatlarını verimli bir şekilde kullanabilir. Apache Doris, vektörleştirilmemiş motorlara kıyasla geniş tablo toplama senaryolarında 5-10 kat daha yüksek performans sunar.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris, çalışma zamanı istatistiklerine dayalı olarak yürütme planını dinamik olarak ayarlamak için uyarlanabilir sorgu yürütme teknolojisi kullanır. Örneğin, bir çalışma zamanı filtresi oluşturabilir ve bunu prob tarafına itebilir. Spesifik olarak, filtreleri prob tarafındaki en düşük seviye tarama düğümüne iter, bu da işlenecek veri miktarını büyük ölçüde azaltır ve birleştirme performansını artırır. Apache Doris'in çalışma zamanı filtresi In/Min/Max/Bloom Filter'ı destekler.

Apache Doris, sorguları paralel yürütme için birden fazla alt göreve ayıran bir Pipeline yürütme motoru kullanır, çok çekirdekli CPU yeteneklerinden tam olarak yararlanır. Aynı zamanda sorgu iş parçacığı sayısını sınırlayarak iş parçacığı patlaması sorununu ele alır. Pipeline yürütme motoru veri kopyalama ve paylaşımını azaltır, sıralama ve toplama işlemlerini optimize eder, böylece sorgu verimliliğini ve iş hacmini önemli ölçüde artırır.

Optimize edici açısından, Apache Doris CBO (maliyet tabanlı optimize edici), RBO (kural tabanlı optimize edici) ve HBO (tarih tabanlı optimize edici) kombinasyonu bir optimizasyon stratejisi kullanır. RBO sabit katlama, alt sorgu yeniden yazma, yüklem itme ve daha fazlasını destekler. CBO birleştirme yeniden sıralama ve diğer optimizasyonları destekler. HBO tarihsel sorgu bilgilerine dayalı olarak optimal yürütme planını önerir. Bu çoklu optimizasyon önlemleri, Doris'in çeşitli sorgu türleri için yüksek performanslı sorgu planlarını listeleyebilmesini sağlar.


## 🎆 Neden Apache Doris'i seçmelisiniz?

- 🎯 **Kullanımı kolay**: İki süreç, başka bağımlılık yok; çevrimiçi küme ölçeklendirme, otomatik kopya kurtarma; MySQL protokolü ile uyumlu ve standart SQL kullanıyor.

- 🚀 **Yüksek performans**: Sütun depolama motoru, modern MPP mimarisi, vektörleştirilmiş sorgu motoru, ön toplanmış materyalize edilmiş görünüm ve veri indeksi ile düşük gecikme süresi ve yüksek verimli sorgular için son derece hızlı performans.

- 🖥️ **Tek birleşik**: Tek bir sistem gerçek zamanlı veri servisi, etkileşimli veri analizi ve çevrimdışı veri işleme senaryolarını destekleyebilir.

- ⚛️ **Federe sorgulama**: Hive, Iceberg, Hudi gibi veri göllerinin ve MySQL ve Elasticsearch gibi veritabanlarının federe sorgularını destekler.

- ⏩ **Çeşitli veri içe aktarma yöntemleri**: HDFS/S3'ten toplu içe aktarma ve MySQL Binlog/Kafka'dan akış içe aktarma destekler; HTTP arayüzü aracılığıyla mikro toplu yazma ve JDBC'de Insert kullanarak gerçek zamanlı yazma destekler.

- 🚙 **Zengin ekosistem**: Spark Doris'i okumak ve yazmak için Spark-Doris-Connector kullanır; Flink-Doris-Connector, Flink CDC'nin Doris'e tam olarak bir kez veri yazmasını mümkün kılar; DBT Doris Adapter, DBT ile Doris'teki verileri dönüştürmek için sağlanır.

## 🙌 Katkıda Bulunanlar

**Apache Doris Apache kuluçkasından başarıyla mezun oldu ve Haziran 2022'de Üst Düzey Proje haline geldi**.

Apache Doris'e katkıları için 🔗[topluluk katkıda bulunanlarına](https://github.com/apache/doris/graphs/contributors) derinden minnettarız.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Kullanıcılar

Apache Doris artık Çin'de ve dünya çapında geniş bir kullanıcı tabanına sahip ve bugüne kadar, **Apache Doris dünya çapında binlerce şirkette üretim ortamlarında kullanılıyor.** Çin'deki piyasa değeri veya değerleme açısından ilk 50 internet şirketinin %80'inden fazlası uzun süredir Apache Doris kullanıyor, Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo ve Ke Holdings dahil. Ayrıca finans, enerji, imalat ve telekomünikasyon gibi bazı geleneksel endüstrilerde yaygın olarak kullanılmaktadır.

Apache Doris kullanıcıları: 🔗[Kullanıcılar](https://doris.apache.org/users)

Apache Doris Web Sitesi'ne şirket logonuzu ekleyin: 🔗[Şirketinizi ekleyin](https://github.com/apache/doris/discussions/27683)
 
## 👣 Başlarken

### 📚 Dokümantasyon

Tüm dokümantasyon   🔗[Dokümantasyon](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ İndir 

Tüm sürüm ve ikili sürüm 🔗[İndir](https://doris.apache.org/download) 

### 🗄️ Derle

Nasıl derleneceğini görün  🔗[Derleme](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Yükle

Nasıl yükleneceğini ve dağıtılacağını görün 🔗[Yükleme ve dağıtım](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Bileşenler

### 📝 Doris Connector

Doris, Spark/Flink'in Connector aracılığıyla Doris'te saklanan verileri okuması için destek sağlar ve ayrıca Connector aracılığıyla Doris'e veri yazmayı da destekler.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Topluluk ve destek

### 📤 Posta listelerine abone olun

Posta listesi Apache topluluğunda en çok tanınan iletişim biçimidir. Nasıl 🔗[Posta listelerine abone olunacağını](https://doris.apache.org/community/subscribe-mail-list) görün

### 🙋 Sorun bildirin veya Pull Request gönderin

Herhangi bir sorunuz varsa, 🔗[GitHub Issue](https://github.com/apache/doris/issues) dosyası oluşturmaktan veya 🔗[GitHub Discussion](https://github.com/apache/doris/discussions)'da yayınlamaktan ve 🔗[Pull Request](https://github.com/apache/doris/pulls) göndererek düzeltmekten çekinmeyin

### 🍻 Nasıl katkıda bulunulur

Önerilerinizi, yorumlarınızı (eleştiriler dahil), yorumlarınızı ve katkılarınızı memnuniyetle karşılıyoruz. 🔗[Nasıl katkıda bulunulur](https://doris.apache.org/community/how-to-contribute/) ve 🔗[Kod gönderme kılavuzu](https://doris.apache.org/community/how-to-contribute/pull-request/)'na bakın

### ⌨️ Doris İyileştirme Önerileri (DSIP)

🔗[Doris İyileştirme Önerisi (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) **Tüm önemli özellik güncellemeleri veya iyileştirmeleri için tasarım belgelerinin bir koleksiyonu** olarak düşünülebilir.

### 🔑 Backend C++ Kodlama Spesifikasyonu
🔗 [Backend C++ Kodlama Spesifikasyonu](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) kesinlikle takip edilmelidir, bu da daha iyi kod kalitesi elde etmemize yardımcı olacaktır.

## 💬 Bize ulaşın

Aşağıdaki posta listesi aracılığıyla bizimle iletişime geçin.

| İsim                                                                          | Kapsam                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Geliştirme ile ilgili tartışmalar | [Abone ol](mailto:dev-subscribe@doris.apache.org)   | [Abonelikten çık](mailto:dev-unsubscribe@doris.apache.org)   | [Arşivler](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Bağlantılar

* Apache Doris Resmi Web Sitesi - [Site](https://doris.apache.org)
* Geliştirici posta listesi - <dev@doris.apache.org>. Posta listesine abone olmak için <dev-subscribe@doris.apache.org>'a e-posta gönderin, yanıtı takip edin.
* Slack kanalı - [Slack'e katıl](https://doris.apache.org/slack)
* Twitter - [@doris_apache'i takip edin](https://twitter.com/doris_apache)


## 📜 Lisans

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Not**
> Üçüncü taraf bağımlılıklarının bazı lisansları Apache 2.0 Lisansı ile uyumlu değildir. Bu nedenle Apache 2.0 Lisansına uyumlu olmak için
bazı Doris özelliklerini devre dışı bırakmanız gerekir. Ayrıntılar için `thirdparty/LICENSE.txt` dosyasına bakın


