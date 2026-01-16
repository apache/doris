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

## 🌍 Baca dalam bahasa lain

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris adalah database analitik yang mudah digunakan, berkinerja tinggi, dan real-time berbasis arsitektur MPP, dikenal karena kecepatan ekstrem dan kemudahan penggunaannya. Hanya memerlukan waktu respons sub-detik untuk mengembalikan hasil kueri di bawah data masif dan dapat mendukung tidak hanya skenario kueri titik konkurensi tinggi tetapi juga skenario analisis kompleks throughput tinggi.

Semua ini menjadikan Apache Doris sebagai alat ideal untuk skenario termasuk analisis laporan, kueri ad-hoc, gudang data terpadu, dan percepatan kueri data lake. Di Apache Doris, pengguna dapat membangun berbagai aplikasi, seperti analisis perilaku pengguna, platform uji AB, analisis pengambilan log, analisis profil pengguna, dan analisis pesanan.

🎉 Lihat 🔗[Semua rilis](https://doris.apache.org/docs/releasenotes/all-release), di mana Anda akan menemukan ringkasan kronologis versi Apache Doris yang dirilis selama setahun terakhir.

👀 Jelajahi 🔗[Situs Web Resmi](https://doris.apache.org/) untuk menemukan fitur inti, blog, dan kasus penggunaan Apache Doris secara detail.

## 📈 Skenario Penggunaan

Seperti yang ditunjukkan pada gambar di bawah ini, setelah berbagai integrasi dan pemrosesan data, sumber data biasanya disimpan di gudang data real-time Apache Doris dan data lake offline atau gudang data (di Apache Hive, Apache Iceberg atau Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris banyak digunakan dalam skenario berikut:

- **Analisis Data Real-time**:

  - **Pelaporan dan Pengambilan Keputusan Real-time**: Doris menyediakan laporan dan dashboard yang diperbarui secara real-time untuk penggunaan perusahaan internal dan eksternal, mendukung pengambilan keputusan real-time dalam proses yang terotomatisasi.
  
  - **Analisis Ad Hoc**: Doris menawarkan kemampuan analisis data multidimensi, memungkinkan analisis intelijen bisnis yang cepat dan kueri ad-hoc untuk membantu pengguna dengan cepat menemukan wawasan dari data kompleks.
  
  - **Profil Pengguna dan Analisis Perilaku**: Doris dapat menganalisis perilaku pengguna seperti partisipasi, retensi, dan konversi, sambil juga mendukung skenario seperti wawasan populasi dan pemilihan kelompok untuk analisis perilaku.

- **Analitik Data Lake**:

  - **Percepatan Kueri Data Lake**: Doris mempercepat kueri data data lake dengan mesin kueri yang efisien.
  
  - **Analitik Federasi**: Doris mendukung kueri federasi di beberapa sumber data, menyederhanakan arsitektur dan menghilangkan silo data.
  
  - **Pemrosesan Data Real-time**: Doris menggabungkan kemampuan pemrosesan aliran data real-time dan batch untuk memenuhi kebutuhan konkurensi tinggi dan latensi rendah dari persyaratan bisnis yang kompleks.

- **Observabilitas Berbasis SQL**:

  - **Analisis Log dan Acara**: Doris memungkinkan analisis real-time atau batch dari log dan acara dalam sistem terdistribusi, membantu mengidentifikasi masalah dan mengoptimalkan kinerja.


## Arsitektur Keseluruhan

Apache Doris menggunakan protokol MySQL, sangat kompatibel dengan sintaks MySQL, dan mendukung SQL standar. Pengguna dapat mengakses Apache Doris melalui berbagai alat klien, dan terintegrasi dengan mulus dengan alat BI.

### Arsitektur Terintegrasi Penyimpanan-Komputasi

Arsitektur terintegrasi penyimpanan-komputasi Apache Doris disederhanakan dan mudah dirawat. Seperti yang ditunjukkan pada gambar di bawah ini, hanya terdiri dari dua jenis proses:

- **Frontend (FE):** Terutama bertanggung jawab untuk menangani permintaan pengguna, penguraian dan perencanaan kueri, manajemen metadata, dan tugas manajemen node.

- **Backend (BE):** Terutama bertanggung jawab untuk penyimpanan data dan eksekusi kueri. Data dipartisi menjadi pecahan dan disimpan dengan beberapa replika di seluruh node BE.

![Arsitektur keseluruhan Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

Dalam lingkungan produksi, beberapa node FE dapat dikerahkan untuk pemulihan bencana. Setiap node FE mempertahankan salinan lengkap metadata. Node FE dibagi menjadi tiga peran:

| Peran      | Fungsi                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Node FE Master bertanggung jawab untuk operasi baca dan tulis metadata. Ketika perubahan metadata terjadi di Master, mereka disinkronkan ke node Follower atau Observer melalui protokol BDB JE. |
| Follower  | Node Follower bertanggung jawab untuk membaca metadata. Jika node Master gagal, node Follower dapat dipilih sebagai Master baru. |
| Observer  | Node Observer bertanggung jawab untuk membaca metadata dan terutama digunakan untuk meningkatkan konkurensi kueri. Ini tidak berpartisipasi dalam pemilihan kepemimpinan cluster. |

Baik proses FE maupun BE dapat diskalakan secara horizontal, memungkinkan satu cluster untuk mendukung ratusan mesin dan puluhan petabita kapasitas penyimpanan. Proses FE dan BE menggunakan protokol konsistensi untuk memastikan ketersediaan tinggi layanan dan keandalan tinggi data. Arsitektur terintegrasi penyimpanan-komputasi sangat terintegrasi, secara signifikan mengurangi kompleksitas operasional sistem terdistribusi.


## Fitur Inti Apache Doris

- **Ketersediaan Tinggi**: Di Apache Doris, baik metadata maupun data disimpan dengan beberapa replika, menyinkronkan log data melalui protokol quorum. Penulisan data dianggap berhasil setelah mayoritas replika menyelesaikan penulisan, memastikan bahwa cluster tetap tersedia bahkan jika beberapa node gagal. Apache Doris mendukung pemulihan bencana dalam kota yang sama dan lintas wilayah, memungkinkan mode master-slave cluster ganda. Ketika beberapa node mengalami kegagalan, cluster dapat secara otomatis mengisolasi node yang rusak, mencegah ketersediaan cluster keseluruhan terpengaruh.

- **Kompatibilitas Tinggi**: Apache Doris sangat kompatibel dengan protokol MySQL dan mendukung sintaks SQL standar, mencakup sebagian besar fungsi MySQL dan Hive. Kompatibilitas tinggi ini memungkinkan pengguna untuk bermigrasi dan mengintegrasikan aplikasi dan alat yang ada dengan mulus. Apache Doris mendukung ekosistem MySQL, memungkinkan pengguna untuk menghubungkan Doris menggunakan alat MySQL Client untuk operasi dan pemeliharaan yang lebih nyaman. Ini juga mendukung kompatibilitas protokol MySQL untuk alat pelaporan BI dan alat transmisi data, memastikan efisiensi dan stabilitas dalam proses analisis data dan transmisi data.

- **Gudang Data Real-time**: Berdasarkan Apache Doris, layanan gudang data real-time dapat dibangun. Apache Doris menawarkan kemampuan pengambilan data tingkat detik, menangkap perubahan inkremental dari database transaksional online upstream ke Doris dalam hitungan detik. Memanfaatkan mesin tervektorisasi, arsitektur MPP, dan mesin eksekusi Pipeline, Doris menyediakan kemampuan kueri data sub-detik, sehingga membangun platform gudang data real-time berkinerja tinggi dan latensi rendah.

- **Data Lake Terpadu**: Apache Doris dapat membangun arsitektur data lake terpadu berdasarkan sumber data eksternal seperti data lake atau database relasional. Solusi data lake terpadu Doris memungkinkan integrasi yang mulus dan aliran data bebas antara data lake dan gudang data, membantu pengguna memanfaatkan langsung kemampuan gudang data untuk menyelesaikan masalah analisis data di data lake sambil memanfaatkan sepenuhnya kemampuan manajemen data data lake untuk meningkatkan nilai data.

- **Pemodelan Fleksibel**: Apache Doris menawarkan berbagai pendekatan pemodelan, seperti model tabel lebar, model pra-agregasi, skema bintang/salju, dll. Selama impor data, data dapat diratakan menjadi tabel lebar dan ditulis ke Doris melalui mesin komputasi seperti Flink atau Spark, atau data dapat langsung diimpor ke Doris, melakukan operasi pemodelan data melalui tampilan, tampilan materialisasi, atau gabungan multi-tabel real-time.

## Ikhtisar Teknis

Doris menyediakan antarmuka SQL yang efisien dan sepenuhnya kompatibel dengan protokol MySQL. Mesin kuerinya didasarkan pada arsitektur MPP (Pemrosesan Paralel Massal), mampu mengeksekusi kueri analitik kompleks secara efisien dan mencapai kueri real-time latensi rendah. Melalui teknologi penyimpanan kolom untuk pengkodean dan kompresi data, ini secara signifikan mengoptimalkan kinerja kueri dan rasio kompresi penyimpanan.

### Antarmuka

Apache Doris mengadopsi protokol MySQL, mendukung SQL standar, dan sangat kompatibel dengan sintaks MySQL. Pengguna dapat mengakses Apache Doris melalui berbagai alat klien dan mengintegrasikannya dengan mulus dengan alat BI, termasuk tetapi tidak terbatas pada Smartbi, DataEase, FineBI, Tableau, Power BI, dan Apache Superset. Apache Doris dapat berfungsi sebagai sumber data untuk alat BI apa pun yang mendukung protokol MySQL.

### Mesin Penyimpanan

Apache Doris memiliki mesin penyimpanan kolom, yang mengkodekan, mengompresi, dan membaca data berdasarkan kolom. Ini memungkinkan rasio kompresi data yang sangat tinggi dan sebagian besar mengurangi pemindaian data yang tidak perlu, sehingga membuat penggunaan sumber daya IO dan CPU lebih efisien.

Apache Doris mendukung berbagai struktur indeks untuk meminimalkan pemindaian data:

- **Indeks Kunci Gabungan yang Diurutkan**: Pengguna dapat menentukan paling banyak tiga kolom untuk membentuk kunci pengurutan gabungan. Ini dapat secara efektif memangkas data untuk lebih mendukung skenario pelaporan yang sangat konkuren.

- **Indeks Min/Max**: Ini memungkinkan penyaringan data yang efektif dalam kueri kesetaraan dan rentang tipe numerik.

- **Indeks BloomFilter**: Ini sangat efektif dalam penyaringan kesetaraan dan pemangkasan kolom kardinalitas tinggi.

- **Indeks Terbalik**: Ini memungkinkan pencarian cepat untuk bidang apa pun.

Apache Doris mendukung berbagai model data dan telah mengoptimalkannya untuk skenario yang berbeda:

- **Model Detail (Model Kunci Duplikat):** Model data detail yang dirancang untuk memenuhi persyaratan penyimpanan detail tabel fakta.

- **Model Kunci Primer (Model Kunci Unik):** Memastikan kunci unik; data dengan kunci yang sama ditimpa, memungkinkan pembaruan data tingkat baris.

- **Model Agregasi (Model Kunci Agregasi):** Menggabungkan kolom nilai dengan kunci yang sama, secara signifikan meningkatkan kinerja melalui pra-agregasi.

Apache Doris juga mendukung tampilan materialisasi tabel tunggal yang sangat konsisten dan tampilan materialisasi multi-tabel yang diperbarui secara asinkron. Tampilan materialisasi tabel tunggal secara otomatis diperbarui dan dipelihara oleh sistem, tidak memerlukan intervensi manual dari pengguna. Tampilan materialisasi multi-tabel dapat diperbarui secara berkala menggunakan penjadwalan dalam cluster atau alat penjadwalan eksternal, mengurangi kompleksitas pemodelan data.

### 🔍 Mesin Kueri

Apache Doris memiliki mesin kueri berbasis MPP untuk eksekusi paralel antar node dan dalam node. Ini mendukung gabungan shuffle terdistribusi untuk tabel besar untuk menangani kueri yang rumit dengan lebih baik.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Mesin kueri Apache Doris sepenuhnya tervektorisasi, dengan semua struktur memori diletakkan dalam format kolom. Ini dapat secara signifikan mengurangi panggilan fungsi virtual, meningkatkan tingkat hit cache, dan membuat penggunaan instruksi SIMD yang efisien. Apache Doris memberikan kinerja 5-10 kali lebih tinggi dalam skenario agregasi tabel lebar daripada mesin yang tidak tervektorisasi.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris menggunakan teknologi eksekusi kueri adaptif untuk secara dinamis menyesuaikan rencana eksekusi berdasarkan statistik waktu proses. Misalnya, ini dapat menghasilkan filter waktu proses dan mendorongnya ke sisi probe. Secara khusus, ini mendorong filter ke node pemindaian tingkat terendah di sisi probe, yang secara signifikan mengurangi jumlah data yang akan diproses dan meningkatkan kinerja gabungan. Filter waktu proses Apache Doris mendukung In/Min/Max/Bloom Filter.

Apache Doris menggunakan mesin eksekusi Pipeline yang memecah kueri menjadi beberapa sub-tugas untuk eksekusi paralel, memanfaatkan sepenuhnya kemampuan CPU multi-core. Ini secara bersamaan mengatasi masalah ledakan thread dengan membatasi jumlah thread kueri. Mesin eksekusi Pipeline mengurangi penyalinan dan berbagi data, mengoptimalkan operasi pengurutan dan agregasi, sehingga secara signifikan meningkatkan efisiensi dan throughput kueri.

Dalam hal pengoptimal, Apache Doris menggunakan strategi pengoptimalan gabungan dari CBO (Pengoptimal Berbasis Biaya), RBO (Pengoptimal Berbasis Aturan), dan HBO (Pengoptimal Berbasis Sejarah). RBO mendukung pelipatan konstanta, penulisan ulang subkueri, penurunan predikat, dan lainnya. CBO mendukung pengurutan ulang gabungan dan pengoptimalan lainnya. HBO merekomendasikan rencana eksekusi optimal berdasarkan informasi kueri historis. Langkah-langkah pengoptimalan ganda ini memastikan bahwa Doris dapat menghitung rencana kueri berkinerja tinggi untuk berbagai jenis kueri.


## 🎆 Mengapa memilih Apache Doris?

- 🎯 **Mudah Digunakan**: Dua proses, tidak ada dependensi lain; penskalaan cluster online, pemulihan replika otomatis; kompatibel dengan protokol MySQL, dan menggunakan SQL standar.

- 🚀 **Kinerja Tinggi**: Kinerja yang sangat cepat untuk kueri latensi rendah dan throughput tinggi dengan mesin penyimpanan kolom, arsitektur MPP modern, mesin kueri tervektorisasi, tampilan materialisasi pra-agregasi dan indeks data.

- 🖥️ **Terpadu Tunggal**: Satu sistem dapat mendukung skenario penyajian data real-time, analisis data interaktif dan pemrosesan data offline.

- ⚛️ **Kueri Federasi**: Mendukung kueri federasi dari data lake seperti Hive, Iceberg, Hudi, dan database seperti MySQL dan Elasticsearch.

- ⏩ **Berbagai Metode Impor Data**: Mendukung impor batch dari HDFS/S3 dan impor aliran dari MySQL Binlog/Kafka; mendukung penulisan mikro-batch melalui antarmuka HTTP dan penulisan real-time menggunakan Insert di JDBC.

- 🚙 **Ekologi Kaya**: Spark menggunakan Spark-Doris-Connector untuk membaca dan menulis Doris; Flink-Doris-Connector memungkinkan Flink CDC mengimplementasikan penulisan data tepat satu kali ke Doris; DBT Doris Adapter disediakan untuk mengubah data di Doris dengan DBT.

## 🙌 Kontributor

**Apache Doris telah lulus dengan sukses dari inkubator Apache dan menjadi Proyek Tingkat Atas pada Juni 2022**.

Kami sangat menghargai 🔗[kontributor komunitas](https://github.com/apache/doris/graphs/contributors) atas kontribusi mereka terhadap Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Pengguna

Apache Doris sekarang memiliki basis pengguna yang luas di China dan di seluruh dunia, dan hingga saat ini, **Apache Doris digunakan dalam lingkungan produksi di ribuan perusahaan di seluruh dunia.** Lebih dari 80% dari 50 perusahaan Internet teratas di China dalam hal kapitalisasi pasar atau valuasi telah menggunakan Apache Doris untuk waktu yang lama, termasuk Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo, dan Ke Holdings. Ini juga banyak digunakan di beberapa industri tradisional seperti keuangan, energi, manufaktur, dan telekomunikasi.

Pengguna Apache Doris: 🔗[Pengguna](https://doris.apache.org/users)

Tambahkan logo perusahaan Anda di Situs Web Apache Doris: 🔗[Tambahkan Perusahaan Anda](https://github.com/apache/doris/discussions/27683)
 
## 👣 Memulai

### 📚 Dokumentasi

Semua Dokumentasi   🔗[Dokumentasi](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Unduh 

Semua versi rilis dan biner 🔗[Unduh](https://doris.apache.org/download) 

### 🗄️ Kompilasi

Lihat cara mengompilasi  🔗[Kompilasi](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Instalasi

Lihat cara menginstal dan menyebarkan 🔗[Instalasi dan penyebaran](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Komponen

### 📝 Doris Connector

Doris menyediakan dukungan untuk Spark/Flink untuk membaca data yang disimpan di Doris melalui Connector, dan juga mendukung menulis data ke Doris melalui Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Komunitas dan Dukungan

### 📤 Berlangganan Daftar Surat

Daftar Surat adalah bentuk komunikasi yang paling diakui dalam komunitas Apache. Lihat cara 🔗[Berlangganan Daftar Surat](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Laporkan Masalah atau Kirim Pull Request

Jika Anda memiliki pertanyaan, jangan ragu untuk mengajukan 🔗[GitHub Issue](https://github.com/apache/doris/issues) atau mempostingnya di 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) dan memperbaikinya dengan mengirimkan 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Cara Berkontribusi

Kami menyambut saran, komentar (termasuk kritik), komentar dan kontribusi Anda. Lihat 🔗[Cara Berkontribusi](https://doris.apache.org/community/how-to-contribute/) dan 🔗[Panduan Pengiriman Kode](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Proposal Peningkatan Doris (DSIP)

🔗[Proposal Peningkatan Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) dapat dianggap sebagai **Koleksi Dokumen Desain untuk semua Pembaruan atau Peningkatan Fitur Utama**.

### 🔑 Spesifikasi Pengkodean Backend C++
🔗 [Spesifikasi Pengkodean Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) harus diikuti dengan ketat, yang akan membantu kami mencapai kualitas kode yang lebih baik.

## 💬 Hubungi Kami

Hubungi kami melalui daftar surat berikut.

| Nama                                                                          | Cakupan                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Diskusi terkait pengembangan | [Berlangganan](mailto:dev-subscribe@doris.apache.org)   | [Berhenti berlangganan](mailto:dev-unsubscribe@doris.apache.org)   | [Arsip](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Tautan

* Situs Web Resmi Apache Doris - [Situs](https://doris.apache.org)
* Daftar surat pengembang - <dev@doris.apache.org>. Kirim email ke <dev-subscribe@doris.apache.org>, ikuti balasan untuk berlangganan daftar surat.
* Saluran Slack - [Bergabung dengan Slack](https://doris.apache.org/slack)
* Twitter - [Ikuti @doris_apache](https://twitter.com/doris_apache)


## 📜 Lisensi

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Catatan**
> Beberapa lisensi dari dependensi pihak ketiga tidak kompatibel dengan Lisensi Apache 2.0. Jadi Anda perlu menonaktifkan
beberapa fitur Doris agar sesuai dengan Lisensi Apache 2.0. Untuk detailnya, lihat file `thirdparty/LICENSE.txt`


