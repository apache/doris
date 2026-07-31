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

## 🌍 Đọc bằng các ngôn ngữ khác

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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

Apache Doris là cơ sở dữ liệu phân tích và tìm kiếm thời gian thực, mã nguồn mở, được xây dựng trên kiến trúc MPP. Doris cung cấp phân tích SQL tốc độ cao, tăng tốc truy vấn lakehouse và tìm kiếm lai trên dữ liệu có cấu trúc, văn bản và dữ liệu vector.

Truy cập [trang web chính thức](https://doris.apache.org/) để xem tổng quan sản phẩm mới nhất, các trường hợp sử dụng, cập nhật hệ sinh thái, blog và câu chuyện người dùng. Để theo dõi cập nhật phiên bản, hãy xem [tất cả ghi chú phát hành](https://doris.apache.org/releases/all-release).

## 📈 Trường hợp sử dụng

| Trường hợp sử dụng | Giá trị mang lại |
| -------- | ---------------- |
| [Phân tích cho người dùng cuối](https://doris.apache.org/use-cases/customer-facing-analytics) | Cung cấp phân tích tương tác dưới một giây cho người dùng bên ngoài. |
| [Kho dữ liệu](https://doris.apache.org/use-cases/data-warehousing) | Xây dựng một kho dữ liệu thời gian thực cho nhiều miền nghiệp vụ. |
| [Quan sát hệ thống](https://doris.apache.org/use-cases/observability) | Phân tích log, sự kiện và chỉ số thông lượng cao bằng SQL. |
| [Doris cho AI](https://doris.apache.org/use-cases/ai) | Dùng tìm kiếm vector, văn bản, JSON và dữ liệu có cấu trúc trong một engine SQL duy nhất. |

## 🚀 Khả năng cốt lõi

Apache Doris được xây dựng xoay quanh ba khả năng cốt lõi. Trang web là nguồn tham chiếu chính cho các mô tả sản phẩm và ví dụ chi tiết.

| Khả năng | Giá trị mang lại |
| ---------- | ---------------- |
| [Phân tích thời gian thực](https://doris.apache.org/#real-time-analytics) | Nạp dữ liệu streaming, chuyển đổi tăng dần và truy vấn dưới một giây ở mức đồng thời cao. |
| [Phân tích lakehouse](https://doris.apache.org/#lakehouse-analytics) | Phân tích SQL tốc độ cao trên các định dạng bảng mở như Iceberg, Delta Lake và Hudi. |
| [Tìm kiếm lai](https://doris.apache.org/#hybrid-search) | Phân tích native bằng SQL trên JSON, dữ liệu toàn văn và dữ liệu vector cho khối lượng công việc AI và tìm kiếm. |

## 🔌 Hệ sinh thái

Doris nằm ở trung tâm của stack dữ liệu hiện đại. Doris kết nối cơ sở dữ liệu nguồn, hệ thống streaming và lưu trữ lakehouse với các công cụ BI, AI, phân tích và quan sát hệ thống ở phía tiêu thụ.

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

Để xem phạm vi hệ sinh thái mới nhất, hãy truy cập [trang web chính thức](https://doris.apache.org/) và [tài liệu kết nối và tích hợp](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro).

## 👣 Bắt đầu

- [Apache Doris là gì](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [Bắt đầu nhanh](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [Tải xuống](https://doris.apache.org/download)
- [Cài đặt](https://doris.apache.org/docs/dev/install/intro)
- [Biên dịch từ nguồn](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 Kiến trúc

Apache Doris hỗ trợ cả triển khai gắn kết compute-storage và triển khai tách rời compute-storage. Ở chế độ tách rời, các nhóm compute không trạng thái chạy trên object storage dùng chung, nhờ đó bạn có thể mở rộng compute theo nhu cầu và cô lập khối lượng công việc.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

Tìm hiểu thêm trong [hướng dẫn triển khai](https://doris.apache.org/docs/dev/install/intro) và [hướng dẫn chọn chế độ triển khai](https://doris.apache.org/docs/dev/install/choosing-deployment-mode).

## 📣 Cập nhật dự án

| Tài nguyên | Giá trị mang lại |
| -------- | ---------------- |
| [Báo cáo cộng đồng](https://doris.apache.org/community-report/) | Cập nhật hằng tuần về hoạt động cộng đồng, PR đã merge, người đóng góp và tiến độ tính năng. |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | Thảo luận kế hoạch năm 2026 cho AI và tìm kiếm lai, query engine, lưu trữ và data lake. |

## 🧩 Thành phần

Doris cung cấp connector và công cụ cho các quy trình kỹ thuật dữ liệu phổ biến.

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 Người dùng

Apache Doris được hàng nghìn công ty trên toàn thế giới sử dụng trong môi trường production, thuộc các lĩnh vực dịch vụ internet, tài chính, bán lẻ, logistics, sản xuất, năng lượng, viễn thông, AI và nhiều ngành khác.

- [Người dùng Apache Doris](https://doris.apache.org/why-doris/users/)
- [Thêm công ty của bạn](https://github.com/apache/doris/discussions/27683)

## 🙌 Người đóng góp

Apache Doris đã tốt nghiệp Apache Incubator và trở thành Apache Top-Level Project vào tháng 6 năm 2022. Cảm ơn tất cả [người đóng góp trong cộng đồng](https://github.com/apache/doris/graphs/contributors) đã cùng xây dựng Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 Cộng đồng và hỗ trợ

- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [Cách đóng góp](https://doris.apache.org/community/how-to-contribute/)
- [Hướng dẫn gửi mã](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 Liên hệ với chúng tôi

- [Tham gia Slack](https://doris.apache.org/slack)
- [Đăng ký danh sách gửi thư của nhà phát triển](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 Liên kết

- Trang web chính thức của Apache Doris: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 Giấy phép

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Lưu ý**
> Một số giấy phép của các dependency bên thứ ba không tương thích với Apache 2.0 License. Vì vậy, bạn cần tắt
một số tính năng của Doris để tuân thủ Apache 2.0 License. Để biết chi tiết, hãy tham khảo `thirdparty/LICENSE.txt`
