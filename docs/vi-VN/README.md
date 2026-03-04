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

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

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




Apache Doris là một cơ sở dữ liệu phân tích dễ sử dụng, hiệu suất cao và thời gian thực dựa trên kiến trúc MPP, được biết đến với tốc độ cực nhanh và dễ sử dụng. Nó chỉ cần thời gian phản hồi dưới một giây để trả về kết quả truy vấn dưới dữ liệu khổng lồ và có thể hỗ trợ không chỉ các kịch bản truy vấn điểm đồng thời cao mà còn cả các kịch bản phân tích phức tạp thông lượng cao.

Tất cả điều này làm cho Apache Doris trở thành một công cụ lý tưởng cho các kịch bản bao gồm phân tích báo cáo, truy vấn ad-hoc, kho dữ liệu thống nhất và tăng tốc truy vấn hồ dữ liệu. Trên Apache Doris, người dùng có thể xây dựng các ứng dụng khác nhau, chẳng hạn như phân tích hành vi người dùng, nền tảng thử nghiệm AB, phân tích truy xuất nhật ký, phân tích hồ sơ người dùng và phân tích đơn hàng.

🎉 Xem 🔗[Tất cả các phiên bản](https://doris.apache.org/docs/releasenotes/all-release), nơi bạn sẽ tìm thấy tóm tắt theo thời gian của các phiên bản Apache Doris được phát hành trong năm qua.

👀 Khám phá 🔗[Trang web chính thức](https://doris.apache.org/) để khám phá chi tiết các tính năng cốt lõi, blog và trường hợp sử dụng của Apache Doris.

## 📈 Kịch bản sử dụng

Như được hiển thị trong hình bên dưới, sau khi tích hợp và xử lý dữ liệu khác nhau, các nguồn dữ liệu thường được lưu trữ trong kho dữ liệu thời gian thực Apache Doris và hồ dữ liệu ngoại tuyến hoặc kho dữ liệu (trong Apache Hive, Apache Iceberg hoặc Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris được sử dụng rộng rãi trong các kịch bản sau:

- **Phân tích dữ liệu thời gian thực**:

  - **Báo cáo và ra quyết định thời gian thực**: Doris cung cấp báo cáo và bảng điều khiển được cập nhật thời gian thực cho cả sử dụng doanh nghiệp nội bộ và bên ngoài, hỗ trợ ra quyết định thời gian thực trong các quy trình tự động.
  
  - **Phân tích ad-hoc**: Doris cung cấp khả năng phân tích dữ liệu đa chiều, cho phép phân tích trí tuệ kinh doanh nhanh chóng và truy vấn ad-hoc để giúp người dùng nhanh chóng khám phá thông tin chi tiết từ dữ liệu phức tạp.
  
  - **Hồ sơ người dùng và phân tích hành vi**: Doris có thể phân tích hành vi người dùng như tham gia, giữ chân và chuyển đổi, đồng thời hỗ trợ các kịch bản như thông tin chi tiết về dân số và lựa chọn nhóm để phân tích hành vi.

- **Phân tích hồ dữ liệu**:

  - **Tăng tốc truy vấn hồ dữ liệu**: Doris tăng tốc truy vấn dữ liệu hồ dữ liệu với động cơ truy vấn hiệu quả của nó.
  
  - **Phân tích liên kết**: Doris hỗ trợ truy vấn liên kết trên nhiều nguồn dữ liệu, đơn giản hóa kiến trúc và loại bỏ các silo dữ liệu.
  
  - **Xử lý dữ liệu thời gian thực**: Doris kết hợp khả năng xử lý luồng dữ liệu thời gian thực và xử lý hàng loạt để đáp ứng nhu cầu đồng thời cao và độ trễ thấp của các yêu cầu kinh doanh phức tạp.

- **Khả năng quan sát dựa trên SQL**:

  - **Phân tích nhật ký và sự kiện**: Doris cho phép phân tích thời gian thực hoặc hàng loạt nhật ký và sự kiện trong các hệ thống phân tán, giúp xác định vấn đề và tối ưu hóa hiệu suất.


## Kiến trúc tổng thể

Apache Doris sử dụng giao thức MySQL, tương thích cao với cú pháp MySQL và hỗ trợ SQL tiêu chuẩn. Người dùng có thể truy cập Apache Doris thông qua các công cụ khách khác nhau và nó tích hợp liền mạch với các công cụ BI.

### Kiến trúc tích hợp lưu trữ-tính toán

Kiến trúc tích hợp lưu trữ-tính toán của Apache Doris được đơn giản hóa và dễ bảo trì. Như được hiển thị trong hình bên dưới, nó chỉ bao gồm hai loại quy trình:

- **Frontend (FE):** Chủ yếu chịu trách nhiệm xử lý yêu cầu người dùng, phân tích và lập kế hoạch truy vấn, quản lý siêu dữ liệu và các tác vụ quản lý nút.

- **Backend (BE):** Chủ yếu chịu trách nhiệm lưu trữ dữ liệu và thực thi truy vấn. Dữ liệu được phân vùng thành các mảnh và được lưu trữ với nhiều bản sao trên các nút BE.

![Kiến trúc tổng thể của Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

Trong môi trường sản xuất, nhiều nút FE có thể được triển khai để khôi phục sau thảm họa. Mỗi nút FE duy trì một bản sao đầy đủ của siêu dữ liệu. Các nút FE được chia thành ba vai trò:

| Vai trò      | Chức năng                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | Nút FE Master chịu trách nhiệm cho các hoạt động đọc và ghi siêu dữ liệu. Khi thay đổi siêu dữ liệu xảy ra trong Master, chúng được đồng bộ hóa với các nút Follower hoặc Observer thông qua giao thức BDB JE. |
| Follower  | Nút Follower chịu trách nhiệm đọc siêu dữ liệu. Nếu nút Master thất bại, một nút Follower có thể được chọn làm Master mới. |
| Observer  | Nút Observer chịu trách nhiệm đọc siêu dữ liệu và chủ yếu được sử dụng để tăng tính đồng thời của truy vấn. Nó không tham gia vào các cuộc bầu cử lãnh đạo cụm. |

Cả quy trình FE và BE đều có thể mở rộng theo chiều ngang, cho phép một cụm duy nhất hỗ trợ hàng trăm máy và hàng chục petabyte dung lượng lưu trữ. Các quy trình FE và BE sử dụng giao thức nhất quán để đảm bảo tính khả dụng cao của dịch vụ và độ tin cậy cao của dữ liệu. Kiến trúc tích hợp lưu trữ-tính toán được tích hợp cao, giảm đáng kể độ phức tạp vận hành của các hệ thống phân tán.


## Tính năng cốt lõi của Apache Doris

- **Tính khả dụng cao**: Trong Apache Doris, cả siêu dữ liệu và dữ liệu đều được lưu trữ với nhiều bản sao, đồng bộ hóa nhật ký dữ liệu thông qua giao thức quorum. Việc ghi dữ liệu được coi là thành công khi đa số các bản sao đã hoàn thành việc ghi, đảm bảo rằng cụm vẫn khả dụng ngay cả khi một số nút thất bại. Apache Doris hỗ trợ cả khôi phục sau thảm họa trong cùng thành phố và giữa các khu vực, cho phép các chế độ chủ-tớ cụm kép. Khi một số nút gặp sự cố, cụm có thể tự động cách ly các nút lỗi, ngăn chặn tính khả dụng tổng thể của cụm bị ảnh hưởng.

- **Tương thích cao**: Apache Doris tương thích cao với giao thức MySQL và hỗ trợ cú pháp SQL tiêu chuẩn, bao gồm hầu hết các chức năng MySQL và Hive. Tính tương thích cao này cho phép người dùng di chuyển và tích hợp liền mạch các ứng dụng và công cụ hiện có. Apache Doris hỗ trợ hệ sinh thái MySQL, cho phép người dùng kết nối Doris bằng các công cụ MySQL Client để vận hành và bảo trì thuận tiện hơn. Nó cũng hỗ trợ tương thích giao thức MySQL cho các công cụ báo cáo BI và công cụ truyền dữ liệu, đảm bảo hiệu quả và ổn định trong các quy trình phân tích dữ liệu và truyền dữ liệu.

- **Kho dữ liệu thời gian thực**: Dựa trên Apache Doris, một dịch vụ kho dữ liệu thời gian thực có thể được xây dựng. Apache Doris cung cấp khả năng thu thập dữ liệu cấp giây, nắm bắt các thay đổi gia tăng từ các cơ sở dữ liệu giao dịch trực tuyến thượng nguồn vào Doris trong vòng vài giây. Tận dụng các động cơ vectơ hóa, kiến trúc MPP và động cơ thực thi Pipeline, Doris cung cấp khả năng truy vấn dữ liệu dưới một giây, do đó xây dựng một nền tảng kho dữ liệu thời gian thực hiệu suất cao và độ trễ thấp.

- **Hồ dữ liệu thống nhất**: Apache Doris có thể xây dựng kiến trúc hồ dữ liệu thống nhất dựa trên các nguồn dữ liệu bên ngoài như hồ dữ liệu hoặc cơ sở dữ liệu quan hệ. Giải pháp hồ dữ liệu thống nhất của Doris cho phép tích hợp liền mạch và luồng dữ liệu tự do giữa hồ dữ liệu và kho dữ liệu, giúp người dùng sử dụng trực tiếp khả năng kho dữ liệu để giải quyết các vấn đề phân tích dữ liệu trong hồ dữ liệu trong khi tận dụng đầy đủ khả năng quản lý dữ liệu hồ dữ liệu để tăng giá trị dữ liệu.

- **Mô hình hóa linh hoạt**: Apache Doris cung cấp các phương pháp mô hình hóa khác nhau, chẳng hạn như mô hình bảng rộng, mô hình tiền tổng hợp, lược đồ sao/bông tuyết, v.v. Trong quá trình nhập dữ liệu, dữ liệu có thể được làm phẳng thành bảng rộng và ghi vào Doris thông qua các động cơ tính toán như Flink hoặc Spark, hoặc dữ liệu có thể được nhập trực tiếp vào Doris, thực hiện các hoạt động mô hình hóa dữ liệu thông qua chế độ xem, chế độ xem vật chất hóa hoặc kết hợp nhiều bảng thời gian thực.

## Tổng quan kỹ thuật

Doris cung cấp giao diện SQL hiệu quả và hoàn toàn tương thích với giao thức MySQL. Động cơ truy vấn của nó dựa trên kiến trúc MPP (xử lý song song lớn), có khả năng thực thi hiệu quả các truy vấn phân tích phức tạp và đạt được các truy vấn thời gian thực độ trễ thấp. Thông qua công nghệ lưu trữ cột cho mã hóa và nén dữ liệu, nó tối ưu hóa đáng kể hiệu suất truy vấn và tỷ lệ nén lưu trữ.

### Giao diện

Apache Doris áp dụng giao thức MySQL, hỗ trợ SQL tiêu chuẩn và tương thích cao với cú pháp MySQL. Người dùng có thể truy cập Apache Doris thông qua các công cụ khách khác nhau và tích hợp liền mạch với các công cụ BI, bao gồm nhưng không giới hạn ở Smartbi, DataEase, FineBI, Tableau, Power BI và Apache Superset. Apache Doris có thể hoạt động như nguồn dữ liệu cho bất kỳ công cụ BI nào hỗ trợ giao thức MySQL.

### Động cơ lưu trữ

Apache Doris có động cơ lưu trữ cột, mã hóa, nén và đọc dữ liệu theo cột. Điều này cho phép tỷ lệ nén dữ liệu rất cao và giảm đáng kể việc quét dữ liệu không cần thiết, do đó sử dụng hiệu quả hơn tài nguyên IO và CPU.

Apache Doris hỗ trợ các cấu trúc chỉ mục khác nhau để giảm thiểu việc quét dữ liệu:

- **Chỉ mục khóa hợp chất được sắp xếp**: Người dùng có thể chỉ định tối đa ba cột để tạo thành khóa sắp xếp hợp chất. Điều này có thể cắt tỉa dữ liệu hiệu quả để hỗ trợ tốt hơn các kịch bản báo cáo đồng thời cao.

- **Chỉ mục Min/Max**: Điều này cho phép lọc dữ liệu hiệu quả trong các truy vấn tương đương và phạm vi của các loại số.

- **Chỉ mục BloomFilter**: Điều này rất hiệu quả trong việc lọc tương đương và cắt tỉa các cột có độ lớn cao.

- **Chỉ mục đảo ngược**: Điều này cho phép tìm kiếm nhanh cho bất kỳ trường nào.

Apache Doris hỗ trợ nhiều mô hình dữ liệu và đã tối ưu hóa chúng cho các kịch bản khác nhau:

- **Mô hình chi tiết (Mô hình khóa trùng lặp):** Mô hình dữ liệu chi tiết được thiết kế để đáp ứng các yêu cầu lưu trữ chi tiết của bảng sự kiện.

- **Mô hình khóa chính (Mô hình khóa duy nhất):** Đảm bảo các khóa duy nhất; dữ liệu có cùng khóa được ghi đè, cho phép cập nhật dữ liệu ở cấp hàng.

- **Mô hình tổng hợp (Mô hình khóa tổng hợp):** Hợp nhất các cột giá trị có cùng khóa, cải thiện đáng kể hiệu suất thông qua tiền tổng hợp.

Apache Doris cũng hỗ trợ các chế độ xem vật chất hóa bảng đơn nhất quán mạnh và các chế độ xem vật chất hóa đa bảng được làm mới không đồng bộ. Các chế độ xem vật chất hóa bảng đơn được hệ thống tự động làm mới và duy trì, không yêu cầu can thiệp thủ công từ người dùng. Các chế độ xem vật chất hóa đa bảng có thể được làm mới định kỳ bằng cách sử dụng lập lịch trong cụm hoặc công cụ lập lịch bên ngoài, giảm độ phức tạp của mô hình hóa dữ liệu.

### 🔍 Động cơ truy vấn

Apache Doris có động cơ truy vấn dựa trên MPP để thực thi song song giữa và trong các nút. Nó hỗ trợ kết hợp shuffle phân tán cho các bảng lớn để xử lý tốt hơn các truy vấn phức tạp.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

Động cơ truy vấn của Apache Doris được vectơ hóa hoàn toàn, với tất cả các cấu trúc bộ nhớ được bố trí ở định dạng cột. Điều này có thể giảm đáng kể các cuộc gọi hàm ảo, tăng tỷ lệ trúng bộ nhớ cache và sử dụng hiệu quả các lệnh SIMD. Apache Doris cung cấp hiệu suất cao hơn 5-10 lần trong các kịch bản tổng hợp bảng rộng so với các động cơ không được vectơ hóa.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris sử dụng công nghệ thực thi truy vấn thích ứng để điều chỉnh động kế hoạch thực thi dựa trên thống kê thời gian chạy. Ví dụ, nó có thể tạo bộ lọc thời gian chạy và đẩy nó sang phía đầu dò. Cụ thể, nó đẩy các bộ lọc đến nút quét cấp thấp nhất ở phía đầu dò, điều này giảm đáng kể lượng dữ liệu cần xử lý và tăng hiệu suất kết hợp. Bộ lọc thời gian chạy của Apache Doris hỗ trợ In/Min/Max/Bloom Filter.

Apache Doris sử dụng động cơ thực thi Pipeline phân tách truy vấn thành nhiều nhiệm vụ con để thực thi song song, tận dụng đầy đủ khả năng CPU đa lõi. Nó đồng thời giải quyết vấn đề bùng nổ luồng bằng cách giới hạn số lượng luồng truy vấn. Động cơ thực thi Pipeline giảm sao chép và chia sẻ dữ liệu, tối ưu hóa các hoạt động sắp xếp và tổng hợp, do đó cải thiện đáng kể hiệu quả và thông lượng truy vấn.

Về mặt tối ưu hóa, Apache Doris sử dụng chiến lược tối ưu hóa kết hợp của CBO (tối ưu hóa dựa trên chi phí), RBO (tối ưu hóa dựa trên quy tắc) và HBO (tối ưu hóa dựa trên lịch sử). RBO hỗ trợ gấp hằng số, viết lại truy vấn con, đẩy xuống vị ngữ và hơn thế nữa. CBO hỗ trợ sắp xếp lại kết hợp và các tối ưu hóa khác. HBO đề xuất kế hoạch thực thi tối ưu dựa trên thông tin truy vấn lịch sử. Các biện pháp tối ưu hóa đa dạng này đảm bảo rằng Doris có thể liệt kê các kế hoạch truy vấn hiệu suất cao cho các loại truy vấn khác nhau.


## 🎆 Tại sao chọn Apache Doris?

- 🎯 **Dễ sử dụng**: Hai quy trình, không có phụ thuộc khác; mở rộng cụm trực tuyến, khôi phục bản sao tự động; tương thích với giao thức MySQL và sử dụng SQL tiêu chuẩn.

- 🚀 **Hiệu suất cao**: Hiệu suất cực nhanh cho các truy vấn độ trễ thấp và thông lượng cao với động cơ lưu trữ cột, kiến trúc MPP hiện đại, động cơ truy vấn vectơ hóa, chế độ xem vật chất hóa tiền tổng hợp và chỉ mục dữ liệu.

- 🖥️ **Thống nhất duy nhất**: Một hệ thống duy nhất có thể hỗ trợ các kịch bản phục vụ dữ liệu thời gian thực, phân tích dữ liệu tương tác và xử lý dữ liệu ngoại tuyến.

- ⚛️ **Truy vấn liên kết**: Hỗ trợ truy vấn liên kết của hồ dữ liệu như Hive, Iceberg, Hudi và cơ sở dữ liệu như MySQL và Elasticsearch.

- ⏩ **Nhiều phương pháp nhập dữ liệu**: Hỗ trợ nhập hàng loạt từ HDFS/S3 và nhập luồng từ MySQL Binlog/Kafka; hỗ trợ ghi micro-batch thông qua giao diện HTTP và ghi thời gian thực bằng Insert trong JDBC.

- 🚙 **Hệ sinh thái phong phú**: Spark sử dụng Spark-Doris-Connector để đọc và ghi Doris; Flink-Doris-Connector cho phép Flink CDC thực hiện ghi dữ liệu chính xác một lần vào Doris; DBT Doris Adapter được cung cấp để chuyển đổi dữ liệu trong Doris với DBT.

## 🙌 Người đóng góp

**Apache Doris đã tốt nghiệp thành công từ lò ấp Apache và trở thành dự án cấp cao nhất vào tháng 6 năm 2022**.

Chúng tôi đánh giá cao 🔗[người đóng góp cộng đồng](https://github.com/apache/doris/graphs/contributors) vì sự đóng góp của họ cho Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Người dùng

Apache Doris hiện có cơ sở người dùng rộng lớn ở Trung Quốc và trên toàn thế giới, và tính đến ngày nay, **Apache Doris được sử dụng trong môi trường sản xuất tại hàng nghìn công ty trên toàn thế giới.** Hơn 80% trong số 50 công ty Internet hàng đầu ở Trung Quốc về vốn hóa thị trường hoặc định giá đã sử dụng Apache Doris trong thời gian dài, bao gồm Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo và Ke Holdings. Nó cũng được sử dụng rộng rãi trong một số ngành truyền thống như tài chính, năng lượng, sản xuất và viễn thông.

Người dùng của Apache Doris: 🔗[Người dùng](https://doris.apache.org/users)

Thêm logo công ty của bạn tại Trang web Apache Doris: 🔗[Thêm công ty của bạn](https://github.com/apache/doris/discussions/27683)
 
## 👣 Bắt đầu

### 📚 Tài liệu

Tất cả tài liệu   🔗[Tài liệu](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Tải xuống 

Tất cả phiên bản phát hành và nhị phân 🔗[Tải xuống](https://doris.apache.org/download) 

### 🗄️ Biên dịch

Xem cách biên dịch  🔗[Biên dịch](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Cài đặt

Xem cách cài đặt và triển khai 🔗[Cài đặt và triển khai](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Thành phần

### 📝 Doris Connector

Doris cung cấp hỗ trợ cho Spark/Flink để đọc dữ liệu được lưu trữ trong Doris thông qua Connector và cũng hỗ trợ ghi dữ liệu vào Doris thông qua Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Cộng đồng và hỗ trợ

### 📤 Đăng ký danh sách thư

Danh sách thư là hình thức giao tiếp được công nhận nhất trong cộng đồng Apache. Xem cách 🔗[Đăng ký danh sách thư](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Báo cáo vấn đề hoặc gửi Pull Request

Nếu bạn gặp bất kỳ câu hỏi nào, vui lòng tạo 🔗[GitHub Issue](https://github.com/apache/doris/issues) hoặc đăng nó trong 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) và sửa nó bằng cách gửi 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Cách đóng góp

Chúng tôi hoan nghênh các đề xuất, nhận xét (bao gồm cả chỉ trích), nhận xét và đóng góp của bạn. Xem 🔗[Cách đóng góp](https://doris.apache.org/community/how-to-contribute/) và 🔗[Hướng dẫn gửi mã](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Đề xuất cải tiến Doris (DSIP)

🔗[Đề xuất cải tiến Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) có thể được coi là **Bộ sưu tập tài liệu thiết kế cho tất cả các cập nhật hoặc cải tiến tính năng chính**.

### 🔑 Đặc tả mã hóa Backend C++
🔗 [Đặc tả mã hóa Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) phải được tuân thủ nghiêm ngặt, điều này sẽ giúp chúng tôi đạt được chất lượng mã tốt hơn.

## 💬 Liên hệ với chúng tôi

Liên hệ với chúng tôi thông qua danh sách thư sau.

| Tên                                                                          | Phạm vi                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Thảo luận liên quan đến phát triển | [Đăng ký](mailto:dev-subscribe@doris.apache.org)   | [Hủy đăng ký](mailto:dev-unsubscribe@doris.apache.org)   | [Lưu trữ](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Liên kết

* Trang web chính thức Apache Doris - [Trang web](https://doris.apache.org)
* Danh sách thư nhà phát triển - <dev@doris.apache.org>. Gửi thư đến <dev-subscribe@doris.apache.org>, làm theo phản hồi để đăng ký danh sách thư.
* Kênh Slack - [Tham gia Slack](https://doris.apache.org/slack)
* Twitter - [Theo dõi @doris_apache](https://twitter.com/doris_apache)


## 📜 Giấy phép

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Lưu ý**
> Một số giấy phép của các phụ thuộc bên thứ ba không tương thích với Giấy phép Apache 2.0. Vì vậy, bạn cần vô hiệu hóa
một số tính năng Doris để tuân thủ Giấy phép Apache 2.0. Để biết chi tiết, tham khảo tệp `thirdparty/LICENSE.txt`


