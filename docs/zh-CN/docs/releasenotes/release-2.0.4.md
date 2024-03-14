---
{
    "title": "Release 2.0.4",
    "language": "zh-CN"
}
---

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

亲爱的社区小伙伴们，Apache Doris 2.0.4  版本已于 2024 年 1 月 26 日正式发布，该版本在新优化器、倒排索引、数据湖等功能上有了进一步的完善与更新，使 Apache Doris 能够适配更广泛的场景。此外，该版本进行了若干的改进与优化，以提供更加稳定高效的性能体验。新版本已经上线，欢迎大家下载使用！

**官网下载：** [https://doris.apache.org/download/](https://doris.apache.org/download/)

**GitHub 下载：** [https://github.com/apache/doris/releases](https://github.com/apache/doris/releases)


## 行为变更
- 提供了更精确的 Precision 和 Scale 推导，可满足金融场景计算的高要求
  - [https://github.com/apache/doris/pull/28034](https://github.com/apache/doris/pull/28034)
- Drop Policy 支持了 User 和 Role
  - [https://github.com/apache/doris/pull/29488](https://github.com/apache/doris/pull/29488)

## 新功能
- 新优化器支持了 datev1， datetimev1 及 decimalv2 数据类型
- 新优化器支持了 ODBC 外表
- 倒排索引支持了 `lower_case` 和 `ignore_above` 选项
- 倒排索引支持了 `match_regexp` 和 `match_phrase_prefix` 查询加速
- 数据湖支持了 Paimon Native Reader
- 数据湖支持读取 LZO 压缩的 Parquet 文件
- 审计日志支持 `insert into`

## 改进和优化
- 对数据均衡、迁移等存储管控进行了改进
- 对数据冷却策略进行了改进，以节省本地硬盘存储空间
- 对 ASCII 字符串 substr 进行了优化
- 针对使用 date 函数查询时的分区裁剪进行了优化
- 针对优化器自动统计信息收集的可观测性和性能进行了优化

## 致谢

感谢 73 位开发者为 Apache Doris 2.0.4 版本做出了重要贡献 ，正是由于他们的努力，Apache Doris 在性能和稳定性方面取得了显著的进步。

airborne12、amorynan、AshinGau、BePPPower、bingquanzhao、BiteTheDDDDt、bobhan1、ByteYue、caiconghui、CalvinKirs、cambyzju、caoliang-web、catpineapple、csun5285、dataroaring、deardeng、dutyu、eldenmoon、englefly、feifeifeimoon、fornaix、Gabriel39、gnehil、HappenLee、hello-stephen、HHoflittlefish777、hubgeter、hust-hhb、ixzc、jacktengg、jackwener、Jibing-Li、kaka11chen、KassieZ、LemonLiTree、liaoxin01、LiBinfeng-01、lihuigang、liugddx、luwei16、morningman、morrySnow、mrhhsg、Mryange、nextdreamblue、Nitin-Kashyap、platoneko、py023、qidaye、shuke987、starocean999、SWJTU-ZhangLei、w41ter、wangbo、wsjz、wuwenchi、Xiaoccer、xiaokang、XieJiann、xingyingone、xinyiZzz、xuwei0912、xy720、xzj7019、yujun777、zclllyybb、zddr、zhangguoqiang666、zhangstar333、zhannngchen、zhiqiang-hhhh、zy-kkk、zzzxl1993