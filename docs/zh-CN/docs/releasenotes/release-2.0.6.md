---
{
    "title": "Release 2.0.6",
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


亲爱的社区小伙伴们，[Apache Doris 2.0.6](https://doris.apache.org/download/) 版本已于 2024 年 3 月 12 日正式与大家见面。本次版本中，有 51 位贡献者提交了约 114 个功能改进以及问题修复，进一步提升了系统的稳定性和性能，欢迎大家下载体验。

**官网下载：** [https://doris.apache.org/download/](https://doris.apache.org/download/)

**GitHub 下载：** [https://github.com/apache/doris/releases](https://github.com/apache/doris/releases)


## 行为变更
- 无

## 新功能
- 自动选择物化视图时支持匹配带别名的函数
- 增加安全下线一个 tablet 副本的命令
- 外表统计信息增加行数统计缓存
- 统计信息收集支持 Rollup

## 改进和优化
- 使用 protobuf 稳定序列化减少 Tablet Schema 缓存内存占用
- 提升 `show column stats` 的性能
- 统计信息收集和优化器支持 Iceberg 和 Paimon 的行数估计
- JDBC Catalog支持读取 SQL Server 的 Timestamp 类型


## 致谢
最后，衷心感谢 50 位开发者为 Apache Doris 2.0.6 版本做出了重要贡献:

924060929, AshinGau, BePPPower, BiteTheDDDDt, CalvinKirs, cambyzju, deardeng, DongLiang-0, eldenmoon, englefly, feelshana, feiniaofeiafei, felixwluo, HappenLee, hust-hhb, iwanttobepowerful, ixzc, JackDrogon, Jibing-Li, KassieZ, larshelge, liaoxin01, LiBinfeng-01, liutang123, luennng, morningman, morrySnow, mrhhsg, qidaye, starocean999, TangSiyang2001, wangbo, wsjz, wuwenchi, xiaokang, XieJiann, xuwei0912, xy720, xzj7019, yiguolei, yujun777, Yukang-Lian, Yulei-Yang, zclllyybb, zddr, zhangstar333, zhannngchen, zhiqiang-hhhh, zy-kkk, zzzxl1993
