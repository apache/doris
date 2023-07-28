---
{
    "title": "Release 1.2.4",
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

在 1.2.4 版本中，Doris 团队已经修复了自 1.2.3 版本发布以来近 150 个问题或性能改进项。同时，1.2.4 版本也作为 1.2.3 的迭代版本，具备更高的稳定性，建议所有用户升级到这个版本。

# Behavior Changed

- 针对 Date/DatetimeV2 和 DecimalV3 类型，在 `DESCRIBLE` 和 `SHOW CREATE TABLE` 语句的结果中，将不再显示为 Date/DatetimeV2 或 DecimalV3，而直接显示为 Date/Datetime 或 Decimal。
  - 这个改动用于兼容部分 BI 系统。如果想查看列的实际类型，可以通过 `DESCRIBE ALL` 语句查看。

- 查询 `information_schema` 库中的表时，默认不再返回 External Catalog 中的元信息。
  - 这个改动避免了因 External Catalog 的连接问题导致的 information_schema 库不可查的问题，从而解决部分 BI 系统与 Doris 配合使用的问题。可以通过 FE 的配置项 `infodb_support_ext_catalog `控制，默认为 false，即不返回 External Catalog 中的元信息。

# Improvement

### JDBC Catalog

- 支持通过 JDBC Catalog 连接其他 Trino/Presto 集群

​        参考文档：[https://doris.apache.org/zh-CN/docs/dev/lakehouse/multi-catalog/jdbc#trino](https://doris.apache.org/zh-CN/docs/dev/lakehouse/multi-catalog/jdbc#trino)

- JDBC Catalog 连接 Clickhouse 数据源支持 Array 类型映射

​        参考文档：[https://doris.apache.org/zh-CN/docs/dev/lakehouse/multi-catalog/jdbc#clickhouse](https://doris.apache.org/zh-CN/docs/dev/lakehouse/multi-catalog/jdbc#clickhouse)

### Spark Load

- Spark Load 支持 Resource Manager HA 相关配置

​        参考 PR： [https://github.com/apache/doris/pull/15000](https://github.com/apache/doris/pull/15000)

# Bug Fixes

- 修复 Hive Catalog 的若干连通性问题。

- 修复 Hudi Catalog 的若干问题。

- 优化 JDBC Catalog 的连接池，避免过多的连接。

- 修复通过 JDBC Catalog 从另一个 Doris 集群导入数据是会发生 OOM 的问题。

- 修复若干查询和导入的规划问题。

- 修复 Unique Key Merge-On-Write 表的若干问题。

- 修复若干 BDBJE 问题，解决某些情况下 FE 元数据异常的问题。

- 修复 `CREATE VIEW` 语句不支持 Table Valued Function 的问题。

- 修复若干内存统计的问题。

- 修复读取 Parquet/ORC 表的若干问题。

- 修复 DecimalV3 的若干问题。

- 修复 `SHOW QUERY/LOAD PROFILE` 的若干问题。

# 致谢

有 47 位贡献者参与到 1.2.4 的完善和发布中，感谢他们的辛劳付出：

@zy-kkk

@zhannngchen

@zhangstar333

@yixiutt

@yiguolei

@xinyiZzz

@xiaokang

@wsjz

@wangbo

@starocean999

@sohardforaname

@siriume

@pingchunzhang

@nextdreamblue

@mymeiyi

@mrhhsg

@morrySnow

@morningman

@luwei16

@luozenglin

@liujinhui1994

@liaoxin01

@kaka11chen

@jeffreys-cat

@jacktengg

@gavinchou

@dutyu

@dataroaring

@chenlinzhong

@caoliang-web

@cambyzju

@adonis0147

@Yulei-Yang

@Yukang-Lian

@SWJTU-ZhangLei

@Kikyou1997

@Jibing-Li

@JackDrogon

@HappenLee

@GoGoWen

@Gabriel39

@Doris-Extras

@CalvinKirs

@Cai-Yao

@ByteYue

@BiteTheDDDDt

@BePPPower
