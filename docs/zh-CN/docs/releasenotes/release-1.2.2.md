---
{
    "title": "Release 1.2.2",
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

在 1.2.2 版本中，Doris 团队已经修复了自 1.2.1 版本发布以来超过 200 个问题或性能改进项。同时，1.2.2 版本也作为 1.2.1 的迭代版本，具备更高的稳定性，建议所有用户升级到这个版本。


# New Feature

### 数据湖分析 

- **支持自动同步 Hive Metastore 元数据信息。** 默认情况下外部数据源的元数据变更，如创建或删除表、加减列等操作不会同步给 Doris，用户需要使用 `REFRESH CATALOG` 命令手动刷新元数据。在 1.2.2 版本中支持自动刷新 Hive Metastore 元数据信息，通过让 FE 节点定时读取 HMS 的 notification event 来感知 Hive 表元数据的变更情况。

参考文档：[https://doris.apache.org/docs/dev/lakehouse/multi-catalog/](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/)

- **支持读取 Iceberg Snapshot 以及查询 Snapshot 历史。**  在执行 Iceberg 数据写入时，每一次写操作都会产生一个新的快照。默认情况下通过 Apache Doris 读取 Iceberg 表仅会读取最新版本的快照。在 1.2.2 版本中可以使用 `FOR TIME AS OF` 和 `FOR VERSION AS OF` 语句，根据快照 ID 或者快照产生的时间读取历史版本的数据，也可以使用 iceberg_meta 表函数查询指定表的快照信息。

参考文档：[https://doris.apache.org/docs/dev/lakehouse/multi-catalog/iceberg](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/iceberg)

- JDBC Catalog 支持 PostgreSQL、Clickhouse、Oracle、SQLServer。

- **JDBC Catalog 支持 insert into 操作。** 在 Doris 中建立 JDBC Catalog 后，可以通过 insert into 语句直接写入数据，也可以将 Doris 执行完查询之后的结果写入 JDBC Catalog，或者是从一个 JDBC 外表将数据导入另一个 JDBC 外表。

参考文档：[https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/)


### 自动分桶推算

支持通过 `DISTRIBUTED BY HASH(……) BUCKETS AUTO` 语句设置自动分桶，系统帮助用户设定以及伸缩不同分区的分桶数，使分桶数保持在一个相对合适的范围内。

参考文档：[https://mp.weixin.qq.com/s/DSyZGJtjQZUYUsvfK0IcCg](https://mp.weixin.qq.com/s/DSyZGJtjQZUYUsvfK0IcCg)


### 新增函数

增加归类分析函数 `width_bucket` 。

参考文档：[https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/width-bucket/#description](https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/width-bucket/#description)


# Behavior Changes

### 默认情况下禁用 BE 的 Page Cache

关闭此配置以优化内存使用并降低内存 OOM 的风险，但有可能增加一些小查询的查询延迟。如果您对查询延迟敏感，或者具有高并发小查询场景，可以配置 `disable_storage_page_cache=false` 以再次启用 Page Cache。

### 增加新 Session 变量 `group_by_and_having_use_alias_first`

用于控制 group by 和 having 语句是否优先使用列的别名，而非从 From 语句里寻找列的名字，默认为false。

# Improvement

### Compaction 优化

- **支持 Vetical Compaction**。在过去版本中，宽列场景 Compaction 往往会带来大量的内存开销。在 1.2.2 版本中，Vertical Compaction 采用了按列组的方式进行数据合并，单次合并只需要加载部分列的数据，能够极大减少合并过程中的内存占用。在实际测试中，Vertical compaction 使用内存仅为原有 compaction 算法的 1/10，同时 Compaction 速率提升15%。

- 支持 **Segment Compaction**。在过去版本中，当用户大数据量高频导入时可能会遇到 -238 以及 -235 问题，Segment Compaction 允许在导入数据的同时进行数据的合并，以有效控制 Segment 文件的数量，提升高频导入的系统稳定性。

参考文档：[https://doris.apache.org/docs/dev/advanced/best-practice/compaction](https://doris.apache.org/docs/dev/advanced/best-practice/compaction)


### 数据湖分析

- Hive Catalog 支持访问 Hive 1/2/3 版本。

- Hive Catalog 可以使用 Broker 访问数据存储在 JuiceFS 的 Hive。

- Iceberg Catalog 支持 Hive Metastore 和 Rest 作为元数据服务。

- ES Catalog 支持 元数据字段 _id 列映射。

参考文档：[https://doris.apache.org/zh-CN/docs/dev/lakehouse/multi-catalog/hive](https://doris.apache.org/zh-CN/docs/dev/lakehouse/multi-catalog/hive)

- 优化 Iceberg V2 表有大量删除行诗时的读取性能。

- 支持读取 Schema Evolution 后 Iceberg 表。

- Parquet Reader 正确处理列名大小写。


### 其他

- 支持访问 Hadoop KMS 加密的 HDFS 。 

- 支持取消正在执行的导出任务。

参考文档：[https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/CANCEL-EXPORT](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/CANCEL-EXPORT)

- 将`explode_split` 函数执行效率优化 1 倍。

- 将 nullable 列的读取性能优化 3 倍。

- 优化 Memtracker 的部分问题，提高内存管理精度，优化内存应用。


# BugFix

- 修复了使用 Doris-Flink-Connector 导入数据时的内存泄漏问题；[#16430](https://github.com/apache/doris/pull/16430)

- 修复了 BE 可能的线程调度问题，并减少了 BE 线程耗尽导致的 Fragment_sent_timeout。

- 修复了 datetimev2/decivalv3 的部分正确性和精度问题。

- 修复了 Light Schema Change 功能的各种已知问题。

- 修复了 bitmap 类型 Runtime Filter 的各种数据正确性问题。

- 修复了 1.2.1 版本中引入的 CSV 读取性能差的问题。

- 修复了 Spark Load 数据下载阶段导致的 BE OOM 问题。

- 修复了从 1.1.x 版升级到 1.2.x 版时可能出现的元数据兼容性问题。

- 修复了创建 JDBC Catalog 时的元数据问题。

- 修复了由于导入操作导致的 CPU 使用率高的问题。

- 修复了大量失败 Broker Load 作业导致的 FE OOM 问题。

- 修复了加载浮点类型时精度丢失的问题。

- 修复了 Stream Load 使用两阶段提交时出现的内存泄漏问题。

# 其他

添加指标以查看 BE 上的 Rowset 和 Segment 总数字 `doris_be_all_rowsets_num` 和 `doris_be_all_segments_num`

# 致谢

有 53 位贡献者参与到 1.2.2 版本的开发与完善中，感谢他们的付出，他们分别是：

@adonis0147

@AshinGau

@BePPPower

@BiteTheDDDDt

@ByteYue

@caiconghui

@cambyzju

@chenlinzhong

@DarvenDuan

@dataroaring

@Doris-Extras

@dutyu

@englefly

@freemandealer

@Gabriel39

@HappenLee

@Henry2SS

@htyoung

@isHuangXin

@JackDrogon

@jacktengg

@Jibing-Li

@kaka11chen

@Kikyou1997

@Lchangliang

@LemonLiTree

@liaoxin01

@liqing-coder

@luozenglin

@morningman

@morrySnow

@mrhhsg

@nextdreamblue

@qidaye

@qzsee

@spaces-X

@stalary


@starocean999

@weizuo93

@wsjz

@xiaokang

@xinyiZzz

@xy720

@yangzhg

@yiguolei

@yixiutt

@Yukang-Lian

@Yulei-Yang

@zclllyybb

@zddr

@zhangstar333

@zhannngchen

@zy-kkk

