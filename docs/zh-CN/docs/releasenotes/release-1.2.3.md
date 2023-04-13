---
{
    "title": "Release 1.2.3",
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

在 1.2.3 版本中，Doris 团队已经修复了自 1.2.2 版本发布以来超过 200 个问题或性能改进项。同时，1.2.3 版本也作为 1.2.2 的迭代版本，具备更高的稳定性，建议所有用户升级到这个版本。


# Improvement

### JDBC Catalog 

- 支持通过 JDBC Catalog 连接到另一个 Doris 数据库。

目前 JDBC Catalog 连接 Doris 只支持用 5.x 版本的 JDBC jar 包。如果使用 8.x JDBC jar 包可能会出现列类型无法匹配问题。

参考文档：[https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/#doris](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc/#doris)

- 支持通过参数 `only_specified_database` 来同步指定的数据库。

- 支持通过 `lower_case_table_names` 参数控制是否以小写形式同步表名，解决表名区分大小写的问题。

参考文档：[https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc)

- 优化 JDBC Catalog 的读取性能。

### Elasticsearch Catalog

- 支持 Array 类型映射。

- 支持通过 `like_push_down` 属性下推 like 表达式来控制 ES 集群的 CPU 开销。

参考文档：[https://doris.apache.org/docs/dev/lakehouse/multi-catalog/es](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/es)

### Hive Catalog

- 支持 Hive 表默认分区 `__Hive_default_partition__`。

- Hive Metastore 元数据自动同步支持压缩格式的通知事件。

### 动态分区优化

- 支持通过 storage_medium 参数来控制创建动态分区的默认存储介质。

参考文档：[https://doris.apache.org/docs/dev/advanced/partition/dynamic-partition](https://doris.apache.org/docs/dev/advanced/partition/dynamic-partition)


### 优化 BE 的线程模型

- 优化 BE 的线程模型，以避免频繁创建和销毁线程所带来的稳定性问题。

# Bug 修复

- 修复了部分 Unique Key 模型 Merge-on-Write 表的问题；

- 修复了部分 Compaction 相关问题；

- 修复了部分 Delete 语句导致的数据问题；

- 修复了部分 Query 执行问题；

- 修复了在某些操作系统上使用 JDBC Catalog 导致 BE 宕机的问题；

- 修复了部分 Multi-Catalog 的问题；

- 修复了部分内存统计和优化问题；

- 修复了部分 DecimalV3 和 date/datetimev2 的相关问题。

- 修复了部分导入过程中的稳定性问题；

- 修复了部分 Light Schema Change 的问题；

- 修复了使用 `datetime` 类型创建批处理分区的问题；

- 修复了 Broker Load 大数据量导入失败而导致的 FE 内存使用过高的问题；

- 修复了删除表后无法取消 Stream Load 的问题；

- 修复了某些情况下查询 `information_schema` 超时的问题；

- 修复了使用 `select outfile` 并发数据导出导致 BE 宕机的问题；

- 修复了事务性插入操作导致内存泄漏的问题；

- 修复了部分查询和导入 Profile 的问题，并支持通过 FE web ui 直接下载 Profile 文件；

- 修复了 BE Tablet GC 线程导致 IO 负载过高的问题；

- 修复了 Kafka Routine Load 中提交 Offset 不准确的问题。
