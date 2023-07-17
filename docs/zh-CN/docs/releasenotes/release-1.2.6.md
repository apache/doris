---
{
    "title": "Release 1.2.6",
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



# Behavior Change

- 新增 BE 配置项 `allow_invalid_decimalv2_triteral` 以控制是否可以导入超过小数精度的数据，与以前的逻辑兼容。


# 查询

- 修复了部分查询计划的问题；
- 支持会话变量`sql_select_limit`用于与 MySQL 客户端兼容；
- 优化 Cold Run 查询性能；
- 修复 expr 上下文内存泄漏的问题；
- 修复`explode_split`函数在某些情况下执行错误的问题。

# Multi Catalog

- Fix the issue that synchronizing hive metadata caused FE replay edit log to fail.
- Fix `refresh catalog` operation causing FE OOM.
- Fix the issue that jdbc catalog cannot handle `0000-00-00` correctly.

- Fixed the issue that the kerberos ticket cannot be refreshed automatically.

- Optimize the partition pruning performance of hive.
- Fix the inconsistent behavior of trino and presto in jdbc catalog.
- Fix the issue that hdfs short-circuit read could not be used to improve query efficiency in some environments.
- Fix the issue that the iceberg table on CHDFS could not be read.
- 修复了同步 Hive 元数据时 FE 回放编辑日志失败的问题；
- 修复了`refresh catalog`操作导致 FE OOM 的问题；
- 修复了 JDBC Catalog 目录无法正确处理`0000-00-00`的问题；
- 修复了 kerberos ticket 无法自动刷新的问题；
- 优化了 Hive Partition 裁剪性能；
- 修复 JDBC Catalog 中 Trino 和 Presto 不一致的行为；
- 修复了在某些环境中无法使用 HDFS 短路读取来提高查询效率的问题；
- 修复无法读取 CHDFS Iceberg 表的问题。

# 存储

- 修复 Merge-on-Write 表中删除 bitmap 的错误计算；
- 修复了部分 BE 内存问题；
- 修复了 Snappy 压缩的问题；
- 修复 jemalloc 在某些情况下可能导致 BE 崩溃的问题。

# 其他

- Fix several java udf related issues.
- Fix the issue that the `recover table` operation incorrectly triggered the creation of dynamic partitions.
- Fix timezone when importing orc files via broker load.
- Fix the issue that the newly added `PERCENT` keyword caused the replay metadata of the routine load job to fail.
- Fix the issue that the `truncate` operation failed to acts on a non-partitioned table.
- Fix the issue that the mysql connection was lost due to the `show snapshot` operation.
- Optimize the lock logic to reduce the probability of lock timeout errors when creating tables.
- Add session variable `have_query_cache` to be compatible with some old mysql clients.
- Optimize the error message when encountering an error of loading.

- 修复了部分 Java UDF 相关问题；
- 修复了`recover table`操作错误地触发动态分区创建的问题；
- 修复了通过 Broker Load 导入 orc 文件时的时区问题；
- 修复新添加的`PERCENT`关键字导致 Routine Load 作业的回放元数据失败的问题；
- 修复了`truncate`操作无法作用于未分区表的问题；
- 修复了由于`show snapshot`操作导致 MySQL 连接丢失的问题；
- 优化锁逻辑以降低创建表时发生锁超时错误的概率；
- 添加会话变量`have_query_cache`以与一些旧的 MySQL 客户端兼容；
- 优化了导入发生错误时的报错信息。

# 致谢

感谢以下开发者在 Apache Doris 1.2.6 版本中所做的贡献；

@amorynan

@BiteTheDDDDt

@caoliang-web

@dataroaring

@Doris-Extras

@dutyu

@Gabriel39

@HHoflittlefish777

@htyoung

@jacktengg

@jeffreys-cat

@kaijchen

@kaka11chen

@Kikyou1997

@KnightLiJunLong

@liaoxin01

@LiBinfeng-01

@morningman

@mrhhsg

@sohardforaname

@starocean999

@vinlee19

@wangbo

@wsjz

@xiaokang

@xinyiZzz

@yiguolei

@yujun777

@Yulei-Yang

@zhangstar333

@zy-kkk