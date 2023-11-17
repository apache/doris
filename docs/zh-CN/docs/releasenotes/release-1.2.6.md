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


# Behavior Changed

- 新增 BE 配置项 `allow_invalid_decimalv2_triteral` 以控制是否可以导入超过小数精度的 Decimal 类型数据，用于兼容之前的逻辑。

# Bug Fixes

## 查询

- 修复了部分查询计划的问题；
- 支持会话变量 `sql_select_limit` 和 `have_query_cache` 用于与老版本的 MySQL 客户端兼容；
- 优化 Cold Run 查询性能；
- 修复 Expr Context 类内存泄漏的问题；
- 修复 `explode_split` 函数在某些情况下执行错误的问题。

## Multi Catalog

- 修复了同步 Hive 元数据时 FE 回放元数据日志失败的问题；
- 修复了 `refresh catalog` 操作可能导致 FE OOM 的问题；
- 修复了 JDBC Catalog 无法正确处理 `0000-00-00` 日期格式的问题；
- 修复了 kerberos ticket 无法自动刷新的问题；
- 优化了 Hive Partition 裁剪性能；
- 修复 JDBC Catalog 中 Trino 和 Presto 不一致的行为；
- 修复了在某些环境中无法使用 HDFS 短路读取来提高查询效率的问题；
- 修复无法读取 CHDFS Iceberg 表的问题。

## 存储

- 修复 Merge-on-Write 表中删除 bitmap 逻辑计算错误的问题；
- 修复了若干 BE 内存问题；
- 修复了表数据 Snappy 压缩的问题；
- 修复 jemalloc 在某些情况下可能导致 BE 崩溃的问题。

## 其他

- 修复了部分 Java UDF 相关问题；
- 修复了 `recover table` 操作错误地触发动态分区创建的问题；
- 修复了通过 Broker Load 导入 orc 文件时的时区问题；
- 修复新添加的 `PERCENT` 关键字导致 Routine Load 作业的回放元数据失败的问题；
- 修复了 `truncate` 操作无法作用于非分区表的问题；
- 修复了由于 `show snapshot` 操作导致 MySQL 连接丢失的问题；
- 优化锁逻辑以降低创建表时发生锁超时错误的概率；
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
