---
{
    "title": "Release 1.2.5",
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

在 1.2.5 版本中，Doris 团队已经修复了自 1.2.4 版本发布以来近 210 个问题或性能改进项。同时，1.2.5 版本也作为 1.2.4 的迭代版本，具备更高的稳定性，建议所有用户升级到这个版本。

# Behavior Changed

- BE 启动脚本会检查系统的最大文件句柄数需大于等于 65536，否则启动失败。

- BE 配置项 `enable_quick_compaction` 默认设为 true。即默认开启 Quick Compaction 功能。该功能用于优化大批量导入情况下的小文件问题。

- 修改表的动态分区属性后，将不再立即生效，而是统一等待下一次动态分区表的任务调度，以避免一些死锁问题。

# Improvement

- 优化 bthread 和 pthread 的使用，减少查询过程中的 RPC 阻塞问题。

- FE 前端页面的 Profile 页面增加下载 Profile 的按钮。

- 新增 FE 配置 `recover_with_skip_missing_version`，用于在某些故障情况下，查询跳过有问题的数据副本。

- 行级权限功能支持 Catalog 外表。

- Hive Catalog 支持 BE 端自动刷新 kerberos 票据，无需手动刷新。

- JDBC Catalog 支持通过 MySQL/ClickHouse 系统库（`information_schema`）下的表。

# Bug Fixes

- 修复低基数列优化导致的查询结果不正确的问题

- 修复若干访问 HDFS 的认证和兼容性问题。

- 修复若干浮点和 decimal 类型的问题。

- 修复若干 date/datetimev2 类型的问题。

- 修复若干查询执行和规划的问题。

- 修复 JDBC Catalog 的若干问题。

- 修复 Hive Catalog 的若干查询相关问题，以及 Hive Metastore 元数据同步的问题。

- 修复 `show load profile` 结果不正确的问题。

- 修复若干内存相关问题。

- 修复 `CREATE TABLE AS SELECT` 功能的若干问题。

- 修复 JSONB 类型在不支持 avx2 的机型上导致 BE 宕机的问题。

- 修复动态分区的若干问题。

- 修复 TopN 查询优化的若干问题。

- 修复 Unique Key Merge-on-Write 表模型的若干问题。


# 致谢

有 58 贡献者参与到 1.2.5 的完善和发布中，感谢他们的辛劳付出：

@adonis0147

@airborne12

@AshinGau

@BePPPower

@BiteTheDDDDt

@caiconghui

@CalvinKirs

@cambyzju

@caoliang-web

@dataroaring

@Doris-Extras

@dujl

@dutyu

@fsilent

@Gabriel39

@gitccl

@gnehil

@GoGoWen

@gongzexin

@HappenLee

@herry2038

@jacktengg

@Jibing-Li

@kaka11chen

@Kikyou1997

@LemonLiTree

@liaoxin01

@LiBinfeng-01

@luwei16

@Moonm3n

@morningman

@mrhhsg

@Mryange

@nextdreamblue

@nsnhuang

@qidaye

@Shoothzj

@sohardforaname

@stalary

@starocean999

@SWJTU-ZhangLei

@wsjz

@xiaokang

@xinyiZzz

@yangzhg

@yiguolei

@yixiutt

@yujun777

@Yulei-Yang

@yuxuan-luo

@zclllyybb

@zddr

@zenoyang

@zhangstar333

@zhannngchen

@zxealous

@zy-kkk

@zzzzzzzs
