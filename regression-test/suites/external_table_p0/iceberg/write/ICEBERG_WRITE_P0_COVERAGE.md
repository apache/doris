<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg 写入 P0 覆盖矩阵

## 范围与判定原则

本文档覆盖 Doris 向 Iceberg 表写入时的正确性、兼容性和失败原子性。矩阵既检查单项能力，也检查 schema change、Partition Evolution、snapshot/tag/branch、行级 delete/update/merge、表模型、分区与 bucket、数据类型和 NULL 语义之间的交互。

所有正向写入场景都以 Doris 确定性查询结果和 Spark 读取同一 Iceberg 表的结果一致作为双重 oracle；涉及历史引用时，另行校验 snapshot、tag 和 branch 的隔离性。

覆盖状态含义：

- 已覆盖（验证通过）：P0 suite 对该场景有确定性结果断言，且已在多 BE 环境验证。
- 预期拒绝：Doris 明确不支持该操作，P0 suite 验证错误信息与失败原子性。
- 已覆盖（隔离负向）：已形成可复现产品问题的 regression；默认 P0 隔离运行，避免杀死共享 BE 或提交不可读文件。

## 风险点

| 编号 | 风险描述 | 来源 | 影响面 | 级别 |
| --- | --- | --- | --- | --- |
| R01 | schema change 后 writer 仍按旧列位置或旧 field id 写入，造成静默错列 | 白盒：Iceberg field id 与 Doris slot 映射 | 数据正确性 | P0 |
| R02 | Partition Evolution 后新文件落入旧 spec、分区值计算错误，或跨 spec 过滤漏数 | 黑盒 + 白盒：多 partition spec 并存 | 写入与查询正确性 | P0 |
| R03 | 字符串、数值、日期时间、decimal、布尔和 NULL 作为 identity/bucket/truncate/time transform 源时行为不一致 | 黑盒：类型与边界输入 | 分区路由、裁剪 | P0 |
| R04 | schema/partition 演进后 snapshot、tag、branch 绑定了错误 schema 或数据版本 | 黑盒：历史读与引用 | time travel 正确性 | P0 |
| R05 | MOR 的 delete/update/merge 跨新旧 spec 时生成错误 delete file；COW 被拒绝后仍发布快照 | 白盒：row-level DML commit | 数据丢失、失败原子性 | P0 |
| R06 | Duplicate、Unique MOW、Unique MOR、Aggregate 源表语义在 INSERT SELECT 时被改变 | 黑盒：不同 Doris 表模型 | 跨表写入正确性 | P0 |
| R07 | RANGE/LIST/无分区源表以及 HASH/RANDOM/AUTO bucket 在多 BE 执行时产生重复或丢行 | 黑盒 + 白盒：分布式 exchange 与 sink writer | 分布式写入正确性 | P0 |
| R08 | primitive、ARRAY、MAP、STRUCT 及嵌套 NULL 在 schema change 前后写入错误 | 黑盒：复杂类型与 NULL | 数据正确性、兼容性 | P0 |
| R09 | NULL 写入 Iceberg required 列未报错、部分数据或空快照被提交 | 白盒：required 校验与 commit | 约束、失败原子性 | P0 |
| R10 | INSERT OVERWRITE 在演进后的当前 spec、branch 或 NULL 分区上误删其他分区 | 黑盒：覆盖写 | 数据丢失 | P0 |
| R11 | 单 BE 可通过但多 BE 并发 sink 出现文件名、commit 或分区冲突 | 白盒：并行 writer 与统一 commit | 分布式稳定性 | P1 |
| R12 | nullable STRING 或 DML 产生的 Nullable block 经过 truncate transform 时 BE FATAL | 白盒：partition transformer 列类型约束 | 集群可用性 | P0 |

## 组合覆盖

| 维度 | 场景 | 状态 | P0 suite |
| --- | --- | --- | --- |
| 基础写入 | Parquet/ORC、primitive/复杂类型、INSERT/OVERWRITE | 已覆盖 | `test_iceberg_write_insert`、`test_iceberg_insert_overwrite` |
| Partition transform | identity、bucket、truncate、year/month/day/hour | 已覆盖 | `test_iceberg_write_transform_partitions`、`test_iceberg_static_partition_overwrite` |
| schema + partition 演进 | add/rename/drop/type promotion 与 ADD/REPLACE/DROP partition field 后继续写入和过滤 | 已覆盖（验证通过） | `test_iceberg_write_evolution_refs` |
| 复杂类型演进 | ARRAY/MAP/STRUCT promotion、STRUCT 新增字段、旧文件与新写入并存 | 已覆盖（验证通过） | `test_iceberg_write_complex_evolution` |
| 历史版本 | 演进前后 snapshot、tag、branch；branch 独立写入和覆盖写 | 已覆盖（验证通过） | `test_iceberg_write_evolution_refs` |
| MOR | partition evolution 后 DELETE/UPDATE/MERGE，校验当前、delete files 与历史版本 | 已覆盖（验证通过） | `test_iceberg_write_dml_modes_evolution` |
| COW | partition evolution 后 DELETE/UPDATE/MERGE 拒绝，且数据和 snapshot 数不变 | 预期拒绝 | `test_iceberg_write_dml_modes_evolution` |
| Doris 源表模型 | Duplicate、Unique MOW、Unique MOR、Aggregate | 已覆盖（验证通过） | `test_iceberg_write_source_models` |
| Doris 源分区 | 无分区、RANGE、LIST | 已覆盖（验证通过） | `test_iceberg_write_source_models` |
| Doris 源 bucket | HASH 固定 bucket、RANDOM bucket、HASH AUTO bucket | 已覆盖（验证通过） | `test_iceberg_write_source_models` |
| 分区源类型 | STRING/INT/BIGINT/DATE/DATETIME/DECIMAL 的 bucket 与适用 transform；BOOLEAN identity 与非法 bucket | 已覆盖（验证通过） | `test_iceberg_write_partition_types_null` |
| NULL 分区 | identity NULL、数值/decimal bucket 与 truncate NULL、time transform NULL、多列组合 NULL | 已覆盖（验证通过） | `test_iceberg_write_partition_types_null` |
| nullable STRING truncate | nullable STRING 经过 truncate transform 的 INSERT，以及 NOT NULL 源列经 UPDATE block 写入 | 已覆盖（隔离负向） | `test_iceberg_write_nullable_truncate_negative` |
| nullable 数据 | 顶层 NULL、ARRAY NULL 元素、MAP NULL value、STRUCT NULL child | 已覆盖并增强 | `test_iceberg_write_insert`、`test_iceberg_write_complex_evolution` |
| required 列正向与 schema change | required 列合法写入、nullable 列写 NULL、增加 required 列与 nullable→required 拒绝 | 已覆盖（验证通过） | `test_iceberg_write_nullability_atomicity` |
| required 列写 NULL | VALUES 与分布式 INSERT SELECT 混合批次写 NULL | 已覆盖（隔离负向） | `test_iceberg_write_required_null_values_negative`、`test_iceberg_write_required_null_select_negative` |
| 覆盖写 | 当前 spec、静态分区、branch、空输入 | 已覆盖并增强 | `test_iceberg_static_partition_overwrite`、`test_iceberg_write_evolution_refs` |
| 分布式执行 | 多 bucket 源表、多分区 Iceberg sink、多 BE writer、suite 间无共享 catalog/database | 已覆盖（验证通过） | 所有本次新增 suite |
| Spark 交叉验证 | Doris 写入后由 Spark 与 Doris 查询同一 Iceberg 表并逐行比较 | 已覆盖（验证通过） | 六个正向 suite |

## 本次新增用例设计

| 用例 | 目标 | 覆盖风险 | 测试维度 | 前置条件 | 负载描述 | 执行预期 |
| --- | --- | --- | --- | --- | --- | --- |
| W01 | 验证 schema 与 partition spec 同时演进后的写入、过滤和历史引用 | R01、R02、R04、R10 | 功能、正确性、兼容性 | Iceberg REST catalog | 演进前后多批 Doris 写入，建立 snapshot/tag/branch，并对 branch 覆盖写 | 当前、历史和 branch 各自返回确定数据；跨 spec 过滤不漏数 |
| W02 | 验证复杂类型 field id 在演进后保持正确 | R01、R08 | 功能、正确性 | Iceberg v2 | ARRAY/MAP value promotion、STRUCT child promotion/add，写入含嵌套 NULL 的新旧行 | 旧值按新 schema 可读，新值不串字段，嵌套 NULL 保留 |
| W03 | 验证 MOR/COW 与 partition evolution、NULL 分区、time travel 的交互 | R02、R04、R05 | 功能、正确性、异常 | Iceberg v2 MOR/COW | MOR 执行 delete/update/merge；COW 执行相同操作 | MOR 当前与历史版本一致；COW 明确拒绝且无新 snapshot |
| W04 | 验证不同 Doris 表模型、分区和 bucket 作为 Iceberg 写入源 | R06、R07、R11 | 正确性、兼容性 | 多 BE Doris | 四种表模型、三种分区方式、HASH/RANDOM/AUTO bucket 执行 INSERT SELECT | 写入结果保持各源表语义，无重复或丢行 |
| W05 | 验证不同类型与 NULL 的 partition/bucket transform | R02、R03、R11 | 功能、正确性、边界 | Iceberg v2 | identity/bucket/truncate/time transform 多列组合，包含 NULL | 数据与 `$partitions` 统计一致；NULL 行可过滤且可继续写入 |
| W06 | 验证 required/nullable schema change 与合法写入 | R09、R11 | 异常、正确性 | Iceberg required 列 | 拒绝增加无默认值 required 列和 nullable→required；执行 VALUES/INSERT SELECT 合法写入 | schema change 失败不产生 snapshot；合法写入与 Spark 结果一致 |
| W07 | 验证 required 列 NULL 拒绝和 statement 原子性 | R09、R11 | 隔离负向、正确性 | 隔离 Iceberg database | VALUES 写 NULL；多 bucket 源表 INSERT SELECT 混合有效与 NULL 行 | 修复前会错误提交并产生不可读文件；修复后整条语句在 snapshot 发布前拒绝 |
| W08 | 验证 STRING truncate 的 Nullable block 处理 | R03、R05、R12 | 隔离负向、稳定性 | 可重启的隔离 Doris 集群 | nullable STRING INSERT；partition evolution 后 UPDATE 产生 Nullable block | 修复前 BE FATAL；修复后写入成功并保持 NULL 分区语义 |

## P0/P1 覆盖检查

R01-R12 均映射到至少一个 P0 regression。六个正向 suite 已在双 BE 环境通过，并由 Spark/Doris 交叉校验同表结果；两个产品问题使用独立 suite 和显式隔离开关保存复现，避免默认 P0 破坏共享集群。

COW 行级 DML 属于当前明确限制，以负向 regression 固化失败语义，不标记为产品缺陷。required 列 NULL 错误提交对应 DORIS-27494；nullable STRING truncate 导致 BE FATAL 对应 DORIS-27512。
