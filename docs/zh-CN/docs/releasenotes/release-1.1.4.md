---
{
    "title": "Release 1.1.4",
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



作为 1.1 LTS（Long-term Support，长周期支持）版本基础之上的 Bugfix 版本，在 Apache Doris 1.1.4 版本中，Doris 团队修复了自 1.1.3 版本以来的约 60 个 Issue 或性能优化项。改进了 Spark Load 的使用体验，优化了诸多内存以及 BE 异常宕机的问题，系统稳定性和性能得以进一步加强，推荐所有用户下载和使用。

# 新增功能

- Broker Load 支持 华为云 OBS 对象存储。[#13523](https://github.com/apache/doris/pull/13523)

- Spark Load 支持 Parquet 和 Orc 文件。[#13438](https://github.com/apache/doris/pull/13438)


# 优化改进

- 禁用 Metric Hook 中的互斥量，其将影响数据导入过程中的查询性能。 [#10941](https://github.com/apache/doris/pull/10941)


# Bug 修复

- 修复了当 Spark Load 加载文件时 Where 条件不生效的问题。 [#13804](https://github.com/apache/doris/pull/13804)

- 修复了 If 函数存在 Nullable 列时开启向量化返回错误结果的问题。 [#13779](https://github.com/apache/doris/pull/13779)

- 修复了在使用 Anti Join 和其他 Join 谓词时产生错误结果的问题。 [#13743](https://github.com/apache/doris/pull/13743)

- 修复了当调用函数 concat(ifnull)时 BE 宕机的问题。 [#13693](https://github.com/apache/doris/pull/13693)

- 修复了 group by 语句中存在函数时 planner 错误的问题。 [#13613](https://github.com/apache/doris/pull/13613)

- 修复了 lateral view 语句不能正确识别表名和列名的问题。 [#13600](https://github.com/apache/doris/pull/13600)

- 修复了使用物化视图和表别名时出现未知列的问题。 [#13605](https://github.com/apache/doris/pull/13605)

- 修复了 JSONReader 无法释放值和解析 allocator 内存的问题。 [#13513](https://github.com/apache/doris/pull/13513)

- 修复了当 enable_vectorized_alter_table 为 true 时允许使用 to_bitmap() 对负值列创建物化视图的问题。 [#13448](https://github.com/apache/doris/pull/13448)

- 修复了函数 from_date_format_str 中微秒数丢失的问题。 [#13446](https://github.com/apache/doris/pull/13446)

- 修复了排序 exprs 的 nullability 属性在使用子 smap 信息进行替换后可能不正确的问题。 [#13328](https://github.com/apache/doris/pull/13328)

- 修复了 case when 有 1000 个条件时出现 Core 的问题。 [#13315](https://github.com/apache/doris/pull/13315)

- 修复了 Stream Load 导入数据时最后一行数据丢失的问题。 [#13066](https://github.com/apache/doris/pull/13066)

- 恢复表或分区的副本数与备份前相同。 [#11942](https://github.com/apache/doris/pull/11942)
