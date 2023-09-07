---
{
    "title": "Release 1.1.5",
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

在 1.1.5 版本中，Doris 团队已经修复了自 1.1.4 版本发布以来约 36 个问题或性能改进项。同时，1.1.5 版本也是作为 1.1 LTS 版本的错误修复版本，建议所有用户升级到这个版本。


# Behavior Changes


当别名与原始列名相同时，例如 "select year(birthday) as birthday"，在 group by、order by、having 子句中使用别名时将与 MySQL 中保持一致，Group by 和 having 将首先使用原始列，order by 将首先使用别名。这里可能会对用户带来疑惑，因此建议最好不要使用与原始列名相同的别名。

# Features

支持 Hash 函数 murmur_hash3_64。[#14636](https://github.com/apache/doris/pull/14636)

# Improvements

为日期函数 convert_tz 添加时区缓存以提高性能。[#14616](https://github.com/apache/doris/pull/14616)

当调用 show 子句时，按 tablename 对结果进行排序。 [#14492](https://github.com/apache/doris/pull/14492)

# Bug Fix

修复 if 语句中带有常量时导致 BE 可能 Coredump 的问题。[#14858](https://github.com/apache/doris/pull/14858)

修复 ColumnVector::insert_date_column 可能崩溃的问题 [#14839](https://github.com/apache/doris/pull/14839)

更新 high_priority_flush_thread_num_per_store 默认值为 6，将提高负载性能。 [#14775](https://github.com/apache/doris/pull/14775)

优化 quick compaction core。 [#14731](https://github.com/apache/doris/pull/14731)

修复分区列非 duplicate key 时 Spark Load 抛出 IndexOutOfBounds 错误的问题。
 [#14661](https://github.com/apache/doris/pull/14661)

修正 VCollectorIterator 中的内存泄漏问题。 [#14549](https://github.com/apache/doris/pull/14549)

修复了存在 Sequence 列时可能存在的建表问题。 [#14511](https://github.com/apache/doris/pull/14511)

使用 avg rowset 来计算批量大小，而不是使用 total_bytes，因为它要花费大量的 Cpu。 [#14273](https://github.com/apache/doris/pull/14273)

修复了 right outer join 可能导致 core 的问题。[#14821](https://github.com/apache/doris/pull/14821)

优化了 TCMalloc gc 的策略。 [#14777](https://github.com/apache/doris/pull/14777) [#14738](https://github.com/apache/doris/pull/14738) [#14374](https://github.com/apache/doris/pull/14374)


