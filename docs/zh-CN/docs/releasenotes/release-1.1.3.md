---
{
    "title": "Release 1.1.3",
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



作为 1.1.2 LTS（Long-term Support，长周期支持）版本基础之上的 Bugfix 版本，在 Apache Doris 1.1.3 版本中，有超过 80 个 Issue 或性能优化项被合入，优化了在导入或查询过程中的内存控制，修复了许多导致 BE Core 以及产生错误查询结果的问题，系统稳定性和性能得以进一步加强，推荐所有用户下载和使用。

# 新增功能

- 在 ODBC 表中支持 SQLServer 和 PostgreSQL 的转义标识符。

- 支持使用 Parquet 作为导出文件格式。

# 优化改进

- 优化了 Flush 策略以及避免过多 Segment 小文件。 [#12706](https://github.com/apache/doris/pull/12706) [#12716](https://github.com/apache/doris/pull/12716)

- 重构 Runtime Filter 以减少初始准备时间。 [#13127](https://github.com/apache/doris/pull/13127)

- 修复了若干个在查询或导入过程中的内存控制问题。 [#12682](https://github.com/apache/doris/pull/12682) [#12688](https://github.com/apache/doris/pull/12688) [#12708](https://github.com/apache/doris/pull/12708) [#12776](https://github.com/apache/doris/pull/12776) [#12782](https://github.com/apache/doris/pull/12782) [#12791](https://github.com/apache/doris/pull/12791) [#12794](https://github.com/apache/doris/pull/12794) [#12820](https://github.com/apache/doris/pull/12820) [#12932](https://github.com/apache/doris/pull/12932) [#12954](https://github.com/apache/doris/pull/12954) [#12951](https://github.com/apache/doris/pull/12951)

# Bug 修复

- 修复了 largeint 类型在 Compaction 过程中导致 Core 的问题。 [#10094](https://github.com/apache/doris/pull/10094)

- 修复了 Grouping set 导致 BE Core 或者返回错误结果的问题。 [#12313](https://github.com/apache/doris/pull/12313)

- 修复了使用 orthogonal_bitmap_union_count 函数时执行计划 PREAGGREGATION 显示错误的问题。 [#12581](https://github.com/apache/doris/pull/12581)

- 修复了 Level1Iterator 未被释放导致的内存泄漏问题。 [#12592](https://github.com/apache/doris/pull/12592)

- 修复了当 2 BE 且存在 Colocation 表时通过 Decommission 下线节点失败的问题。 [#12644](https://github.com/apache/doris/pull/12644)

- 修复了 TBrokerOpenReaderResponse 过大时导致堆栈缓冲区溢出而导致的 BE Core 问题。 [#12658](https://github.com/apache/doris/pull/12658)

- 修复了出现 -238错误时 BE 节点可能 OOM 的问题。 [#12666](https://github.com/apache/doris/pull/12666)

- 修复了 LEAD() 函数错误子表达式的问题。 [#12587](https://github.com/apache/doris/pull/12587)

- 修复了行存代码中相关查询失败的问题。 [#12712](https://github.com/apache/doris/pull/12712)

- 修复了 curdate()/current_date() 函数产生错误结果的问题。 [#12720](https://github.com/apache/doris/pull/12720)

- 修复了 lateral View explode_split 函数出现错误结果的问题。 [#13643](https://github.com/apache/doris/pull/13643)

- 修复了两张相同表中 Bucket Shuffle Join 计划错误的问题。 [#12930](https://github.com/apache/doris/pull/12930)

- 修复了更新或导入过程中 Tablet 版本可能错误的问题。 [#13070](https://github.com/apache/doris/pull/13070)

- 修复了在加密函数下使用 Broker 导入数据时 BE 可能发生 Core 的问题。 [#13009](https://github.com/apache/doris/pull/13009)

# 升级说明

默认情况下禁用 PageCache 和 ChunkAllocator 以减少内存使用，用户可以通过修改配置项 `disable_storage_page_cache` 和 `chunk_reserved_bytes_limit` 来重新启用。

Storage Page Cache 和 Chunk Allocator 分别缓存用户数据块和内存预分配。

这两个功能会占用一定比例的内存，并且不会释放。 这部分内存占用无法灵活调配，导致在某些场景下，因这部分内存占用而导致其他任务内存不足，影响系统稳定性和可用性。因此我们在 1.1.3 版本中默认关闭了这两个功能。

但在某些延迟敏感的报表场景下，关闭该功能可能会导致查询延迟增加。如用户担心升级后该功能对业务造成影响，可以通过在 be.conf 中增加以下参数以保持和之前版本行为一致。

```
disable_storage_page_cache=false
chunk_reserved_bytes_limit=10%
```

* `disable_storage_page_cache`：是否关闭 Storage Page Cache。 1.1.2（含）之前的版本，默认是false，即打开。1.1.3 版本默认为 true，即关闭。
* `chunk_reserved_bytes_limit`：Chunk allocator 预留内存大小。1.1.2（含）之前的版本，默认是整体内存的 10%。1.1.3 版本默认为 209715200（200MB）。
