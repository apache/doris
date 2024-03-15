---
{
    "title": "BE OOM分析",
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

# BE OOM分析

<version since="1.2.0">

理想情况下，在 [Memory Limit Exceeded Analysis](./memory-limit-exceeded-analysis.md) 中我们定时检测操作系统剩余可用内存，并在内存不足时及时响应，如触发内存GC释放缓存或cancel内存超限的查询，但因为刷新进程内存统计和内存GC都具有一定的滞后性，同时我们很难完全catch所有大内存申请，在集群压力过大时仍有OOM风险。

## 解决方法
参考 [BE 配置项](../../../admin-manual/config/be-config.md) 在`be.conf`中调小`mem_limit`，调大`max_sys_mem_available_low_water_mark_bytes`。

## 内存分析
若希望进一步了解 OOM 前BE进程的内存使用位置，减少进程内存使用，可参考如下步骤分析。

1. `dmesg -T`确认 OOM 的时间和 OOM 时的进程内存。

2. 查看 be/log/be.INFO 的最后是否有 `Memory Tracker Summary` 日志，如果有说明 BE 已经检测到内存超限，则继续步骤3，否则继续步骤8
```
Memory Tracker Summary:
    Type=consistency, Used=0(0 B), Peak=0(0 B)
    Type=batch_load, Used=0(0 B), Peak=0(0 B)
    Type=clone, Used=0(0 B), Peak=0(0 B)
    Type=schema_change, Used=0(0 B), Peak=0(0 B)
    Type=compaction, Used=0(0 B), Peak=0(0 B)
    Type=load, Used=0(0 B), Peak=0(0 B)
    Type=query, Used=206.67 MB(216708729 B), Peak=565.26 MB(592723181 B)
    Type=global, Used=930.42 MB(975614571 B), Peak=1017.42 MB(1066840223 B)
    Type=tc/jemalloc_cache, Used=51.97 MB(54494616 B), Peak=-1.00 B(-1 B)
    Type=process, Used=1.16 GB(1246817916 B), Peak=-1.00 B(-1 B)
    MemTrackerLimiter Label=Orphan, Type=global, Limit=-1.00 B(-1 B), Used=474.20 MB(497233597 B), Peak=649.18 MB(680718208 B)
    MemTracker Label=BufferAllocator, Parent Label=Orphan, Used=0(0 B), Peak=0(0 B)
    MemTracker Label=LoadChannelMgr, Parent Label=Orphan, Used=0(0 B), Peak=0(0 B)
    MemTracker Label=StorageEngine, Parent Label=Orphan, Used=320.56 MB(336132488 B), Peak=322.56 MB(338229824 B)
    MemTracker Label=SegCompaction, Parent Label=Orphan, Used=0(0 B), Peak=0(0 B)
    MemTracker Label=SegmentMeta, Parent Label=Orphan, Used=948.64 KB(971404 B), Peak=943.64 KB(966285 B)
    MemTracker Label=TabletManager, Parent Label=Orphan, Used=0(0 B), Peak=0(0 B)
    MemTrackerLimiter Label=DataPageCache, Type=global, Limit=-1.00 B(-1 B), Used=455.22 MB(477329882 B), Peak=454.18 MB(476244180 B)
    MemTrackerLimiter Label=IndexPageCache, Type=global, Limit=-1.00 B(-1 B), Used=1.00 MB(1051092 B), Peak=0(0 B)
    MemTrackerLimiter Label=SegmentCache, Type=global, Limit=-1.00 B(-1 B), Used=0(0 B), Peak=0(0 B)
    MemTrackerLimiter Label=DiskIO, Type=global, Limit=2.47 GB(2655423201 B), Used=0(0 B), Peak=0(0 B)
    MemTrackerLimiter Label=ChunkAllocator, Type=global, Limit=-1.00 B(-1 B), Used=0(0 B), Peak=0(0 B)
    MemTrackerLimiter Label=LastSuccessChannelCache, Type=global, Limit=-1.00 B(-1 B), Used=0(0 B), Peak=0(0 B)
    MemTrackerLimiter Label=DeleteBitmap AggCache, Type=global, Limit=-1.00 B(-1 B), Used=0(0 B), Peak=0(0 B)
```

3. 当 OOM 前 be/log/be.INFO 的最后包含系统内存超限的日志时，参考 [Memory Limit Exceeded Analysis](./memory-limit-exceeded-analysis.md) 中的日志分析方法，查看进程每个类别的内存使用情况。若当前是`type=query`内存使用较多，若已知 OOM 前的查询继续步骤4，否则继续步骤5；若当前是`type=load`内存使用多继续步骤6，若当前是`type=global`内存使用多继续步骤7。

4. `type=query`查询内存使用多，且已知 OOM 前的查询时，比如测试集群或定时任务，重启BE节点，参考 [Memory Tracker](./memory-tracker.md) 查看实时 memory tracker 统计，`set global enable_profile=true`后重试查询，观察具体算子的内存使用位置，确认查询内存使用是否合理，进一步考虑优化SQL内存使用，比如调整join顺序。

5. `type=query`查询内存使用多，且未知 OOM 前的查询时，比如位于线上集群，则在`be/log/be.INFO`从后向前搜`Deregister query/load memory tracker, queryId` 和 `Register query/load memory tracker, query/load id`，同一个query id若同时打出上述两行日志则表示查询或导入成功，若只有 Register 没有 Deregister，则这个查询或导入在 OOM 前仍在运行，这样可以得到OOM 前所有正在运行的查询和导入，按照步骤4的方法对可疑大内存查询分析其内存使用。

6. `type=load`导入内存使用多时。

7. `type=global`内存使用多时，继续查看`Memory Tracker Summary`日志后半部分已经打出得`type=global`详细统计。当 DataPageCache、IndexPageCache、SegmentCache、ChunkAllocator、LastSuccessChannelCache 等内存使用多时，参考 [BE 配置项](../../../admin-manual/config/be-config.md) 考虑修改cache的大小；当 Orphan 内存使用过多时，如下继续分析。
  - 若`Parent Label=Orphan`的tracker统计值相加只占 Orphan 内存的小部分，则说明当前有大量内存没有准确统计，比如 brpc 过程的内存，此时可以考虑借助 heap profile [Memory Tracker](https://doris.apache.org/zh-CN/community/developer-guide/debug-tool) 中的方法进一步分析内存位置。
  - 若`Parent Label=Orphan`的tracker统计值相加占 Orphan 内存的大部分，当`Label=TabletManager`内存使用多时，进一步查看集群 Tablet 数量，若 Tablet 数量过多则考虑删除过时不会被使用的表或数据；当`Label=StorageEngine`内存使用过多时，进一步查看集群 Segment 文件个数，若 Segment 文件个数过多则考虑手动触发compaction；

8. 若`be/log/be.INFO`没有在 OOM 前打印出`Memory Tracker Summary`日志，说明 BE 没有及时检测出内存超限，观察 Grafana 内存监控确认BE在 OOM 前的内存增长趋势，若 OOM 可复现，考虑在`be.conf`中增加`memory_debug=true`，重启集群后会每秒打印集群内存统计，观察 OOM 前的最后一次`Memory Tracker Summary`日志，继续步骤3分析；

</version>
