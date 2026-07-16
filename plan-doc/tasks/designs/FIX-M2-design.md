# FIX-M2 — 翻闸后 hive 丢批量/异步 split → 连接器补 batch 通路

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §3 M2。
> recon+对抗红队：`wf_40498e52-19f`（SOUND_WITH_CHANGES：**非** trivial 照抄 MaxCompute——须**额外** override `planScanForPartitionBatch` 否则 batch 重复 split；红队证实机制 + 要求登记两条偏差）。

## Problem

翻闸后（`"hms"` ∈ `SPI_READY_TYPES`）每张 hive 表走通用 `PluginDrivenScanNode`。该节点仅在连接器 opt-in 时进
batch/async split。`HiveScanPlanProvider` 两 flavor 都不 opt-in → 大分区 hive 扫描把所有选中分区的所有文件 split
一次性同步物化进 FE 堆——hive 是最大分区数外部源，回归最重。结果正确（fail-safe），FE 堆 + 规划延迟回归。

## Root Cause

`PluginDrivenScanNode.computeBatchMode` 仅在连接器返 `streamingSplitEstimate>=0`（iceberg-only）或
`supportsBatchScan==true`（MaxCompute-only）时开 batch。`HiveScanPlanProvider` 两者都不 override → 继承
`supportsBatchScan=false`/`streamingSplitEstimate=-1` → `shouldUseBatchMode` 恒 false → 同步 `getSplits`。legacy
`HiveScanNode.isBatchMode` 按 `prunedPartitions.size() >= num_partitions_in_batch_mode`(默认 1024) 异步、无 opt-in 门。

**关键 nuance（非照抄 MaxCompute）**：MaxCompute 只 override `supportsBatchScan`、靠 SPI **默认**
`planScanForPartitionBatch`——正确仅因其 `planScan` **partition-set-scoped**（消费 `requiredPartitions`）。hive 的
`planScan` **非** partition-set-scoped：从 `handle.getPrunedPartitions()` 解析、**忽略** 传入分区集。
`PluginDrivenScanNode` 把 Nereids 剪枝分区**名**切 batch、每 batch 调
`planScanForPartitionBatch(...,batch)`。若 hive 继承默认 → 每 batch 重跑全剪枝集 `planScan` → 每分区文件被 emit
`num_batches` 次 → **重复行**（正确性 bug）。故 hive **必须也** override `planScanForPartitionBatch` 把解析 scope 到 batch。

batch 串 = Nereids selected-partition map 的键 = `ConnectorPartitionInfo.getPartitionName()` = HMS 渲染
`"key=value/..."` 名 = `hmsClient.getPartitions(db,table,names)` 接受、连接器 `applyFilter` 已用的形式 → batch-scoped
解析 = 干净 `getPartitions(batch)` 往返。

## Design（`HiveScanPlanProvider` 两连接器局部 override；无 fe-core 改）

1. `supportsBatchScan(session, handle)` → true iff **分区表**（`getPartitionKeyNames()` 非空）**且非事务**
   （`!isTransactional()`）。**ACID 刻意排除**：其扫描在 `planScan`/`planAcidScan` 开一个 metastore 读事务；batch 路
   在后台池线程 per-batch 跑 `planScanForPartitionBatch`，路由 ACID 会 per-batch 开→泄漏读事务。ACID 分区表保持同步
   路径（正确、只是不流式）。排除用的 `isTransactional()` = `planScan` 分支 ACID 的同一 accessor。
2. `planScanForPartitionBatch(session, handle, columns, filter, limit, partitionBatch)` → 仅解析 batch 分区：
   `hmsClient.getPartitions(db, table, partitionBatch)` → `convertPartitions` → 格式/split-size/hadoopConf 检测
   （与 `planScan` 逐字节同）→ `listAndSplitFiles` per 分区 → 返回该 batch ranges。复用 `planScan` 同款 helper，
   ~30 行、无新 import。ACID 路不可达（`supportsBatchScan` 已排除）。
- 订正 class-javadoc `:62` stale bullet。

**为何不改 `planScan` 为 requiredPartitions-scoped**：会重做非-batch 路（读 getPrunedPartitions）、扩大 blast
radius；targeted `planScanForPartitionBatch` override 是外科选择。

## 登记偏差（fail-safe，结果不变；Rule 12 显式登记非静默）

- **BATCH-ACID-SYNC（永久·by-design）**：ACID 分区表保持同步（legacy 曾对 ACID 也 batch）。per-batch-ACID-事务
  方案会破坏单读事务不变式 / 泄漏共享读锁——已否。仅罕见 ACID+≥1024 分区丢异步。
- **BATCH-UNPRUNED-SYNC（**由 M3 解**，批次 2）**：真正无过滤扫描（无 WHERE → `PruneFileScanPartition` 不触发 →
  `initSelectedPartitions` 返 `isPruned=false`）→ fe-core `shouldUseBatchMode` 现用 `!isPruned` 恒 false → 保持同步；
  legacy 无条件 batch。**非连接器可修**——是 fe-core `isPruned` 门；M3 的 `==NOT_PRUNED` 修复正是让无过滤扫描也 batch。

## Risk

- 线程：`planScanForPartitionBatch` 在 `scheduleExecutor` 线程用线程安全共享 `HiveFileListingCache`、每调新 `Configuration`、
  每 batch 一次 `getPartitions` RPC（legacy 付等价 per-分区列举成本）。TCCL 由 `PluginDrivenScanNode.onPluginClassLoader` pin。
- 名保真：batch 名直传 `getPartitions`（无 unescape/比较）→ hudi-H1 转义陷阱不适用（往返恒等）。
- Nereids vs 连接器剪枝：batch 仅在 `isPruned` 且 size≥阈值时触发；两者同谓词剪枝 → batch 集 == 同步集（不增删行）。
- Scope：无 fe-core 改、无新 import、`planScan`/`planAcidScan` 不动。

## Test

- Unit `HiveScanBatchModeTest`（无 Mockito；复用 `HiveFileListingCache.DirectoryLister` counting fake + `FakeHmsClient`
  echo + `FakeSession`）4 测：`supportsBatchScan` 分区非事务→true / 非分区→false / 事务分区→false；
  `planScanForPartitionBatch` 携全 3 剪枝分区、batch=`[year=2024/month=01]` → `ranges.size()==1` **且** lister 仅列该分区。
  - RED：`supportsBatchScan` 继承默认 false；batch hook 继承默认→委派 `planScan`→解析全 3→3 ranges/3 location→`size==1` 挂。
    `size==1` 编码 WHY（Rule 9）：非 partition-scoped（重复 split bug）即挂。
  - 结果：4/4 + hive 模块 284/284 绿、0 checkstyle、import 门 0。
- E2E live-gated（docker HMS/HDFS + 真 FE+BE）：分区数 > `num_partitions_in_batch_mode` 且带谓词仍留≥阈值幸存者的表，
  断言 (a) 行数正确无重复、(b) 走异步（EXPLAIN 显 `isBatchMode()` 下的 `(approximate) inputSplitNum` 前缀）；可 A/B 把
  阈值设高于分区数（同步）比对结果一致。
