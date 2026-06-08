# FIX-BATCH-MODE-SPLIT 设计（P3-11 / NG-7 / F6=F13）

> 严重度：🟡 minor（性能/内存，行正确）。**用户拍板（2026-06-08）：实现 batch SPI 路径（非 DV）、design-first（本文档供评审、过目后再进实现）。**
> 来源：`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` §A NG-7。
> recon：workflow `wiczf63pp`（5 agent，A legacy 机器 / B 消费侧契约 / C SPI 面 / D 通用节点闸门 / E Batch-D 红线）。
> **状态：⬜ 待评审 — 未动任何代码。**

---

## Problem

翻闸后 `PluginDrivenScanNode` 不 override `isBatchMode/numApproximateSplits/startSplit`，继承 `SplitGenerator`
默认（`isBatchMode()=false`、`numApproximateSplits()=-1`、`startSplit()=no-op`），故 plugin-driven（含 MaxCompute）
读路径**永远走同步 `getSplits()`**：一次性同步枚举**全部已裁剪分区**的所有 split。legacy `MaxComputeScanNode:214-298`
对多分区表**分批异步**建 read session、经 `SplitAssignment` 流式喂 split。

**影响（P1-4 落地后已收窄）**：现在同步路径是「单 session 跨**已裁剪**分区集」（非全分区）。残留降级仅在**裁剪后仍命中
大量分区**（≥ `num_partitions_in_batch_mode`，默认 1024）时显现：规划同步阻塞、无流式、单大 session → 大分区表
规划慢 + 内存大、潜在 OOM。纯效率/内存，**行结果正确**。

## Root Cause

通用插件层缺口：batch-mode 的消费侧 dispatch（`isBatchMode==true` → `SplitAssignment.init()` → `startSplit()`
异步喂 split）只在 `FileQueryScanNode.createScanRangeLocations:369-413` 实现，而其触发完全依赖 ScanNode 子类
override `isBatchMode/numApproximateSplits/startSplit`。`PluginDrivenScanNode` 三者皆未 override → 死走非-batch。
同时现有 SPI `ConnectorScanPlanProvider` 纯同步（仅 `planScan` 系列返回 `List`），无按分区分批/流式入口。

## 关键预核（recon 已证，决定可行性）

- ✅ **`PluginDrivenScanNode extends FileQueryScanNode`**（`:86`）→ **已继承** batch dispatch 分支（`FileQueryScanNode:369-413`）
  + `stop()` 拆解（`:689-698` 关 `SplitAssignment` + 注销 `SplitSource`）。**无需新建 ScanNode 类型、无需复制 dispatch**。
- ✅ **`PluginDrivenSplit extends FileSplit`**（`PluginDrivenSplit.java:35`），legacy `MaxComputeSplit` 同（`:29`）。
  故 batch 路径 `FileQueryScanNode:381` 的 `(FileSplit) splitAssignment.getSampleSplit()` 硬转型**安全**（否则 ClassCastException）。
- ✅ **`SplitAssignment.addToQueue` 守空**（`:143-146` `if (splits.isEmpty()) return;`）→ 某分区批 0 split 不崩。
  **【SF-2 设计验证修正】** 区分两种「空」：
  - **非空选但每批 0 split**（可达）→ 守空 + `startSplit` finally 的完成计数仍触发 `finishSchedule()`（`numFinished==total`）→
    `init()` 因 `!needMoreSplit()` 以 `sampleSplit==null` 退出 → `FileQueryScanNode:378` 当空扫，**无挂死**。
  - **全空选**（`selectedPartitions.isEmpty()`，`startSplit` 提前 return **不**调 `finishSchedule`，镜像 legacy `:241-244`）→
    该分支在 batch 模式下**不可达**（`isBatchMode` 要求 `size() >= numPartitions >= 1`，见 isBatchMode 闸），
    仅为 legacy 保真保留的 **dead-code-by-invariant**；故不存在「全空选经 startSplit 致 `init()` 30s 挂死」路径。
- ✅ legacy `isBatchMode` 4 个闸门输入：3 个通用可得（分区列=`selectedPartitions!=NOT_PRUNED`、slots=`desc.getSlots()`、
  阈值=`sessionVariable.getNumPartitionsInBatchMode()` vs `selectedPartitions.size()`），**仅 `odpsTable.getFileNum()>0` 需经 SPI 暴露**。

## Design — Shape A（薄 SPI + fe-core 编排，逐字镜像 legacy）

recon C 在 3 个候选（A 薄 SPI / B callback-sink / C iterator）中**强推 A**：连接器零 fe-core 类泄漏、其余 6 连接器默认不动、
与 legacy byte-identical、唯一真实消费者（MaxCompute）。详见「替代方案」节。

### (1) SPI 改动（additive，零破坏）—— `ConnectorScanPlanProvider`（fe-connector-api）加两个 default

```java
/** 连接器级 batch 资格闸（替代 legacy odpsTable.getFileNum()>0）。默认 false → 其余连接器走同步路。 */
default boolean supportsBatchScan(ConnectorSession session, ConnectorTableHandle handle) {
    return false;
}

/** 单分区批 → 单 read session → 该批 ConnectorScanRange。默认委托 planScan(6 参) over 子集，
 *  故已正确实现 6 参 planScan 的连接器（MaxCompute）无需 override 本方法。
 *  ⚠️ 默认委托仅对「planScan(6 参) 按分区集建一个 session」语义的连接器正确；若未来 full-adopter 的
 *  planScan 非按分区集分片，需 override 本方法 + supportsBatchScan 才允许开 batch（否则保持默认 false）。 */
default List<ConnectorScanRange> planScanForPartitionBatch(
        ConnectorSession session, ConnectorTableHandle handle,
        List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter,
        long limit, List<String> partitionBatch) {
    return planScan(session, handle, columns, filter, limit, partitionBatch);
}
```

### (2) 连接器改动（MaxComputeScanPlanProvider）—— **仅 1 个 override**

```java
@Override
public boolean supportsBatchScan(ConnectorSession session, ConnectorTableHandle handle) {
    // 镜像 legacy MaxComputeScanNode:220-221 的 odpsTable.getFileNum()>0
    return <从 handle 取 odpsTable>.getFileNum() > 0;
}
```

`planScanForPartitionBatch` **不 override**：默认委托 `planScan(6 参)`，而 MaxCompute 的 `planScan(6 参)` 对给定分区集
正是「建一个 TableBatchReadSession over 该子集 → 该批 split」（recon C），与 legacy `createTableBatchReadSession(子集)` 同形。
**parity 必验项**（impl/review）：连接器 `planScan` 的 session 构建逐字等同 legacy `createTableBatchReadSession`
（ArrowOptions MILLI/MICRO、splitOptions、required cols/partitions、filterPredicate）。

### (3) fe-core 改动（PluginDrivenScanNode）—— 3 个 override 原子落地（镜像 `MaxComputeScanNode:214-298`）

> ⚠️ **三者必须一起加**：只加 `isBatchMode` 会令节点进 batch 分支但 `startSplit` no-op + `numApproximateSplits=-1`
> → `init()` 挂 30s 后抛 "Failed to get first split" + "Approximate split number should not be negative"（recon D）。

```java
@Override
public boolean isBatchMode() {
    if (selectedPartitions == null || !selectedPartitions.isPruned) return false; // 非分区/未裁剪
    if (desc.getSlots().isEmpty()) return false;
    // 【SF-1 设计验证】getScanPlanProvider() 默认 null（Connector.java:41-43）；isBatchMode 跑在
    // dispatch（FileQueryScanNode:369）+ explain（FileScanNode:142）两路径、对每个 plugin-driven scan 执行，
    // 无 SPI provider 的 full-adopter 会 NPE。镜像 getSplits():391 既有 null-guard。
    ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
    if (scanProvider == null || !scanProvider.supportsBatchScan(connectorSession, currentHandle)) {
        return false;
    }
    int numPartitions = sessionVariable.getNumPartitionsInBatchMode();
    return numPartitions > 0 && selectedPartitions.selectedPartitions.size() >= numPartitions;
}

@Override
public int numApproximateSplits() {
    return selectedPartitions == null ? -1 : selectedPartitions.selectedPartitions.size();
}

@Override
public void startSplit(int numBackends) {
    this.totalPartitionNum = selectedPartitions.totalPartitionNum;
    this.selectedPartitionNum = selectedPartitions.selectedPartitions.size();
    if (selectedPartitions.selectedPartitions.isEmpty()) {
        return; // 无数据可读（镜像 legacy :241-244）
    }
    // 与 getSplits 同序做 projection + filter 下推；【DEC-1】batch 不下推 limit（镜像 legacy 批路径忽略 limit）
    final List<ConnectorColumnHandle> columns = buildColumnHandles();
    tryPushDownProjection(columns);
    final Optional<ConnectorExpression> remainingFilter = buildRemainingFilter();
    final ConnectorTableHandle handle = currentHandle;          // 异步前 capture（projection 已改完 currentHandle）
    final ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
    final List<String> allPartitions = new ArrayList<>(selectedPartitions.selectedPartitions.keySet());
    final int batchSize = sessionVariable.getNumPartitionsInBatchMode();

    Executor scheduleExecutor = Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor();
    AtomicReference<UserException> batchException = new AtomicReference<>(null);
    AtomicInteger numFinished = new AtomicInteger(0);

    CompletableFuture.runAsync(() -> {                          // OUTER：驱动批循环（镜像 legacy :258-296）
        for (int begin = 0; begin < allPartitions.size(); begin += batchSize) {
            int end = Math.min(begin + batchSize, allPartitions.size());
            if (batchException.get() != null || splitAssignment.isStop()) break;
            List<String> batch = allPartitions.subList(begin, end);
            int curBatchSize = end - begin;
            try {
                CompletableFuture.runAsync(() -> {              // INNER：每批建 session→喂 split
                    try {
                        List<ConnectorScanRange> ranges = scanProvider.planScanForPartitionBatch(
                                connectorSession, handle, columns, remainingFilter, -1L, batch);
                        List<Split> batchSplits = new ArrayList<>(ranges.size());
                        for (ConnectorScanRange r : ranges) batchSplits.add(new PluginDrivenSplit(r));
                        if (splitAssignment.needMoreSplit()) splitAssignment.addToQueue(batchSplits);
                    } catch (Exception e) {
                        batchException.set(new UserException(e.getMessage(), e));
                    } finally {
                        if (batchException.get() != null) splitAssignment.setException(batchException.get());
                        if (numFinished.addAndGet(curBatchSize) == allPartitions.size()) {
                            splitAssignment.finishSchedule();
                        }
                    }
                }, scheduleExecutor);
            } catch (Exception e) {
                batchException.set(new UserException(e.getMessage(), e));
            }
            if (batchException.get() != null) splitAssignment.setException(batchException.get());
        }
    }, scheduleExecutor);
}
```

非-batch `getSplits()` **保持不动**（含 P3-9 limit-opt + P1-4 pruned-to-zero 短路）；本设计纯加 batch 分支。

## 设计决策（请评审）

- **DEC-1：batch 路径不下推 limit（`planScanForPartitionBatch(..., -1L, batch)`）。** 镜像 legacy——legacy `startSplit`
  的 `createTableBatchReadSession` 从不应用 limit；limit-opt 仅在非-batch `getSplits` 的 `getSplitsWithLimitOptimization`。
  传 -1 使 MaxCompute `planScan` 的 `shouldUseLimitOptimization`（要求 `limit>0`，见 P3-9/D-032）不触发 → **batch 与
  limit-opt 互斥**（recon C 警示二者会撞）。实践中二者本就少同现（limit-opt 要 onlyPartitionEquality→通常选少分区<阈值）。
- **DEC-2：fileNum 闸门走新 `supportsBatchScan` capability**（默认 false），而非复用 `estimateScanRangeCount>0`。
  后者语义是「并行度预估」、默认 -1，借用会模糊语义；专用布尔更清晰、对其余连接器默认安全。
- **DEC-3：executor 复用 `ExtMetaCacheMgr.getScheduleExecutor()` + outer-driver/inner-batch 嵌套结构逐字照搬**
  （recon A 警示：同一有界池跑 outer+N inner 有 starvation 风险，但这是 legacy 既有语义，须保持一致、不另起池）。
- **DEC-4：`isBatchMode()` 结果建议字段缓存**（mirror IcebergScanNode `:992-1027`）——它在 dispatch / explain / 多处被读，
  且 `num_partitions_in_batch_mode` 是 `fuzzy=true`（测试随机 0..1024），重算会令 dispatch 与 explain 脱钩。

## Risk Analysis

- **并发/生命周期契约**（recon B，最高风险）：`startSplit` 必须严守 `SplitAssignment` 协议——loop on `needMoreSplit()`、
  `addToQueue` 推、正常结束 `finishSchedule()`、异常 `setException()`、尊重 `isStop()` 早退；`numApproximateSplits()≥0`
  （否则 `FileQueryScanNode:384` 抛）；`init()` 阻塞 30s 等首 split，故须快出首 split 或快 finish/except。上面代码逐字镜像 legacy 满足之。
- **handle 线程可见性**：projection 下推在异步 submit **前**同步改完 `currentHandle`，已 capture 进 final 局部，异步只读 → 安全。
- **空批/全空选**：非空选每批 0 split → `addToQueue` 守空 + 完成计数 `finishSchedule` → 空扫无挂；全空选分支在 batch gate 下**不可达**（dead-code-by-invariant，见预核 SF-2）。
- **【SF-1】provider-less 连接器 NPE**：`isBatchMode` 必须 null-guard `getScanPlanProvider()`（默认 null）——它跑在 dispatch+explain 两路径、对所有 full-adopter 执行。已在设计 isBatchMode 加守卫 + 补 truth-table null-provider 行。
- **限定不溢出到其余连接器**：SPI 两 default 均 false/委托，其余 6 连接器（es/jdbc/hive/paimon/hudi/trino）字节不变（recon C 已核）。
- **测试 harness 缺位**：`PluginDrivenScanNode` 是 `FileQueryScanNode` 子类、裸构造需绕 ctor + stub 大量依赖，且 batch 路径
  涉及真 `SplitAssignment`/executor/RPC（同 [DV-015] harness 缺位）→ batch wiring 的 offline 直测受限，逻辑半可单测、
  端到端真值待 live（见 Test Plan + 拟登 DV-019）。

## Test Plan

### Unit Tests（逻辑半，可 offline）
- `isBatchMode()` 真值表：非裁剪→false、空 slots→false、**null provider→false（SF-1，mirror getSplits:391）**、
  `supportsBatchScan=false`→false、`size<阈值`→false、`size≥阈值且全闸过`→true（**pin `num_partitions_in_batch_mode`**，
  因 fuzzy 随机；编码 WHY=大分区裁剪集才批，per Rule 9）。
- `numApproximateSplits()` = `selectedPartitions.size()`（含 null 防御）。
- mutation：闸门各条件取反 → 对应 test 变红；`numApproximateSplits` 常量化 → 红。
- SPI default：`supportsBatchScan` 默认 false、`planScanForPartitionBatch` 默认委托 `planScan`（连接器 api 层测）。

### 受限/待 live（拟登 DV-019）
- `startSplit` 的 async 批循环 + `SplitAssignment` 喂 split + executor + 30s/异常/isStop 路径 → 无轻量 harness，
  逻辑由「逐字镜像 legacy + 上述不变式 UT」+ live e2e 守。

### E2E（CI-skip，真值闸）
- 大分区表（裁剪后 ≥ `num_partitions_in_batch_mode`）：`EXPLAIN`/profile 证 **batched/streamed** split 生成
  （`(approximate)` 标记 + `inputSplitNum` 近似 + 规划耗时/内存 ≪ 同步路）；行结果与同步路一致。
- 阈值/资格边界：`num_partitions_in_batch_mode` 设 0 / 大于选中分区数 → 走非-batch（回归 getSplits）。
- 全空选 + 单分区 → 正常空扫 / 单批。

## Batch-D 红线（recon E，必须写入）

**Batch-D 红线**：legacy `MaxComputeScanNode` 的 batch-mode 逻辑（`MaxComputeScanNode.java:214-298` 的
`isBatchMode`/`numApproximateSplits`/`startSplit` 异步分批建 read session + 流式喂 split）是**唯一逻辑副本**，
只能在**本 P3-11 通用 batch SPI 路径落地后**才允许删除；在此之前 Batch-D 设计 §1 对 `source/MaxComputeScanNode`
的「zero survivor risks」声明**不成立**。

- 读裁剪那半红线（`MaxComputeScanNode:718-731`）已由 FIX-PRUNE-PUSHDOWN（`072cd545c54`）清除 → **P3-11 是删
  `MaxComputeScanNode` 的最后一道前置闸**（第 5 道，前 4 道 overwrite/write-dist/bind/prune 均已落）。
- **附带动作**：对 `P4-batchD-maxcompute-removal-design.md` §1（≈`:45`/`:63`）的 `source/MaxComputeScanNode`
  「zero survivor」声明加一行限定（dead-code-after-flip 仅指实例化链；read-pruning 已清、batch-mode 待 P3-11），
  交叉引用 HANDOFF `:64` 与各 per-fix 红线。

## 设计验证（clean-room，workflow `wcpg9lblj`）

4 lens（correctness/concurrency、legacy-parity、SPI/blast-radius、test/red-line）独立审 → 每 finding 3 skeptic 对抗 verify
（≥2 票判真才留）→ synthesis。**结论 GO-WITH-EDITS：0 mustFix、2 shouldFix（已折入本文档）、17 rejected**。

- **SF-1（3/3，真 NPE）**：`isBatchMode` 漏 `getScanPlanProvider()` null-guard（默认 null、跑 dispatch+explain 两路径）
  → 已加守卫镜像 `getSplits:391` + 补 truth-table null-provider 行。**唯一有运行期影响的修正。**
- **SF-2（2/3，doc-only）**：预核「全空选 finishSchedule 仍触发」与 startSplit 提前 return 自相矛盾 → 已改为
  dead-code-by-invariant（batch gate 下不可达）+ 区分「非空选每批 0 split」可达路径。
- **17 rejected**：含 2 个 near-miss（planScanForPartitionBatch 默认委托对非分区分片 adopter 的陷阱 1/3、DEC-4 缓存 1/3）
  → 均 <2/3，已顺手在 SPI 注释加一行 caveat（前者），无须 action。
- legacy-parity / 并发契约 / blast-radius / Batch-D 红线核心判定**均通过**（无 confirmed 反对）。

## 实现 + 守门（已落）

- **改动**：SPI `ConnectorScanPlanProvider` +2 default（`supportsBatchScan` false / `planScanForPartitionBatch` 委托 6 参 planScan）；
  连接器 `MaxComputeScanPlanProvider.supportsBatchScan`=`odpsTable.getFileNum()>0`（`planScanForPartitionBatch` 不 override，继承默认）；
  fe-core `PluginDrivenScanNode` +`isBatchMode`/`computeBatchMode`(SF-1 null-guard)/纯静态 `shouldUseBatchMode`/`numApproximateSplits`/`startSplit`
  + `isBatchModeCache` 字段 + imports（Env/CompletableFuture/Executor/Atomic*）。
- **守门**：编译 BUILD SUCCESS（fe-connector-api+maxcompute+fe-core）；fe-core UT 9/9；fe-connector-api UT 2/2；checkstyle 0；
  import-gate 净；**mutation 5/5 向红**（A `!isPruned`→`false` / B `!hasSlots` flip / C `!supportsBatchScan` flip / D `>0`→`>=0` / E `>=`→`>`）。
- **operational 坑（auto-memory 记）**：mutation 跑中 `/mnt/disk1` 系统级 100% 满（非本 repo 数据，target 仅 3.65G）致 cp 还原失败一度 truncate 产线文件；已从 RAM(`/dev/shm`) 备份还原、D/E 重跑确认。教训：mutation 还原备份须放 RAM/异盘，勿与构建同盘。

## impl-review（clean-room，workflow `wve7y1jst`，3 lens + 对抗 verify）

**结论 GO-WITH-EDITS：0 mustFix、1 shouldFix、2 nit（6 rejected），均注释/文档级、无产线逻辑改**：
- **TQ-1（shouldFix，3/3）**：测试 javadoc 过度声称 SF-1 null-provider 已覆盖——实则 9 测全调纯静态 `shouldUseBatchMode`（传预算 `supportsBatchScan` 布尔），从不经 `computeBatchMode` 的 null-guard。**修=诚实降级**（option b）：改测试注释不再声称覆盖 + 把 null-guard 与 `startSplit` async 记为 live-only/DV-019 gap（构造 `PluginDrivenScanNode` 需本模块缺位的 harness）。
- **LP-1（nit，2/3）**：`!isPruned` vs legacy 引用 `!= NOT_PRUNED`——等价且略强（非分区表恒携 NOT_PRUNED）。修=`shouldUseBatchMode` javadoc 加注。
- **TQ-2（nit，2/3）**：`testNotPrunedNeverBatches` 对 `!isPruned` guard 非判别（NOT_PRUNED 空 map，0>=阈值恒 false）；真正杀手是 `testUnprocessedPruningNeverBatches`。修=注释挑明。
- legacy-parity / 并发契约 / SPI blast-radius 核心判定均通过（无 confirmed 反对）。

## Implementation Plan（评审通过后）
1. SPI：`ConnectorScanPlanProvider` 加 `supportsBatchScan` + `planScanForPartitionBatch` 两 default。
2. 连接器：`MaxComputeScanPlanProvider.supportsBatchScan` override（fileNum>0）；核 `planScan` session 构建 parity。
3. fe-core：`PluginDrivenScanNode` 加 `isBatchMode`/`numApproximateSplits`/`startSplit`（+ isBatchMode 字段缓存）。
4. UT + mutation（逻辑半）；checkstyle + import-gate + 连接器编译 BUILD SUCCESS。
5. Batch-D 设计 doc 加红线限定行。
6. clean-room 设计验证 workflow（多 lens 对抗）→ impl-review workflow 收敛 → 独立 commit + hash 回填 + D-035/DV-019。

## 替代方案（recon C 提供，留档）
- **Shape B（callback sink）**：连接器侧 push，新增 `ConnectorScanRangeSink` 类型，连接器自控批大小/顺序/async。
  优：真流式背压在连接器内。劣：新 SPI 类型 + 连接器须精确实现线程/生命周期契约、batching 策略与 scan-node 既得信息重复、
  难 byte-identical legacy（生命周期所有权移入连接器）。
- **Shape C（lazy iterator）**：`Iterator<List<ConnectorScanRange>> planScanBatched(...)`，`startSplit` 拉取喂 SplitAssignment。
  优：纯返回值扩展、连接器 pull/可单测。劣：异常须经 `next()` 透传（包 unchecked）、对唯一消费者过度泛化。
- **DV-only（不实现）**：原 HANDOFF 建议，已被用户否决（用户定「实现」）。

## 关联
- 决策 [D-035]（待）、偏差 [DV-019]（待，wiring harness 缺位）
- 复审 [§A NG-7](../../reviews/P4-maxcompute-full-rereview-2026-06-07.md)、[READ-C5](../../reviews/P4-cutover-review-findings.md)
- 前置 [FIX-PRUNE-PUSHDOWN 设计](./P4-T06e-FIX-PRUNE-PUSHDOWN-design.md) / [D-031]；Batch-D [removal 设计](../P4-batchD-maxcompute-removal-design.md)
- recon 全量证据：workflow `wiczf63pp`
