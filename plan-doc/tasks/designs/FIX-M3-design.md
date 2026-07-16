# FIX-M3 — batch 闸门 `!isPruned` → `== NOT_PRUNED`（无谓词大分区表恢复异步 batch split）

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §3 M3。
> 批次 2（fe-core 通用节点）。**同时解 M2 登记的 BATCH-UNPRUNED-SYNC 偏差**（`FIX-M2-design.md` §登记偏差）。
> HEAD 复核（本轮，primary source）：legacy 从 git 历史取证、`SelectedPartitions` 全 producer 枚举、sibling gate 对照。

## Problem

翻闸后（`"hms"` ∈ `SPI_READY_TYPES`）分区外表走通用 `PluginDrivenScanNode`。其 batch/async split 通路由纯静态门
`shouldUseBatchMode`（`:1134`）决定。该门第一条守卫（`:1136`）用 `!selectedPartitions.isPruned`，与 legacy
`MaxComputeScanNode.isBatchMode` 的 `selectedPartitions != NOT_PRUNED` **语义不等价**：

- **无谓词分区表**（`SELECT col FROM t`，无 WHERE → `PruneFileScanPartition` 不触发）：
  `ExternalTable.initSelectedPartitions:447` 返 `new SelectedPartitions(size, fullMap, isPruned=false)`——
  一个**非 `NOT_PRUNED` 哨兵**、`isPruned=false`、**满** map 的对象。
  - legacy：`!= NOT_PRUNED` → true → batch 合格（若 `size >= num_partitions_in_batch_mode`，默认 1024）。
  - 现 SPI：`!isPruned` = `!false` = true → 守卫**返回 false → 不 batch**。
- 后果：≥1024 分区的无谓词全表扫**同步**一次性把所有分区所有文件 split 物化进 FE 堆 → 规划延迟 + 堆压
  （legacy 用异步 streaming batch）。结果正确（fail-safe），是**性能回归**。
- 范围：现只影响 **opt-in `supportsBatchScan` 的连接器**——MaxCompute（`getFileNum()>0`）与**翻闸后的 hive**
  （M2 刚加的 `supportsBatchScan`）。iceberg 走 streaming flavor（不看 `isPruned`），不受影响。
- `:1129-1132` 行内 javadoc 自称「`!isPruned` subsumes BOTH legacy gates…marginally stronger」是**错的**——
  它只对非分区（NOT_PRUNED）情形成立，恰恰漏了上面的无谓词分区情形。同文件同作者的 sibling
  `displayPartitionCounts:298` 却**正确**用 `== NOT_PRUNED`（其 javadoc `:288-296` 明确解释此正是「无谓词分区表
  `isPruned=false` 但满 map、须当已处理」的情形）——证明该分歧是**误用**而非有意设计。

## Root Cause

单点：`PluginDrivenScanNode.shouldUseBatchMode:1136` 用 `!selectedPartitions.isPruned` 作「非哨兵」判据，
但 `isPruned` 与「是否 NOT_PRUNED 哨兵」并不同义——无谓词分区表二者取值相反。

**Legacy 权威取证**（git，删除 commit `1da88365e85`，故取 `1da88365e85^`）
`.../maxcompute/source/MaxComputeScanNode.java:215-229`：
```java
public boolean isBatchMode() {
    if (table.getPartitionColumns().isEmpty()) return false;              // 非分区 → false
    if (desc.getSlots().isEmpty() || odpsTable.getFileNum() <= 0) return false;  // 无 slot / 无文件 → false
    int numPartitions = sessionVariable.getNumPartitionsInBatchMode();
    return numPartitions > 0
            && selectedPartitions != SelectedPartitions.NOT_PRUNED       // ← 权威门：!= NOT_PRUNED，非 isPruned
            && selectedPartitions.selectedPartitions.size() >= numPartitions;
}
```
- `getPartitionColumns().isEmpty()`（非分区）→ `initSelectedPartitions` 返 NOT_PRUNED → 被 `!= NOT_PRUNED` 门吸收
  （故 SPI 折叠这两门到一条 `== NOT_PRUNED` **确实无损**，此点原 javadoc 说对了）；
- `desc.getSlots().isEmpty()` → SPI `hasSlots`；`getFileNum()<=0` → 折进连接器 `supportsBatchScan`；
- `!= NOT_PRUNED` → **应落在 `:1136`，现被误写成 `!isPruned`**。

**Producer 枚举（证明修复闭合、无隐藏第三态）**——全 fe-core `new SelectedPartitions(...)`：
| 处 | isPruned | 是哨兵? | 何时 |
|----|----------|---------|------|
| `LogicalFileScan:275` NOT_PRUNED | false | **是** | 非分区 / 未剪枝 |
| `ExternalTable:447` | false | 否 | **无谓词分区表（满 map）← 到达本门的唯一分歧态** |
| `HMSExternalTable:485` | false | 否 | `initHudiSelectedPartitions`（**hudi-only**，经 translator:938 喂 `HudiScanNode`，**不经本门**）——与本修无关（红队更正：非「旧 HMS 死路」） |
| `PruneFileScanPartition:68` | true | 否 | 剪到空 |
| `PruneFileScanPartition:108` | true | 否 | 剪枝非空 |

→ 到达 `PluginDrivenScanNode` 本门、`isPruned=false` 且非哨兵的**仅** `ExternalTable:447`「无谓词分区满 map」一态
（`HMSExternalTable:485` 走 `HudiScanNode`，不经本门）。故把 `!isPruned` 换成 `== NOT_PRUNED` 后，
新增 batch 的**恰**是该态（+ 已一致的 `isPruned=true` 剪枝态），**无**别的对象被这门影响。

## Design（单点 fe-core；connector-agnostic，无源判别）

1. `PluginDrivenScanNode.shouldUseBatchMode:1136`：
   ```java
   if (selectedPartitions == null || selectedPartitions == SelectedPartitions.NOT_PRUNED) {
       return false;
   }
   ```
   对齐 legacy `!= NOT_PRUNED` 与同文件 sibling `displayPartitionCounts:298`。守卫仍 connector-agnostic
   （通用哨兵比较，非源名分支）；仅 opt-in `supportsBatchScan` 的连接器实际受益。
2. 订正 javadoc：
   - `:1122` summary bullet：`!isPruned` → `== NOT_PRUNED` 表述；
   - `:1129-1132` 段落重写——`== NOT_PRUNED` 折叠「非分区 + 未 Nereids 剪枝的空哨兵」两态；无谓词分区满 map（非哨兵）
     **落到 size≥阈值 检查**（对齐 legacy），指向 sibling `displayPartitionCounts` 的同款解释。
3. **无**其它改动：`computeBatchMode` 流式 flavor、`numApproximateSplits`、async pump 均不动。

**解 M2 BATCH-UNPRUNED-SYNC**：M2 已 override hive `supportsBatchScan`（分区∧非事务），本修使无谓词分区 hive
表也进 batch（size≥阈值时），恰是 M2 设计 §登记偏差点名「由 M3 解」的一条。

## Risk

- **结果安全**：batch 仅是 split 生成策略（异步 streaming vs 同步物化），产出 split 集/行数完全一致；不改查询结果。
- **Blast radius**：MaxCompute + 翻闸 hive 的**无谓词 ≥阈值 分区表**从同步转异步 batch。iceberg（streaming flavor）、
  非分区表、有谓词剪枝表、连接器未 opt-in `supportsBatchScan` 者——**行为不变**。
- **`numApproximateSplits`**：新 batch 态走 `:1160` 返 `selectedPartitions.size()`（满分区数，非负），对齐 legacy
  `MaxComputeScanNode.numApproximateSplits:233`（同 `selectedPartitions.size()`）。
- **EXPLAIN 回归**：batch 表 EXPLAIN 可能出现 batch/approximate 标记差异；无谓词大分区 MaxCompute/hive 用例
  （`test_max_compute_partition_prune.groovy`、`test_hive_partitions.groovy` 设 `num_partitions_in_batch_mode`）须 e2e
  真集群回归（本地无法跑，live-gated）。
- **铁律**：无新 `if(hive/iceberg)`/`instanceof`/属性解析；单点哨兵比较，通用节点 connector-agnostic。

## Test Plan

### Unit（`PluginDrivenScanNodeBatchModeTest`，fe-core 有 Mockito）
- **反转** `testUnprocessedPruningNeverBatches`（`:86-95`）→ 更名 `testUnprocessedPruningStillBatches`：
  `new SelectedPartitions(THRESHOLD, items(THRESHOLD), false)`（= `initSelectedPartitions:447` 对无谓词分区表的产物）
  在 `hasSlots=true, supportsBatchScan=true, threshold=THRESHOLD` 下断言 **`assertTrue`**。
  注释改为编码 WHY：无谓词分区全扫是**最需**异步 batch 的态（对齐 legacy `!= NOT_PRUNED`）；此前 `assertFalse` 编码的
  正是 M3 回归。**RED-able**：现码 `!isPruned` 返 false → 该 `assertTrue` 现会失败。
- `testNotPrunedNeverBatches`（NOT_PRUNED 哨兵，空 map）、`testNullSelectionNeverBatches` 保持 `assertFalse`
  （`== NOT_PRUNED` 守卫仍挡哨兵与 null）——守住「非分区 / 空哨兵不 batch」。
- 其余门测（hasSlots / supportsBatchScan / threshold / 边界）不变。

### E2E（live-gated，须真集群；memory `hms-iceberg-delegation-needs-e2e`）
- MaxCompute ≥阈值 无谓词分区表 `SELECT col FROM odps_t`：断言进 batch（EXPLAIN/规划无 OOM）；
- 翻闸 hive ≥阈值 无谓词分区表：同断言（BATCH-UNPRUNED-SYNC 已解）；
- 二者结果与同步路径逐行一致（batch 不改行）。
- 登记为 gated（本模块无 live harness，DV-019 同类）。

## 设计红队（`wf_811e6242-d8b`，3 lens 对抗）+ 最终确认

**verdict**：BLAST-RADIUS = **SOUND**（无遗漏 producer、行不变、仅 hive+MaxCompute 无谓词 ≥阈值 分区表受影响）；
LEGACY-PARITY = **SOUND_WITH_CHANGES**（MaxCompute 核心 parity **精确**、一行修无须改动，仅 doc 措辞更正）；
COMPLETENESS = **SOUND_WITH_CHANGES**（命中 1 blocker + 1 major，均已解，见下）。核心一行修**无争议**：restores legacy
`!= NOT_PRUNED`，producer 枚举证闭合。

**折入的更正：**
1. **（已改上表）** `HMSExternalTable:485` 是 hudi initializer 走 `HudiScanNode`，非「旧 HMS 死路」——不到达本门。
2. **hive parity 仅「方向性」非「精确」**：legacy `HiveScanNode.isBatchMode`（git `785ae1d7dda:290-306`）**无** NOT_PRUNED/isPruned
   门、阈值用 `>= 0`（vs SPI `> 0`）、`numApproximateSplits = numSplitsPerPartition × partitions`（vs SPI = 分区数）。
   这两处差异**均 pre-existing**（本一行修不碰阈值算子、不碰 numApproximateSplits），故不驳斥本修；但本 doc 的 hive parity 叙述
   限定为「恢复无谓词场景的 batch **方向**」，非「与 legacy hive 全等」。**MaxCompute** 才是本修的精确 parity 目标
   （legacy `MaxComputeScanNode` 同样 `!= NOT_PRUNED` + `numApproximateSplits = 分区数`）。

**blocker（docker-hive golden，已解）**：`test_hive_partitions.groovy:200` 是 docker-gated（`enableHiveTest`）checked-in 断言
`(approximate)inputSplitNum=60`，**非**「deferred live-gated e2e」。该表 6 分区、`num_partitions_in_batch_mode=1` 强制 batch、
**期望** `(approximate)` 前缀——即它本身就是「无谓词全表扫应 batch」的旁证。翻闸+M2 后闸门 `!isPruned` 挡掉 batch → 前缀消失 →
**已 red**。本修恢复 batch → 前缀回；但 batch 模式 `selectedSplitNum = numApproximateSplits()`（`FileQueryScanNode:383`）=
分区数 `6`（非 legacy 的 `10×6=60`）。**用户 2026-07-11 签字：采用 SPI 统一口径（近似分片数=分区数）**，golden `60→6`
（对齐 MaxCompute、Trino「引擎层统一报分片」思路；不给 hive 补 split-count 估算）。**docker-gated，本地不可跑 → 源 golden 已改，
真值须真集群回归**（sweep 确认全 regression-test 仅此 1 处 `(approximate)` 断言受影响；maxcompute p2 只断言 `partition=N/M`/结果，不受影响）。

**major（supersession，已登记非静默）**：`!isPruned` 门 + 其 pinning 测试同随 commit `1da88365e85` 引入；
**LP-1**（P4-T06e impl-review，nit）曾判 `!isPruned` vs `!= NOT_PRUNED`「等价且略强」、**TQ-2** 特意留
`testUnprocessedPruningNeverBatches` 钉住 `!isPruned`，签为 **D-035 / DV-019**。本修**证明该「等价」为假**（无谓词分区表二者相反），
反转该测试为 `testNoPredicatePartitionedTableBatches`（assertTrue）。**已在 `decisions-log.md` D-035 / `deviations-log.md` DV-019
补 SUPERSEDED 批注**（非静默；对齐 M5「推翻先前签字」的登记纪律）。

## 最终改动清单（本条落地）
- `PluginDrivenScanNode.java:1136` `!isPruned` → `== NOT_PRUNED` + javadoc（summary bullet + 段落）重写。
- `PluginDrivenScanNodeBatchModeTest.java`：`testUnprocessedPruningNeverBatches`→`testNoPredicatePartitionedTableBatches`
  （assertFalse→assertTrue，注释编码 WHY + supersession）；订正 `testNotPrunedNeverBatches` 注释（`!isPruned`→`== NOT_PRUNED`）
  + class-javadoc「must be pruned」措辞。
- `test_hive_partitions.groovy:200` golden `60→6`（+ 注释解释 SPI 口径）。
- `decisions-log.md` D-035 / `deviations-log.md` DV-019：补 SUPERSEDED 批注。
