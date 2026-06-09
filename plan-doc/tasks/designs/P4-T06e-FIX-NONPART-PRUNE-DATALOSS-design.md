# [P4-T06e] FIX-NONPART-PRUNE-DATALOSS (GAP8) — design

> 来源：Batch-D 红线扩充对抗复审 workflow `wbw4xszrg`（schema-table unit）。用户定 **Fix now，repro-test 先行**。
> 关联 auto-memory：[[catalog-spi-nonpartitioned-prune-dataloss]]。

## Problem

翻闸后，对**非分区** max_compute 表执行带 WHERE 的查询静默返回 **0 行**：

```sql
SELECT * FROM mc_catalog.db.non_partitioned_tbl WHERE col > 5;   -- 0 行（错！应返回匹配行）
SELECT * FROM mc_catalog.db.non_partitioned_tbl;                 -- 正常（无 WHERE，规则不触发）
```

行正确性回归（静默丢行），非性能问题。仅影响走 `LogicalFileScan` + `PluginDrivenScanNode` 的插件表——当前=MaxCompute（翻闸后唯一 live 的 PluginDriven 文件扫描连接器）。

## Root Cause（已 5 处核码确认）

| # | 位置 | 行为 |
|---|---|---|
| 1 | `PluginDrivenExternalTable.supportInternalPartitionPruned()` :205-212 | 返 `!getPartitionColumns().isEmpty()` → **非分区表 = false**。注释「observably equivalent to true（initSelectedPartitions returns NOT_PRUNED either way）」**只对了 init 一半**。 |
| 2 | `ExternalTable.initSelectedPartitions()` :440 | `!supportInternalPartitionPruned()` → 初始 `NOT_PRUNED`(isPruned=false)。故 `PruneFileScanPartition` 的 `whenNot(isPruned)` 放行，**规则会触发**（有 filter 时）。 |
| 3 | `PruneFileScanPartition.build()` :64-69 | 触发后 `if (supportInternalPartitionPruned())` = **false → else 支** 覆写 `selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true)`（isPruned=**true**，空 map）。 |
| 4 | `PhysicalPlanTranslator.visitPhysicalFileScan()` :761 | 对每个 PluginDriven scan **无条件** `setSelectedPartitions(fileScan.getSelectedPartitions())`。 |
| 5 | `PluginDrivenScanNode.resolveRequiredPartitions()` :172-176 + `getSplits()` :409-412 | isPruned=true + 空 map → 返**空 list（非 null）**；`getSplits`：`requiredPartitions != null && isEmpty()` → `Collections.emptyList()` → **0 split → 0 行**。 |

**两 commit 叠加**：
- 坏 override 来自 `35cfa50f988 [P4-T06d] FIX-PART-GATES`——当时 **dormant**（彼时 `getSplits` 不读 selectedPartitions，isPruned=true+空 无害）。
- `072cd545c54 [P4-T06e] P1-4 FIX-PRUNE-PUSHDOWN` 加的「isPruned+空 → 0 split 短路」**激活**了 dormant 坑。短路本意是「分区表裁剪到 0 分区」（如 `WHERE pt='不存在'`），未料**非分区**表也落 isPruned=true+空。

**为何 CI 没抓**：
- 单测 `PluginDrivenScanNodePartitionPruningTest:97` 只钉静态 helper（`emptyPruned → 空 list`）= **钉住了错的不变式**（违 Rule 9：测试无法在业务逻辑错时失败）。
- live e2e `test_max_compute_partition_prune.groovy` 只测**分区**表；非分区+WHERE 无覆盖。
- 仅 MaxCompute 走 `PluginDrivenScanNode`（jdbc/es/trino 非 PluginDrivenExternalTable、不产 LogicalFileScan），故未在别处暴露。

## Blast radius（已核 + 设计验证 `wijd3qgk0` 更正）

- **无类 extends PluginDrivenExternalTable**（grep 0 hit）——override 仅 `PluginDrivenExternalTable` 实例命中。
- ⚠️ **更正原稿「仅 MaxCompute / 注释 aspirational」**：`CatalogFactory.SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute}`（:51-52），这 4 类**任一**连接器 provider 加载时即建 `PluginDrivenExternalCatalog` → 表为 `PluginDrivenExternalTable`（TableType `PLUGIN_EXTERNAL_TABLE`）→ `BindRelation:543-544` 产 `LogicalFileScan` → `PhysicalPlanTranslator:753` 路由 `PluginDrivenScanNode`（**首匹配**）。故本 override + 本 bug 是**通用插件层**问题，**非 MaxCompute 专有**：任何非分区 SPI 驱动表 + WHERE 都会 0 行。**当前仅 MaxCompute 被翻闸/加载暴露**（jdbc/es 在本分支多半未加载 SPI provider，走降级/legacy 故未现）。Option A 对全部 4 类**中性或有益、绝不有害**（非分区 → pruneExternalPartitions 返 NOT_PRUNED → 扫全表）。
- `PruneFileScanPartition` 只匹配 `logicalFileScan()`；HMS/Iceberg/LakeSoul/RemoteDoris 各有**自己**的 `supportInternalPartitionPruned`，不受本 override 影响。
- **MV-path consumer（已核 benign=parity 恢复）**：改 true 后非分区 PluginDriven 表在 `QueryPartitionCollector:75` 从 ELSE(ALL_PARTITIONS) 转 `else-if`（读空 NOT_PRUNED map 的 keySet=空集，无 NPE），`PartitionCompensator:246` 不再 early-return false。**安全**——legacy `MaxComputeExternalTable:83-84` 即无条件 true（`IcebergExternalTable` 同），翻闸前非分区 MC MV 基表本就走这些 true 分支 ⇒ **恢复 legacy parity，非新回归**（`PartitionCompensator:84` 另对 UNPARTITIONED MV early-return，进一步限暴露）。

## Design

**Option A（选用）— `PluginDrivenExternalTable.supportInternalPartitionPruned()` 返无条件 `true`**，镜像 legacy `MaxComputeExternalTable.supportInternalPartitionPruned()`（:82-85 返 true）。

为何安全且正确：
- 非分区：`PruneFileScanPartition` 走 `if` 支 → `pruneExternalPartitions()` :78 见 `getPartitionColumns().isEmpty()` → **返 `NOT_PRUNED`**（isPruned=false）→ `resolveRequiredPartitions` 返 null → 扫全表。✅ 修复。
- 分区：true vs `!isEmpty()`=true → **零变化**（既有路径不动）。
- `initSelectedPartitions` :443 对空分区列也返 `NOT_PRUNED`，与现状一致（init 不变）。
- 这是与 legacy `MaxComputeExternalTable` 的**最忠实 parity**（legacy 即无条件 true）。

**Defensive guard（设计验证定夺：不纳入）**：legacy `MaxComputeScanNode.getSplits():720` 另有 `!getPartitionColumns().isEmpty() && != NOT_PRUNED` 守卫（legacy 双保险），翻闸时该 consumer 侧守卫被丢、未由 Option A 恢复。但设计验证 Lens-4 确认：Option A 在**源头**修复（规则不再对非分区 PluginDriven 表产 isPruned=true+空），故 consumer 守卫**对正确性冗余**。`PluginDrivenScanNode.getSplits:409-412` 短路确「盲信」不变式（isPruned+空 只来自分区表裁剪到 0），但该不变式现由 Option A 维护、且与 `PluginDrivenScanNode:486-489` 自身注释声明一致。**Rule 2/3 取舍 → 只做 Option A**（不加冗余 guard；若 impl-review 认为 data-loss 路径值得 defense-in-depth 再议）。

**被否方案**：
- Option C（改 `PruneFileScanPartition` else 支返 NOT_PRUNED）：该 else 支是**通用**（所有 file-scan 表 supportInternalPartitionPruned=false 时走）→ 动 HMS/Iceberg 等，blast radius 过大，违 Rule 3。
- 改 `resolveRequiredPartitions` 把空 list 当 null：会破坏「分区表真裁剪到 0 分区 → 0 行」的 P1-4 正确语义（`WHERE pt='不存在'` 应返 0 行）。否。

## Implementation Plan（折入设计验证 mustFix/shouldFix）

1. **Fix（一行）**：`PluginDrivenExternalTable.supportInternalPartitionPruned()` 改返无条件 `true`；改写误导注释（:206-211）——新不变式=无条件 true 镜像 legacy `MaxComputeExternalTable`；为何对非分区安全=`PruneFileScanPartition` 走 IF 支 → `pruneExternalPartitions:78` 见空分区列返 `NOT_PRUNED`（**不**走 else 支的 isPruned=true+空 → 不会触发 `PluginDrivenScanNode` 0-split 短路 → 不丢行）。
2. **【mustFix，设计验证 Lens-2】翻转钉错不变式的现有测**：`PluginDrivenExternalTablePartitionTest.testNonPartitionedTableReportsNoPartitionsAndNoPruning:98` 现 `assertFalse(supportInternalPartitionPruned())`（WHY 注释 :95-97 明文为 buggy 值辩护「must NOT opt into pruning」「mutation→true makes red」）。**改为 `assertTrue(...)` + 重写 WHY**：非分区表必须 opt-in 才能让 PruneFileScanPartition 走 NOT_PRUNED 安全支、避免 else 支 isPruned=true+空 → 静默 0 行的 data-loss 链。**此翻转本身即 repro**（修前该断言对现 false 为绿、对 fix 后 true 为红——即它当前钉住 bug；翻转后 mutation 还原 fix→红）。
3. **【test-adequacy，设计验证 Lens-3】repro 主用轻量单测、不强求全 rule-transform**：决定性 bug 面是单方法 `supportInternalPartitionPruned`。step-2 的翻转断言（非分区→true）即**主 repro**（buildable=复用 `tableWithCacheValue` 既有 harness:250-270，非空依赖；非真空=mutation 还原即红）。`PruneFileScanPartition.build().transform(...)` 全链路需真 `CascadesContext`、fe-core 无既有 pattern 可抄 → **不作主测**（可选：若 `PlanChecker`/`MemoTestUtils` 能轻量驱动则补一条「非分区+filter→scan-all」集成测，否则归 e2e/DV）。
4. **保留 helper 契约测 + 加注释**：`PluginDrivenScanNodePartitionPruningTest:92-100` 的 `emptyPruned→空 list` 测**保留**（契约对**真分区**表裁剪到 0 正确），加注释澄清「此态只应来自真分区表裁剪；非分区表经 Option A 永不到此（否则 0 行 data-loss）」+ 指向 step-2。
5. **真值闸 e2e（CI 跳）**：`regression-test/suites/.../test_mc_nonpartitioned_filter.groovy` 非分区 MC 表 `SELECT ... WHERE` 返正确非空行集。

## Risk Analysis

| Risk | Mitigation |
|---|---|
| 改 true 影响 `QueryPartitionCollector`/`PartitionCompensator` 对非分区 PluginDriven 表 | 设计验证核这两 consumer 在非分区(空分区列)下为 no-op；UT 守 + 无 MV-on-MC 既有用例回归。 |
| 改 true 误伤别的 PluginDriven 文件连接器（Hudi-SPI） | Hudi-SPI DV-006 deferred/未 wire；且 true 对非分区任何连接器都正确（pruneExternalPartitions 自处理）。 |
| repro 测 harness 过重/不可建 | 退化为最小集成测（构造非分区 PluginDriven LogicalFileScan 直跑规则）；至少钉「rule 后 resolveRequiredPartitions==null」。 |
| 分区表回归 | true vs 现状对分区表零差异；既有 `PluginDrivenScanNodePartitionPruningTest` + p2 `test_max_compute_partition_prune` 守。 |

## Test Plan

### Unit Tests
- **新增 repro**（fe-core）：非分区 PluginDriven 表 + filter 经 `PruneFileScanPartition` → `resolveRequiredPartitions(scan.selectedPartitions) == null`。先红后绿。
- **mutation**：把 fix 还原（true→`!isEmpty()`）须令 repro 测变红。
- 既有 `PluginDrivenScanNodePartitionPruningTest` 全绿（helper 契约不变）。

### E2E Tests（CI 跳，真实 ODPS = 真值闸）
- 非分区 MC 表 `SELECT ... WHERE <谓词>` 返回**正确非空行集**（修前 0 行）。归入 DV 真值闸（live ODPS）。
- **实现分歧（impl-review 记，非缺陷）**：未新建 `test_mc_nonpartitioned_filter.groovy`，改**扩既有 `test_max_compute_partition_prune.groovy`**——更优：复用 `enable_profile×num_partitions×cross_partition` 矩阵，非分区案例在全模式下被覆盖。加 `no_partition_tb`(id 1..5) DDL 入 seed 注释块 + 直接 `assertEquals` 行数断言（WHERE id=5→1 行 / id>=3→3 行 / full→5 行；无 .out 依赖；gated on enableMaxComputeTest）。**需用户在 ODPS `mc_datalake` 建 `no_partition_tb` 后 live 跑** = DV-021。

## 守门结果（DONE）
编译 BUILD SUCCESS；UT 6/6+5/5 绿；mutation 还原 fix→repro 红→恢复绿；checkstyle 0；import-gate exit 0。设计验证 `wijd3qgk0`(4 lens 全 design-sound,1mF+3sF 折入) + impl-review `wza2khdb2`(2 lens approve,0mF,2 nit 修)。详见 `plan-doc/reviews/P4-T06e-FIX-NONPART-PRUNE-DATALOSS-review-rounds.md`。

## 决策类型
明确修复（用户定 Fix，repro 先行）。连接器无关、纯 fe-core 通用插件层、无 SPI 变更。
