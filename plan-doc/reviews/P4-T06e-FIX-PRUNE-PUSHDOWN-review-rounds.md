# P4-T06e — FIX-PRUNE-PUSHDOWN review 轮次记录

> Issue: DG-1 / F1=F7（分区裁剪从未推到 ODPS read session）
> 设计：[P4-T06e-FIX-PRUNE-PUSHDOWN-design.md](../tasks/designs/P4-T06e-FIX-PRUNE-PUSHDOWN-design.md)
> review 编排脚本：[prune-pushdown-review.workflow.js](./prune-pushdown-review.workflow.js)（clean-room，pipeline finder→adversarial verifier）

---

## 前置：recon（根因 + blast-radius 调查）

workflow `wszm3u9fv`（8 agent，Map 5 reader + Verify 3 lens）+ 主 loop 独立核码（clean-room：先 code 判断，后核对历史）。

**根因 3/3 lens 无法证伪**（translator-path / spi-channel / correctness）：
- `PhysicalPlanTranslator:753-758`（plugin 分支）从不调 `setSelectedPartitions`（对比 Hive `:773` / legacy-MC `:797` / Hudi `:882`）；
- `PluginDrivenScanNode` 无 selectedPartitions 字段；`planScan` 5 参签名无分区通道；
- `MaxComputeScanPlanProvider` 恒传 `Collections.emptyList()`（`:201`/`:320`）→ ODPS session 跨全分区。
- **返回行仍正确**（MaxCompute 未 override `applyFilter`→conjunct 不清→BE 重算）→ **纯性能/内存回归**。
- FE 元数据半边 FIX-PART-GATES **已落**；缺的是 translator→SPI→connector 透传（原 READ-C2 修复建议「②」）。

---

## Round 1 — converged（workflow `w31i0vfo5`，11 agent；4 lens × finder→verifier）

**配置**：4 lens（parity / correctness / blast-radius / test-quality），每 lens finder 产 finding → 每 finding 1 adversarial verifier（默认 mustFix=false，须独立核码证其为真且 must-fix）。

**结论**：**7 verdict，0 must-fix，0 blocker/major 存活 → 1 轮收敛。**

### 存活 real findings（4，全 test-quality，全非 must-fix）

| # | sev(claimed→verdict) | 标题 | 处置 |
|---|---|---|---|
| 1 | blocker→**minor** | translator `setSelectedPartitions` 注入无 UT | 接受。与既有约定一致（`HiveScanNodeTest` 亦不经 translator 测，直构 node 调 setter）；fail-safe（默认 NOT_PRUNED→scan all，非丢数据）；DV-015 live e2e 为真值门。 |
| 2 | major→**minor** | `getSplits()` pruned-to-zero 短路无 UT | 接受。短路是 correctness 不变式，但 code 正确；其逻辑半（三态 resolve）已被 `resolveRequiredPartitions` UT + mutation pin；wiring 半由 DV-015 live 覆盖（同 P0-3/DV-014 先例）。 |
| 3 | major→**minor** | `getSplits→planScan` requiredPartitions threading 无集成测 | 接受。同 #2；threading 是单变量直线流（无分支/转换），最易错的三态映射已单测。 |
| 4 | minor→**minor** | 5 参 planScan→6 参委托无测 | 接受。trivial forwarder；语义契约在 SPI default 方法；`toPartitionSpecs(null)≡toPartitionSpecs([])` 已证等价→该 mutation 行为惰性。连接器模块无 Mockito（建议 fix 不可实现）。 |

### 证伪 findings（3，isReal=false）

- **Hudi-SPI plugin 分支未接 setSelectedPartitions**（claimed major）→ 证伪。`CatalogFactory.SPI_READY_TYPES` 不含 hudi → 该分支生产不可达（真 Hudi 走 legacy HMS 路 `:886` 已设）；且 `HudiScanPlanProvider` 仅实现 4 参 planScan，default 委托丢 requiredPartitions → 即便接也惰性；**设计已显式登记为 scope 边界（DV-006 deferred）**，非本 fix 引入。
- **maxcompute 无 read-session 集成测**（claimed major）→ 证伪。两 createReadSession call site 均喂同一 `requiredPartitionSpecs` 变量（直线流，无 hardcoded emptyList 残留）；连接器模块无 Mockito + session builder 需 live ODPS → 正确分层（逻辑半 fe-core 测、转换半 maxcompute 测、live 半 DV-015）。
- **mutation 覆盖不全**（claimed minor）→ 证伪。设计列的 3 个 mutation **全被现有 UT 杀**（已 mutation 实测：maxcompute toPartitionSpecs→emptyList 红；fe-core 去 isPruned 双红）；tests 已带 WHY 注释（Rule 9）。

### key 裁决（verifier 跨 lens 一致）

- **parity**：三态映射（NOT_PRUNED→all / pruned-非空→subset / pruned-空→短路）镜像 legacy `MaxComputeScanNode.getSplits():718-731`；`toPartitionSpecs`=legacy `new PartitionSpec(key)`；**两** read-session 路径（标准+limit-opt）均接 requiredPartitions（=legacy getSplits + getSplitsWithLimitOptimization）。无分歧。
- **blast-radius**：additive 6 参 default overload；es/jdbc/hive/paimon/hudi/trino **零改**（继承 default 委托回各自 planScan）；既有 4/5 参调用方不破。唯一 override=MaxCompute。
- **correctness**：纯性能/内存回归，行正确（conjunct BE 重算；null/empty=scan-all、非空=subset、空=fe-core 短路三态清晰；默认 NOT_PRUNED 保非裁剪/非 MC 行为不变）。

## 守门（clean source）

- compile：fe-connector-api + fe-connector-maxcompute + fe-core 3 模块绿。
- UT：fe-core `PluginDrivenScanNodePartitionPruningTest` 5/5；maxcompute `MaxComputeScanPlanProviderTest` 3/3。
- mutation：① fe-core 去 `!isPruned` 守卫 → `testNotPrunedScansAllPartitions`+`testUnprocessedPruningScansAllPartitions` 双红（`expected:<null> but was:<[]>`/`<[pt=1]>`）；② maxcompute `toPartitionSpecs`→恒 emptyList → `testConvertsPartitionNamesToSpecs` 红（`<2> vs <0>`）。均还原。
- checkstyle 0×3；import-gate 净。

## KNOWN-LIMITATION（→ DV-015）

`getSplits()` 短路 + translator `setSelectedPartitions` 注入 + planScan threading 无 fe-core 端到端 UT（连接器 scan 无轻量 analyze/spy harness；与 P0-3/DV-014 同因）。逻辑半（`resolveRequiredPartitions` 三态 + `toPartitionSpecs` 转换）已 UT+mutation pin；wiring 半 + 真实裁剪生效由 **DV-015 live e2e** 覆盖（`test_max_compute_partition_prune.groovy`，p2；真值证据=EXPLAIN/profile 仅扫目标分区 + `WHERE pt='不存在'`→0 行不建全分区 session）。
