# P4-T06e — FIX-PRUNE-PUSHDOWN 设计文档

> Issue: **DG-1 / F1=F7**（`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` §B DG-1）
> 决策类型：**明确修复**（用户 2026-06-07 批准「Fix it」，见 task-list-P4-rereview.md P1-4）
> 跨轮更新；review 轮次见 `plan-doc/reviews/P4-T06e-FIX-PRUNE-PUSHDOWN-review-rounds.md`

---

## Problem

翻闸后 MaxCompute 走通用 `PluginDrivenScanNode` 读路径。Nereids 的分区裁剪结果（`SelectedPartitions`）**被计算出来但在 translator 被丢弃**，从未传到 ODPS read session：`MaxComputeScanPlanProvider` 永远以 `requiredPartitions=Collections.emptyList()` 建 `TableBatchReadSession` → **ODPS storage session 建在全分区上**。大分区表 SELECT 退化为整表枚举（规划慢、session+split 内存大、潜在 OOM）。

**非正确性 bug**：返回行仍正确（MaxCompute 未 override `applyFilter` → `convertPredicate` 不清 conjunct；`getScanNodePropertiesResult` 默认 `hasConjunctTracking=false` → `pruneConjunctsFromNodeProperties` 早退 → 全部 conjunct 序列化到 BE 重算）。**纯性能/内存回归**。3 lens 对抗复审（translator-path / spi-channel / correctness）一致无法证伪（recon workflow `wszm3u9fv`）。

## Root Cause

裁剪链路三处断点（全部 file:line 核实 @2026-06-07）：

1. **translator 丢弃**：`PhysicalPlanTranslator.java:753-758`（plugin 分支）调 `PluginDrivenScanNode.create(...)` **从不**调 `setSelectedPartitions`。对比同方法 legacy 分支：Hive `:773`、legacy-MC `:797`、Hudi `:882` 均传 `fileScan.getSelectedPartitions()`。
2. **scan node 无承接**：`PluginDrivenScanNode.java` **无** `selectedPartitions` 字段/setter；`getSplits():370-371` 调 `planScan(session, handle, columns, remainingFilter, limit)` —— SPI 5 参签名**无分区通道**。
3. **connector 恒传空**：`MaxComputeScanPlanProvider.java:201`（标准路径）和 `:320`（limit-opt 路径）`createReadSession(..., Collections.emptyList(), ...)`。

**注**：FE 元数据半边 **已由 FIX-PART-GATES 落地**（`PluginDrivenExternalTable` 已 override `supportInternalPartitionPruned/getPartitionColumns/getNameToPartitionItems`，`:205-265`），故 Nereids 确实算出裁剪集——只缺 translator→SPI→connector 的端到端透传（即原 review READ-C2 修复建议的「②」半，从未实现）。connector 内部管线**已就绪**：`createReadSession` 已接 `requiredPartitions` 参并喂 `.requiredPartitions(...)`（`:244`），仅被恒喂空集。

**legacy 参照**（`MaxComputeScanNode.java`）：`selectedPartitions` 字段（`:109`，translator `:797` 注入）→ `getSplits():718-731` 三态处理：
- `!isPruned`（`!= NOT_PRUNED`）→ `requiredPartitionSpecs` 留空 → 「读全部分区」；
- pruned 非空 → `selectedPartitions.forEach((key,v)-> add(new PartitionSpec(key)))`；
- pruned 空（`:724-727`）→ **return 空结果**（不读任何分区）。

## Design

**核心思路**：复刻 legacy 三态语义，以 **additive default-method overload** 扩 SPI（零破坏其余 6 连接器），把 Nereids `SelectedPartitions` 透传到 `requiredPartitions`。「pruned 空」短路放 **fe-core**（通用、对所有 SPI 连接器有益），故 SPI 通道只需表达 null/empty=全部、非空=子集。

判别键 = `SelectedPartitions.isPruned`（语义等价 legacy 的 `!= NOT_PRUNED`：`NOT_PRUNED.isPruned==false`，真裁剪结果 `isPruned==true` 含可能为空的 map，见 `LogicalFileScan.java:296,309`）。

### 1) SPI — `ConnectorScanPlanProvider`（fe-connector-api）
新增 6 参 `default` overload，**镜像既有 5 参 limit overload 模式**（`:82-89`），默认忽略分区委托回 5 参：
```java
default List<ConnectorScanRange> planScan(
        ConnectorSession session, ConnectorTableHandle handle,
        List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter,
        long limit, List<String> requiredPartitions) {
    return planScan(session, handle, columns, filter, limit);
}
```
**契约**（javadoc 明确）：`requiredPartitions` = 已裁剪分区名列表（如 `"pt=1,region=cn"`，即 `SelectedPartitions.selectedPartitions` 的 keySet，连接器侧 `new PartitionSpec(name)` 可解析）。`null`/空 = 不裁剪/读全部分区；非空 = 仅读这些分区。**「裁剪为零分区」由 fe-core 在调 planScan 前短路，永不到达 SPI**。

### 2) MaxCompute — `MaxComputeScanPlanProvider`
- 把现 5 参 `planScan` body 上移为 **6 参 override**（真实现），threading `requiredPartitions`；5 参 → 委托 6 参传 `null`（保持 passthrough / TVF 等其它调用方零变更）；4 参不变（委托 5 参）。
- 新增 package-private static helper `toPartitionSpecs(List<String>)` → `List<com.aliyun.odps.PartitionSpec>`（null/空→`emptyList`，逐项 `new PartitionSpec(name)`，与 legacy `MaxComputeScanNode:729` 同款转换）。
- 标准路径 `createReadSession(..., toPartitionSpecs(requiredPartitions), splitOptions)`（替 `:201` 的 emptyList）。
- limit-opt 路径：`planScanWithLimitOptimization` 加 `List<String> requiredPartitions` 形参，内部 `createReadSession(..., toPartitionSpecs(requiredPartitions), rowOffsetOptions)`（替 `:320` 的 emptyList）。**对齐 legacy**：legacy limit-opt（`getSplitsWithLimitOptimization(requiredPartitionSpecs)` @`:737`）同样接收裁剪集。

### 3) fe-core — `PluginDrivenScanNode`
- 新增字段 `private SelectedPartitions selectedPartitions = SelectedPartitions.NOT_PRUNED;`（默认 NOT_PRUNED → 未注入时行为不变）+ setter。import `org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions`（fe-core 内部跨包，import-gate 不涉及）。
- 新增 package-private static 纯函数（可单测）：
```java
static List<String> resolveRequiredPartitions(SelectedPartitions sp) {
    if (sp == null || !sp.isPruned) {
        return null;                                  // 未裁剪 → 读全部
    }
    return new ArrayList<>(sp.selectedPartitions.keySet()); // 空=裁剪为零;非空=子集
}
```
- `getSplits()` 内（call planScan 前）：
```java
List<String> requiredPartitions = resolveRequiredPartitions(selectedPartitions);
if (requiredPartitions != null && requiredPartitions.isEmpty()) {
    return Collections.emptyList(); // 裁剪为零分区,无需读 (镜像 legacy MaxComputeScanNode:724-727)
}
... scanProvider.planScan(connectorSession, currentHandle, columns, remainingFilter, limit, requiredPartitions);
```

### 4) fe-core — `PhysicalPlanTranslator`（plugin 分支 `:753-758`）
```java
PluginDrivenScanNode pluginScanNode = PluginDrivenScanNode.create(...);
pluginScanNode.setSelectedPartitions(fileScan.getSelectedPartitions());
scanNode = pluginScanNode;
```
无条件设（非分区表 Nereids 给 NOT_PRUNED → 无效果，与 Hive/legacy-MC 一致）。

## Implementation Plan
1. [fe-connector-api] `ConnectorScanPlanProvider`：+6 参 default overload + javadoc 契约。
2. [fe-connector-maxcompute] `MaxComputeScanPlanProvider`：5 参 body→6 参 override；5 参委托；`toPartitionSpecs`；两处 `createReadSession` threading；`planScanWithLimitOptimization` 加形参。
3. [fe-core] `PluginDrivenScanNode`：字段+setter+`resolveRequiredPartitions`+`getSplits` 短路与 6 参调用。
4. [fe-core] `PhysicalPlanTranslator`：plugin 分支注入。
5. 测试见下。

## Risk Analysis
- **blast radius 最小**：SPI 加 default 方法，es/jdbc/hive/paimon/hudi/trino **零改**（继承 default 委托回原 5 参）。唯一 override = MaxCompute。既有 4/5 参调用方（含 `EsScanPlanProviderTest`、passthrough TVF）不变。
- **parity 风险**：`toPartitionSpecs` 与 legacy `new PartitionSpec(key)` 逐字同款；三态判别用 `isPruned` 语义等价 legacy `!= NOT_PRUNED`。短路位置从 connector 上移到 fe-core，对 MaxCompute 行为等价（legacy 短路也在 fe-core scan node）。
- **null/empty 语义**：SPI 契约明确 null/空=全部、非空=子集、零分区 fe-core 短路不下达。`toPartitionSpecs` 对 null/空容错→emptyList→读全部（= 旧行为，回退安全）。
- **scope 边界**：仅 `visitPhysicalFileScan` plugin 分支（MaxCompute 路径）。**Hudi-SPI plugin 分支（`visitPhysicalHudiScan:861`）本次不接**——Hudi 连接器 live 翻闸前 deferred（DV-006），且其 provider 走 default 忽略 requiredPartitions；登记为已知 scope 边界（非本 fix 引入的回归）。
- **batch-mode（NG-7/P3）解耦**：本 fix 只恢复 requiredPartitions 下推，不引入 SPI batch 路径（async by-spec split）。NG-7 仍为独立 P3，但本 fix 是其前置（裁剪集到位后 batch-by-spec 才有意义）。

## Test Plan

### Unit Tests
- **fe-core** `PluginDrivenScanNodePartitionPruningTest`（`org.apache.doris.datasource`，直调 package-private `resolveRequiredPartitions`，直构 `SelectedPartitions`）：
  - `NOT_PRUNED` → `null`（**WHY**：未裁剪须读全部，不可误传空集致短路丢数据）；
  - `isPruned` + map{`pt=1`,`pt=2`} → `["pt=1","pt=2"]`（**WHY**：裁剪子集须下推，否则全表扫回归）；
  - `isPruned` + 空 map → 空 list（**WHY**：裁剪为零须可被短路识别，区别于「读全部」的 null）。
  - mutation：去 `isPruned` 判别（恒返回 names）→ NOT_PRUNED case 红；恒返回 null → 子集 case 红。
- **fe-connector-maxcompute** `MaxComputeScanPlanProviderTest`（同包直调 package-private `toPartitionSpecs`；连接器模块无 fe-core/Mockito，纯转换免网络）：
  - `null`→空、`[]`→空、`["pt=1"]`→`[PartitionSpec("pt=1")]`（**WHY**：分区名→ODPS spec 转换是下推到 read session 的唯一桥；null/空容错保旧「读全部」行为）。
  - mutation：转换体改为恒 emptyList → `["pt=1"]` case 红。

### E2E Tests
本轮流程 = **编译+UT（无 e2e）**。live e2e（真实 ODPS）为翻闸真值门，**本 fix 必经**但非本轮执行：
- p2 `test_mc_read_*` 分区裁剪：`WHERE pt='x'` EXPLAIN/profile 仅扫目标分区（split 数/规划耗时 ≪ 全表）；
- `WHERE pt='不存在'` 返回 0 行且**不**建全分区 session（短路）。
- 登记为 **DV-015** 真值门（同 P0-3 DV-014：bind 投影无 fe-core analyze harness，靠 live 覆盖）。

## Batch-D 红线
删 legacy `MaxComputeScanNode` 须待本 fix 落（它是分区裁剪下推唯一逻辑副本之一；连同写侧 `PhysicalMaxComputeTableSink`/`bindMaxComputeTableSink`/`allowInsertOverwrite` MC 分支）。复查 Batch-D「zero survivor」声明含本节点的读裁剪。

## doc-sync（随 commit 或横切）
- **更正证伪声明**：`P4-T06d-FIX-PART-GATES-design.md:99-104`（「fe-core only / 不涉及 fe-connector」——实则缺 connector 透传半边）、`P4-T06d-FIX-PART-GATES-review-rounds.md:11-12,42-44`（「pruning 不变式 clean / production CLEAN」——证伪）、`decisions-log.md` D-028（「分区裁剪恢复」叙事只成立元数据半边）。
- **登记**：deviations-log **DV-015**（本轮前裁剪未端到端、本 fix 恢复；live e2e 真值门）；decisions-log 新条（additive 6 参 SPI overload + 三态语义 + 短路位置）。
- 更新 `task-list-P4-rereview.md`（P1-4 行 + 累计结论）、`HANDOFF.md`。
