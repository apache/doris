# P3-T05 设计 — `applyFilter` 真实 EQ/IN 分区裁剪

> 批 B / metadata 补全 · behind 关闭的 gate（`SPI_READY_TYPES` 不含 hms/hudi）· 零 live-path 风险（SPI hudi 分支 dormant，零 live caller）
> 关联：[tasks/P3](../P3-hudi-migration.md) · [HANDOFF 未完成 #1](../../HANDOFF.md) · [DV-005](../../deviations-log.md) · [DV-007](../../deviations-log.md)（批 B scope 校正）
> 对标参照：`HiveConnectorMetadata.applyFilter`（`fe-connector-hive`，:193-234 + 7 个 helper）
> 状态：设计完成（code-grounded，5-reader recon workflow + 主线核读 Hive/Hudi 全文）

---

## Problem

SPI Hudi 路径**不做分区裁剪**，且附带一个**静默的分区来源切换** bug。

`HudiConnectorMetadata.applyFilter`（`HudiConnectorMetadata.java:144-167`）当前对**任何**带 partition key 的表：

1. **完全忽略谓词**：直接 `hmsClient.listPartitionNames(db, table, -1)` 拉**全部**分区名，不解析 `constraint.getExpression()`，不抽取 EQ/IN，不裁剪。
2. **无条件**把全量列表塞进 `prunedPartitionPaths`（`:162-164`）。

两个后果：

- **(perf/正确性) 无分区裁剪**：`year='2024' AND month='01'` 仍扫全部分区 → 性能塌方 + 无谓 HMS/文件系统压力。
- **(隐蔽) 分区来源静默切换**：`prunedPartitionPaths` 一旦非 null，`HudiScanPlanProvider.resolvePartitions`（`:287-289`）就**短路**、直接用它，**绕过** `:307` 的 `HoodieTableMetadata.getAllPartitionPaths()`（Hudi 元数据表，Hudi 的权威分区来源）。即：**只要查询带任意 WHERE（触发 applyFilter），分区来源就从 Hudi-metadata 偷偷换成 HMS**；无 WHERE 时又回到 Hudi-metadata。两个来源对已同步表通常一致，但这是**未声明的行为分叉**，且把"裁剪入口"和"来源选择"耦合在了一起。

**对标**：`HiveConnectorMetadata.applyFilter`（`:193-234`）做真实 EQ/IN 裁剪，且**仅在裁剪真正生效时**才回传修改后的 handle，其余情况 `Optional.empty()`（handle 不变，下游用默认 listing）。Hudi 缺这套逻辑。

---

## Root Cause

Hudi 的 `applyFilter` 是个**占位实现**：从未移植 Hive 的「抽取分区谓词 → 列候选 → 匹配 → 缩减集」链路，也没有 Hive 的「无效裁剪即 `Optional.empty()`」短路守卫，于是退化成"无条件设全量"。

> `ConnectorFilterConstraint.getColumnDomains()` 在 fe-core 侧由 `PluginDrivenScanNode.buildFilterConstraint` 传入**空 map**（`Collections.emptyMap()`），唯一可用的谓词表示是 `getExpression()`（完整表达式树）——与 Hive 一致。故裁剪必须解析 `getExpression()`。

---

## Design

把 `HudiConnectorMetadata.applyFilter` **重写为忠实镜像 `HiveConnectorMetadata.applyFilter`**，但**保留 Hudi 的 `List<String> prunedPartitionPaths` 表示**（不改用 Hive 的 `List<HmsPartitionInfo>`）——因为 `HudiScanPlanProvider.resolvePartitions` 消费的是**相对分区路径串**（喂给 Hudi `HoodieTableFileSystemView`，:208/:237），不是 HMS 分区元数据。HMS 分区**名**（`year=2024/month=01`，Hive 约定）即 Hudi 相对分区**路径**，二者同形，无需转换（现有 `parsePartitionValues` :317-332 已按 `/`+`=` 解析，证明同形假设）。

全部改动在 `fe-connector-hudi` 模块内（**不动 fe-core、不动 BE、不动 thrift、不动 Hive**），gate 保持关闭。

### 改动 1 — `HudiConnectorMetadata.applyFilter` 重写（mirror Hive 7 步）

```java
HudiTableHandle hudiHandle = (HudiTableHandle) handle;
List<String> partKeyNames = hudiHandle.getPartitionKeyNames();
if (partKeyNames == null || partKeyNames.isEmpty()) {
    return Optional.empty();                                    // ① 无分区列 → 不裁剪
}
Map<String, List<String>> partitionPredicates = extractPartitionPredicates(
        constraint.getExpression(), partKeyNames);
if (partitionPredicates.isEmpty()) {
    return Optional.empty();                                    // ② 无分区谓词 → handle 不变，
}                                                              //    resolvePartitions 回落 Hudi-metadata listing（修复来源切换 bug）
List<String> allPartNames = hmsClient.listPartitionNames(
        hudiHandle.getDbName(), hudiHandle.getTableName(), -1); // ③ 列候选（保留现有 -1=unlimited，见下）
if (allPartNames == null || allPartNames.isEmpty()) {
    return Optional.empty();                                    // 无分区可裁
}
List<String> matchedPartNames = prunePartitionNames(
        allPartNames, partKeyNames, partitionPredicates);       // ④ 按谓词匹配
if (matchedPartNames.size() == allPartNames.size()) {
    return Optional.empty();                                    // ⑤ 裁剪无效果 → 不回传
}
HudiTableHandle updatedHandle = hudiHandle.toBuilder()
        .prunedPartitionPaths(matchedPartNames)                 // ⑥ 仅缩减集（可为空=裁光）
        .build();
return Optional.of(new FilterApplicationResult<>(
        updatedHandle, constraint.getExpression(), false));     // ⑦ remaining=全表达式（BE 复评，与 Hive 同）
```

**与 Hive 的两处有意差异（surgical，登记）**：

- **③ HMS listPartitionNames 上限**：Hive 用 `100000`（硬上限，超出静默丢分区 → 潜在漏裁/错结果）；Hudi **保留现状 `-1`（unlimited）**——**严格更安全**（不静默截断），且是 Hudi 占位实现的既有取值，不引入新行为。**不跟 Hive 的 100000**。
- **分区表示**：Hive `List<HmsPartitionInfo>`（含 location/format）；Hudi `List<String>`（路径串）——Hudi 下游不需要 HMS 元数据（Hudi 自己从 FileSystemView 解析文件），保持现状字段，最小 diff。

### 改动 2 — 移植 7 个 private helper（duplicate from Hive）

逐行复刻 `HiveConnectorMetadata` 的：`extractPartitionPredicates`、`extractPredicatesRecursive`、`extractColumnName`、`extractLiteralValue`、`prunePartitionNames`、`parsePartitionName`、`matchesPredicates`。语义完全一致：

- 递归下降识别 `ConnectorAnd`（遍历 conjuncts）/ `ConnectorComparison`（仅 `Operator.EQ`）/ `ConnectorIn`（非 negated）；列名经 `ConnectorColumnRef`、字面值经 `ConnectorLiteral`（`String.valueOf`）。
- `parsePartitionName` 按 `/` 再 `=` 解析；`matchesPredicates` 要求每个分区谓词列在分区值中命中 allowed 集合。
- 只对 **partition key 集合内**的列累积谓词（非分区列谓词被忽略 → 由 BE 复评，正确）。

**为何 duplicate 而非共享**：`fe-connector-hudi` 仅依赖 `fe-connector-hms`，**不依赖 `fe-connector-hive`**（pom 确认）；连接器模块互相 import 对方 metadata 类是错误分层；import-gate 禁连接器 import fe-core。抽到 `fe-connector-hms` 共享需**改 Hive**（移其 private 副本）= 触碰其它连接器工作码（本场只动 hudi，HANDOFF 关键认知 5）。故复刻，**登记 Hive/Hudi 重复**，待 P7 hive migration 时一并 consolidate（届时两模块同改）。与 T02 复刻 `toHiveTypeString`（而非跨模块共享）一脉相承。

### 新增 import

`ConnectorAnd`/`ConnectorComparison`/`ConnectorExpression`/`ConnectorIn`/`ConnectorLiteral`（`connector.api.pushdown`）、`java.util.HashMap`、`java.util.Set`。`FilterApplicationResult`/`ConnectorFilterConstraint` 已 import。

---

## Implementation Plan

1. `HudiConnectorMetadata.java`：重写 `applyFilter`（:144-167）为 7 步；新增 7 个 helper；补 import。
2. 新增 `HudiPartitionPruningTest`（见 Test Plan）：手写 `HmsClient` 测试替身（接口 8 方法，仅实现 `listPartitionNames`，余抛 `UnsupportedOperationException`）。
3. 守门：`mvn -pl fe-connector/fe-connector-hudi -am test -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`（cwd=`fe/`）；checkstyle 0（**禁 static import**，用 `Assertions.assertX`）；import-gate 通过。

---

## Risk Analysis

- **零 live 风险**：gate 关闭，hudi 查询走 legacy `HudiScanNode`；SPI `HudiConnectorMetadata.applyFilter` **零 live caller**（仅 SPI 表触达，hudi 未翻闸）。改动纯硬化 dormant 代码。
- **行为改进**：(a) 真实 EQ/IN 裁剪（性能）；(b) 修复"分区来源静默切换"——无分区谓词时回 `Optional.empty()`，`resolvePartitions` 回落 Hudi-metadata listing，与无 WHERE 路径一致（消除分叉）。
- **正确性边界**：
  - 只裁剪 **EQ/IN over partition columns**；范围谓词（`>`/`<`/`BETWEEN`）、非分区列谓词**不裁剪**（保守），`remaining=全表达式` 交 BE 复评 → **不会漏行**（与 Hive 同语义）。
  - `matched` 为空（谓词命中 0 分区）→ `prunedPartitionPaths=[]` → 扫 0 分区（正确）。
  - 分区名/路径同形假设：现有 `parsePartitionValues` 已依赖，T05 不新增假设。
- **不碰** `SPI_READY_TYPES` / legacy / 其它连接器（含 Hive）/ thrift（HANDOFF 关键认知 3+5）。
- **simplicity（R2/R3）**：单文件 + 镜像 Hive 既证实现，最小 diff；保留 `List<String>` 字段与 `-1` 上限（不引入新行为）。

---

## Test Plan

### Unit Tests（本 task 交付；模块第三个测试类）

`HudiPartitionPruningTest`（**测意图 / Rule 9**：编码「分区裁剪必须正确缩减集合、且绝不在非分区列或范围谓词上误裁」这一 WHY）。手写 `FakeHmsClient implements HmsClient`，`listPartitionNames` 返固定列表如 `["year=2023/month=12","year=2024/month=01","year=2024/month=02"]`，余方法抛 `UnsupportedOperationException`。构造真实 `ConnectorExpression` 树（`ConnectorComparison(EQ,…)` / `ConnectorIn` / `ConnectorAnd` / `ConnectorColumnRef` / `ConnectorLiteral`）：

- **EQ on partition col**：`year='2024'` → handle 含 2 分区（`year=2024/*`），断言**恰为**那 2 个、顺序保留。
- **IN on partition col**：`month IN ('01','12')` → 跨 year 命中 `…/month=01` + `…/month=12`。
- **AND（分区谓词 + 非分区谓词）**：`year='2024' AND price>100` → 仅按 `year` 裁（2 分区），`price>100` 被忽略（非分区列）→ 断言不误裁、不抛。
- **非分区谓词 only**：`price>100` → `Optional.empty()`（**不裁剪、handle 不变**——直击"来源切换"修复）。
- **谓词命中全部分区**：`year IN ('2023','2024')`（覆盖全集）→ `Optional.empty()`（裁剪无效果，不回传）。
- **谓词命中 0 分区**：`year='1999'` → handle 含**空** `prunedPartitionPaths`（扫 0 分区，非 empty-optional）。
- **无分区列表**：`partKeyNames` 空 → `Optional.empty()`（unpartitioned 表，applyFilter 不介入）。

每用例断言 `result.isPresent()` 与（present 时）`((HudiTableHandle)result.getHandle()).getPrunedPartitionPaths()` 的**精确集合**——断言旧占位码（设全量）会失败的行为。

### E2E Tests

**不适用 / 推迟批 E**（与 T02/T04、`tasks/P3 §策略` 一致；precedent DV-003）：gate 关闭，hudi 查询不走 SPI，无法在 regression-test 端到端触达。批 E 翻闸后加 regression：带分区谓词查询 → 断言**仅扫匹配分区**（explain/profile 校 partition 数）。**显式登记，不静默跳过（R12）**。
