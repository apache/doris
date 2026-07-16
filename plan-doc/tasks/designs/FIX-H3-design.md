# FIX-H3 — hudi 把 HMS 名当存储路径喂 fsView → 非 hive-style 表带 filter 0 split（hudi-only）

> 来源：`task-list-65185-reverify-fixes.md` §H3；证据 reverify §2 H3。批次 0 第 4 条（最复杂，hudi-only）。
> 已由 recon agent 核实 hudi 1.0.2 API + 现码；本设计取 recon 推荐的「最小正确 + 可测」形。

## Problem

翻闸后，对 `hive_style_partitioning=false`（Hudi 默认）的 hudi-on-HMS 分区表，**带 filter 的查询返回 0 split（不带 filter 有行）**——即静默丢全部匹配行。

## Root Cause（对 HEAD 核实）

`HudiConnectorMetadata.applyFilter:247` **无条件**把候选分区源设为 `hmsClient.listPartitionNames`（hive-style HMS 名 `year=2024/month=01`），剪枝后原样存入 `prunedPartitionPaths`。scan 侧 `resolvePartitions:588` 直接返回它们，喂给 `fsView.getLatestBaseFilesBeforeOrOn(partitionPath, ...)`（`HudiScanPlanProvider:394/429`）——而 fsView 按 **Hudi 相对存储路径** 索引。非 hive-style 表物理布局是 `2024/01`，与 HMS 名 `year=2024/month=01` 不符 → fsView 找不到文件 → 0 split。不带 filter 时 `resolvePartitions` 回退 `listAllPartitionPaths`（Hudi 元数据，相对路径，正确）→ 有行。

`applyFilter` **绕过了** `use_hive_sync_partition` 感知：连接器自己的列举路 `collectPartitions:641` 是感知的（非 hive-sync 用 `listAllPartitionPaths` 相对路径），但 `applyFilter` 没有。`HudiScanPlanProvider:716-721` 的 javadoc 自称此坑「翻闸前已闭合」是**过时的**（翻闸已发生、修未做）。legacy 默认从 Hudi 元数据取相对路径 → 回归。

## Design（option A — 保留 H1 的 `parsePartitionName`，surgical 加分支）

`applyFilter` 候选分区源改 **`use_hive_sync_partition` 感知**，镜像 `collectPartitions`；候选路径必须与 scan 喂 fsView 的**同形**（Hudi 相对存储路径）：

- **hive-sync**（`useHiveSyncPartition()==true`）：`hmsClient.listPartitionNames`（hive-style 名 == 相对存储布局，fsView 直接认，无需相对化——recon 证实 legacy/collectPartitions 均如此，`FSUtils.getRelativePartitionPath`/`getLocation` 是**红鲱鱼**，超出 parity，不引入）。剪枝复用现有 `prunePartitionNames`（→ `parsePartitionName`，**H1 已修 unescape**）。
- **非 hive-sync**（默认）：`metaClientExecutor.execute(() -> HudiScanPlanProvider.listAllPartitionPaths(buildMetaClient(buildHadoopConf(), basePath)))`——**与不带 filter 的 scan（`resolvePartitions`）同一相对路径源**，成本 net-neutral（`resolvePartitions` 见 `prunedPaths!=null` 即短路，不重复列举）。剪枝用**新** static `prunePartitionPaths`（→ `parsePartitionValues`，处理 hive-style **与** 位置式 `2024/01` 两种布局 + unescape）。

两臂产出的 matched 均为相对存储路径形；`prunedPartitionPaths(matched)`。保留「无剪枝效果 → `Optional.empty()`」与空集守卫。

> **为何 option A 而非统一 `parsePartitionValues`（B）**：B 会使 hudi `parsePartitionName`/`prunePartitionNames` 死掉、需删除、连带撤掉 H1 刚加的 hudi 方法+直接测（同批次内加了又删，churn）。A 让 hive-sync 臂继续用 `parsePartitionName`（HMS hive-style 名的天然解析器，H1 修的 unescape 在此长期有效），非 hive-sync 臂用 `parsePartitionValues`（位置式）——各源用各自天然解析器，surgical、保留已提交工作（Rule 3）。`matchesPredicates` 提为 static 供两个 prune helper 共用。

## Implementation Plan

`HudiConnectorMetadata.java`：
1. `applyFilter`：候选源按 `useHiveSyncPartition()` 二分（如上）；`matchedPartPaths` 二分求得后走共同尾（size 比较 bail + LOG + build handle）。LOG 加 `hiveSync` 便于诊断。
2. 新增 static `prunePartitionPaths(List<String> allPartPaths, List<String> partKeyNames, Map<String,List<String>> predicates)`：逐路径 `matchesPredicates(HudiScanPlanProvider.parsePartitionValues(path, partKeyNames), predicates)`。
3. `matchesPredicates`：`private` → `static`（纯函数；`prunePartitionNames` 与新 `prunePartitionPaths` 共用）。`parsePartitionName`/`prunePartitionNames` 保留（hive-sync 臂用）。

无 fe-core 改动；无 `FSUtils`/`getPartitions`/`getLocation`（recon 证实为 parity 红鲱鱼）。

## Risk Analysis

| Risk | 处置 |
|---|---|
| 非 hive-sync 臂新建 metaClient 增成本 | net-neutral：`resolvePartitions` 见 pruned 即短路，过滤查询只在 `applyFilter` 列举一次（= 不过滤查询在 `resolvePartitions` 的同一次列举）。hive-sync 臂仍用廉价 HMS 列举。 |
| hive-sync 是否需相对化 LOCATION | 否（recon + legacy 证实：标准 hive-sync 布局 name==相对路径；相对化自定义 location 超 parity，不做）。 |
| 与 H1 交互 | hive-sync 臂用 `parsePartitionName`（H1 unescape 有效）；非 hive-sync 臂用 `parsePartitionValues`（本就 unescape）→ 两臂均 unescape 正确。 |
| 位置式路径解析 | `parsePartitionValues` 已证处理 `2024/01`（`HudiPartitionValuesTest.nonHiveStylePositionalPathMapsByPosition`）+ hive-style + 单列 whole-path fallback + fail-loud。 |
| fe-core 铁律 | 仅连接器内；`use_hive_sync_partition` 是**连接器**属性读取（非 fe-core 源名判别），与 `collectPartitions` 同款。 |

## Test Plan

现有 `HudiPartitionPruningTest` 用 `DirectHudiMetaClientExecutor`（**运行** action）+ emptyMap（非 hive-sync）→ 修后非 hive-sync 臂会真建 metaClient（假 basePath）→ 抛。迁移 + 补：

### Unit Tests
1. **迁移现有 8 条**：`applyFilter` helper 的 executor 换成 `StubMetaClientExecutor(PARTITIONS)`（返回 canned、不跑 action → 不建真 metaClient）。现有断言不变（`parsePartitionValues` 处理 hive-style 名，matched 相对路径形 == 原 hive-style 名）→ 全绿；现覆盖**非 hive-sync 臂**（默认、原被破坏的臂）。
2. **H3 核心 RED 测**：`testNonHiveStylePositionalPathsPruneToRelativePaths` — `FakeHmsClient(hive-style 名)` + `StubMetaClientExecutor(["2024/01","2024/02","2023/12"])` + emptyMap + `eq("year","2024")` → 断言 pruned == `["2024/01","2024/02"]`（**相对路径**，非 HMS 名）。RED（改前）：旧码用 HMS 名 → pruned == `["year=2024/month=01",...]` ≠ 断言。
3. **hive-sync 臂测**：`testHiveSyncBranchPrunesHmsNames` — `use_hive_sync_partition=true` + `FakeHmsClient(PARTITIONS)` → 走 HMS 名臂（`parsePartitionName`）→ pruned == matched hive-style 名。
4. **直接 helper 测**：`prunePartitionPaths(["2024/01","2024/02","2023/12"], ["year","month"], {year:[2024]})` == `["2024/01","2024/02"]`（位置式匹配，离线）。

### E2E Tests
非 hive-style hudi 表「带 filter 分区集 == 不带 filter 分区集（都命中）」= **live-gated**（真 hudi 集群），显式登记（Rule 12）。memory `hms-iceberg-delegation-needs-e2e` 口径。

## 守门结果（DONE，commit `9c6fc584eb9`）

`mvn -o -pl :fe-connector-hudi -am test -Dtest=HudiPartitionPruningTest,HudiPartitionValuesTest,HudiConnectorPartitionListingTest` → **BUILD SUCCESS**；39 run / 0 fail（`HudiPartitionPruningTest` 11→迁移 8 + 新 3；`HudiConnectorPartitionListingTest` 18 回归通过，证 `matchesPredicates` static + applyFilter 重构未破坏列举路）；checkstyle 全 0；import-gate `GATE_RC=0`。后续 test-hardening `f0ee2ab06d2` 加 DATE 非回归测（12 run）。

## 最终对抗复审（批次 0 全量，3 并行 skeptic 审已提交码）

**结论：H3 CORRECT & COMPLETE（其声明范围内）；批次 0 四修正确、复合正确、无回归、范围完整（其它连接器独立核实免疫）；10 条新测均可 RED + 编码 WHY。** 详见 session。登记两项（均非本 fix 引入、非阻断）：

- **残余 gap（登记，非本批修）**：`use_hive_sync_partition=true` **且** `hive_style_partitioning=false` 的表——hive-sync 臂喂 HMS hive-style 名给 fsView，但物理布局是位置式 `2024/01` → 带 filter 0 split（同类 bug 的另一臂）。**非 H3 引入**（改前所有表都用 HMS 名，此臂与改前及 legacy `HMSExternalTable` 逐字节相同），且与本 fix「不相对化自定义 location（超 collectPartitions/legacy parity）」立场一致；不带 filter 该组合正常（`resolvePartitions` 恒用相对路径）。→ **登记为 CACHE-later / D-PRUNE 时一并评估**；当前对最常见组合（hive-sync+hive-style、非-hive-sync+任意布局）全部正确。
- **net-neutral 微注**：仅「EQ/IN 恰好命中全部分区」的 no-effect 角落会二次列举 Hudi 元数据（applyFilter 列举→bail→resolvePartitions 再列举）；与改前 HMS-名码同模式、非新回归、非正确性问题。

## 决策类型
明确修复（hudi-only；用户批次 0 范围内）。连接器局部、无 SPI 变更、与 `collectPartitions`/legacy parity。D-PRUNE 抽取延后。
