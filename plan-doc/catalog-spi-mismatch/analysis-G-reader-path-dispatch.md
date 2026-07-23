# 类别 G — Reader-path / catalog-dispatch

## 范围说明与基线声明

本类别覆盖两条与「读路径 reader-type 决策」和「catalog 分派（同格式不同 catalog 走不同路径）」相关的发现：#27（reader-type 决策的多份手搓副本 + 声称的 hudi `force_jni_scanner` 回归）与 #28（同格式不同 catalog 走不同读路径 / detect-and-delegate 未实现）。

**基线声明**：本核实基于当前工作树（git worktree `/mnt/disk1/yy/git/wt-catalog-spi`，分支 `catalog-spi-2-lvl-cache`），逐条独立读当前代码（Read/Grep 当前文件，而非 `git show` 旧分支）并做对抗复核，不轻信原 survey 的结论。下文所有引用行号均以当前工作树为准；两阶段（initial investigate + adversarial verify）结论一致时以一致结论为准，分歧时按 verify 的 `final_verdict` 与 `corrections` 修正 stage-1 草稿。

---

## 总表

| # | 严重 | 原报告结论 | 核实结论 | 一句话 |
|---|------|-----------|---------|--------|
| 27 | 🟠 | 含一个真回归：reader-type 决策 3 份手搓副本，hudi `force_jni_scanner` 在 SPI 侧零处 honor → COW 表静默继续 native | **PARTIAL**（high）| 架构观察（多连接器各自决策、3 种线上编码、thrift 不对称）成立；但作为严重度唯一支撑的「hudi force_jni 回归」在当前码不存在（REFUTED）——SPI 已完整 honor 且有单测锁定 |
| 28 | 🟠 | 同格式不同 catalog 走不同路径；detect-and-delegate 未实现 → type=hms 进 SPI 后 HMS 里的 iceberg/hudi 表被当裸 hive 文件读 | **STALE_FIXED**（high）| 报告对成文时（pre-#65473）代码准确且自标「分阶段债」；缺口已被 commit `3593684715f`（#65473，2026-07-20）原子填平，预测的 bug 从未真正出现 |

---

## #27 reader-type 决策 = 3 份手搓副本（含声称的 hudi force_jni_scanner 回归）

### 核实结论

**PARTIAL**（置信度 high）。stage-1 与 verify 完全一致，无分歧。最终判定：本发现的**架构性观察成立**，但作为其严重度（🟠）与「含一个真回归」标注**唯一支撑**的那条回归——「SPI HudiScanPlanProvider 零处 honor `force_jni_scanner`」——在当前工作树中**不成立（REFUTED）**。因此整体应从「含真回归的 🟠」降级为「纯架构观察、无功能缺陷」。

### 原报告主张

reader-type 决策每连接器各写一套、无共享枚举/决策/开关点；一个概念 3 种线上编码（typed `TPaimonReaderType` 枚举 / hudi 魔法串 `jni` / iceberg-paimon serialized-split 约定）；`TIcebergFileDesc`、`THudiFileDesc` 无 reader_type 字段。**真回归**：hudi 的 `force_jni_scanner` 在 legacy `HudiScanNode` 3 处 honor，SPI `HudiScanPlanProvider` 零处 → `SET force_jni_scanner=true` 在 hudi COW 上 legacy 走 JNI、SPI 静默继续 native（根因：决策拆到 `planScan` 读 session、但 `populateRangeParams` 降级时无 session）。

### 核实过程

**架构观察部分——属实。** 逐连接器读当前代码，reader-type 决策确实各写一套、无共享抽象：

- paimon：`shouldUseNativeReader(forceJni, forceJniScanner, optRawFiles)`（`PaimonScanPlanProvider.java:1152`）= `!forceJni && !forceJniScanner && supportNativeReader`；线上编码为 typed 枚举 `TPaimonReaderType {PAIMON_NATIVE, PAIMON_JNI, PAIMON_CPP}`（`PlanNodes.thrift:362-366`），经 `TPaimonFileDesc.reader_type`（field 17，`PlanNodes.thrift:386`）下发。
- iceberg：native-vs-JNI 分类在 `buildRange`/`planScan` 内（`IcebergScanPlanProvider.java:120`），普通数据文件（含 position/equality delete）走 NATIVE 并挂 `TIcebergDeleteFileDesc`（`:976-1005`），仅 system table 走 JNI serialized-split（`:787-800`）。`TIcebergFileDesc`（`PlanNodes.thrift:333-354`）**无** reader_type 字段，靠 `serialized_split`（field 12）+ BE 端 FORMAT_JNI 约定区分。
- hudi：COW→native（`collectCowSplits`）、MOR 有 delta log→JNI（`collectMorSplits`）；`THudiFileDesc`（`PlanNodes.thrift:413-426`）**无** reader_type 字段，`getScanNodeProperties` 用魔法串 `file_format_type="jni"`（`HudiScanPlanProvider.java:316`）+ per-split `TFileFormatType.FORMAT_JNI`（`HudiScanRange.java:199`）。

故「一个概念 3 种线上编码 / 两个 thrift 无 reader_type 字段 / delete 处理三家不同」均属实；`TPaimonFileDesc` 有 reader_type、`TIcebergFileDesc`/`THudiFileDesc` 无——thrift 不对称属实。

**核心「真回归」部分——REFUTED。** 报告称 SPI `HudiScanPlanProvider` 零处 honor force_jni。`grep force_jni fe-connector-hudi/` 直接命中 `HudiScanPlanProvider`/`HudiScanRange`/`HudiForceJniTest` 全套，反证「零处」。逐行亲读：

- 常量与读取：`FORCE_JNI_SCANNER`（`HudiScanPlanProvider.java:100`）、`isForceJniScannerEnabled(session)`（`:119-123`，读 session 属性 key `force_jni_scanner`）。
- 决策：`boolean forceJni = isForceJniScannerEnabled(session); boolean useNativeCowPath = isCow && !forceJni;`（`:160-161`），注释（`:157-159`）显式复刻 legacy `canUseNativeReader() = !isForceJniScanner() && isCowTable`。
- 分发：`if (useNativeCowPath) collectCowSplits(...) else collectMorSplits(..., forceJni, ...)`（`:288-296`）——COW 在 force_jni 下确走 `collectMorSplits`（JNI 路径）。`buildMorRange` 内 `useNative = logs.isEmpty() && !filePath.isEmpty() && !forceJni`（`:508`）。
- 无 session 降级一致性：报告担心的「`populateRangeParams` 无 session 无法判断」已被解决——`forceJni` 被**烘焙进 split**（`HudiScanRange.java:67-72, 119`），`populateRangeParams` 处 `if (isJni && deltaLogs.isEmpty() && !forceJni)` 才降级 native（`HudiScanRange.java:181-185`），据烘焙 flag 抑制 no-log→native 降级，与 `planScan` 分支保持一致。
- native dict：`getScanNodeProperties` 在 force_jni 下跳过 native schema-evolution dict（`:361`）。
- 单测锁定：`HudiForceJniTest`（`:65-96`）——`forceJniSuppressesNoDeltaLogNativeDowngrade`（true→FORMAT_JNI）配对 `withoutForceJniNoDeltaLogDowngradesToNative`（false→FORMAT_PARQUET）。

**唯一真实偏差**（签字保留、非回归）：COW **incremental（`@incr`）**读取有意忽略 force_jni（`HudiScanPlanProvider.java:582-585` 注释明确标 "signed, deliberate deviation from legacy"）。verify 阶段补充了该偏差的关键含义：legacy 对 `force_jni + COW incremental` 会走 MOR 分支、对 COW relation 调 `collectFileSlices()` → 抛 `UnsupportedOperationException`（latent legacy crash）。故 SPI 这处「偏差」实为**规避一个 legacy 潜在崩溃**（crash 修复方向），比「本该 JNI 却静默 native」更无害，进一步支撑「本发现无功能回归」。

### 背景（架构观察为何成立但非缺陷）

三连接器 reader-type 决策各自手搓，是因为三种湖格式的删除/读语义本质不同：iceberg 数据文件 + position/equality delete 走 NATIVE 并挂 delete 描述符（`IcebergScanPlanProvider.java:976-1005`）、paimon deletion-vector 可 NATIVE（`TPaimonDeletionFileDesc` field 12）、hudi MOR delta log 走 JNI。「delete⇒三家读语义相反」作为**描述**属实，但这源于存储格式本身的删除模型差异，是事实差异而非缺陷。共享枚举的收益因此有限。

### 影响（具体示例——反证报告预测的失效不会发生）

设一张 hudi COW 表（或 MOR 无 log 的 read-optimized slice），执行 `SET force_jni_scanner=true` 后查询：

- 报告预测：SPI 侧因未 honor force_jni 而静默继续 native reader，与 legacy 的 JNI 路径分叉，返回错误结果。
- 当前实际：SPI 路径 `useNativeCowPath = isCow && !forceJni = false` → 走 `collectMorSplits`/`buildMorRange` 输出 FORMAT_JNI split（`HudiScanPlanProvider.java:288-296`），与 legacy 一致；即便进入无 session 的 `populateRangeParams`，也因烘焙进 split 的 forceJni flag 抑制降级（`HudiScanRange.java:181-185`）。**报告描述的错误结果不会发生。**

### 修复方案

**无需功能修复**（无 bug）。若追求跨连接器一致性，可在 `fe-connector-api` 引入统一 `ReaderType` 枚举 + 各连接器 provider 返回，收敛 3 种线上编码——但这碰「fe-core 只出不进 / 禁 scaffolding」铁律边界，且属跨连接器大重构，应作为独立设计任务交 review，**不应挂在本「回归」发现名下**。

---

## #28 同格式不同 catalog = 不同路径（detect-and-delegate 未实现）

### 核实结论

**STALE_FIXED**（置信度 high）。stage-1 与 verify 完全一致，无分歧。报告对成文时（pre-#65473）的代码描述准确，并诚实自标为「分阶段债」而非活跃 bug；但该债已在 commit `3593684715f`（#65473 "[refactor](catalog) Catalog spi 11 hive"，2026-07-20）**原子偿清**。对当前代码，主张的核心断言「detect→delegate 没实现」已不成立。

### 原报告主张

Case A（`type=iceberg` + `iceberg.catalog.type=hms` 与 REST）已统一走同一 `IcebergScanPlanProvider`；但 Case B（`type=hms` catalog 里躺着 `table_type=ICEBERG` 的寄生 iceberg 表）走 legacy `HMSExternalTable` DLA=ICEBERG → legacy `IcebergScanNode`，从不碰 SPI iceberg 连接器 → 同一 iceberg 格式两条路径。关键缺口：`HiveConnector.getScanPlanProvider()` 无条件返回 `HiveScanPlanProvider`，detect→delegate 未实现 → 一旦 `type=hms` 进 `SPI_READY_TYPES`，HMS 里的 iceberg/hudi 表会被当裸 hive 文件读。

### 核实过程（当前工作树 detect-and-delegate 已完整实现）

1. **hms 已 SPI 化**：`CatalogFactory.java:56-57` — `SPI_READY_TYPES = ImmutableSet.of("jdbc","es","trino-connector","max_compute","paimon","iceberg","hms")`，含 hms；`:110-118` 对 hms 走 SPI 连接器路径 → `PluginDrivenExternalCatalog(HiveConnector)`；`:138-142` built-in switch 注释明确 hms/iceberg 不落此 fallback（"hms and iceberg are routed through the SPI connector path ... never reach this built-in fallback"）。故报告的 Case B（type=hms → legacy `HMSExternalTable` → legacy `IcebergScanNode`）在当前分支已不再是 hms 的执行路径。
2. **检测已实现**：`HiveConnectorMetadata.getTableHandle`（`:392-415`）对每张表调 `HiveTableFormatDetector.detect(tableInfo)`（`:398`）；检测器（`HiveTableFormatDetector.java:87-110`）按 `table_type=ICEBERG` / hudi input-format 或 `flink.connector=hudi` / hive input-format 分类，否则 UNKNOWN，镜像 legacy `HMSExternalTable`。
3. **委派已实现**：`getTableHandle` 对 `tableType == ICEBERG` 返回 `icebergSiblingMetadata(session).getTableHandle(...)`（`:410-411`），`HUDI` 委派 `hudiSiblingMetadata`（`:413-414`），返回的是 sibling 自己的 foreign handle（非 `HiveTableHandle`）；`:421-426` UNKNOWN 非 view 则 fail-loud。
4. **handle 路由已实现**：`HiveConnector.getScanPlanProvider(ConnectorTableHandle)`（`:250-256`）= `if (handle instanceof HiveTableHandle) return getScanPlanProvider(); return resolveSiblingOwner(handle).getScanPlanProvider(handle);` — 非 hive handle 路由到 sibling。`getWritePlanProvider(handle)`（`:274-280`）、`getProcedureOps(handle)`（`:292-298`）同构。
5. **fe-core 走 handle-arg 重载**：`PluginDrivenScanNode.java:256` 用 `connector.getScanPlanProvider(currentHandle)`（非零参版本），路由被真实触发。
6. **sibling 构建**：`getOrCreateIcebergSibling`/`getOrCreateHudiSibling`（`HiveConnector.java:530+/565+`）经 `context.createSiblingConnector` 在插件自身 child-first classloader 构建。
7. **git 证据**：commit `3593684715f`（#65473，2026-07-20）同一提交里既把 `"hms"` 加进 `SPI_READY_TYPES`、删掉 legacy `case "hms"`，又新增 `getTableHandle` 的 ICEBERG/HUDI divert——即（a）hms SPI 化 与（b）detect-and-delegate 是**原子地**一起落地，因此报告预测的 bug 窗口从未真正打开。

报告唯一在当前仍准确的细节是：零参 `getScanPlanProvider()`（`HiveConnector.java:232-235`）确实无条件返回 `HiveScanPlanProvider`——但 fe-core 走的是做路由的 handle-arg 重载，该细节不构成 bug。

### 举例说明（报告预测的失效为何不会发生）

设 `type=hms` 目录下有一张 iceberg 表 `t`（HMS 参数 `table_type=ICEBERG`），执行 `SELECT * FROM hms_cat.db.t`：

- 报告预测（pre-fix 假设 divert 缺失）：`getTableHandle` 产出 `HiveTableHandle` → `getScanPlanProvider` 返回 `HiveScanPlanProvider` → BE 把 iceberg 数据文件当裸 hive 文件读 → 忽略 position/equality delete、隐藏分区、schema evolution → 返回错误结果（多出已删除行 / 错列）。
- 当前实际：`getTableHandle` 检测到 ICEBERG（`HiveConnectorMetadata.java:410`）→ 委派 `icebergSiblingMetadata.getTableHandle` → 返回 iceberg 的 foreign handle；`PluginDrivenScanNode` 用 `getScanPlanProvider(handle)` → `resolveSiblingOwner` 路由到 iceberg sibling → 走 `IcebergScanPlanProvider`，正确处理 delete / 隐藏分区 / schema evolution。两条路径不再分叉，同一 iceberg 格式由同一 iceberg 连接器读。hudi 同理。

### 需要注意的遗留（非本发现的 bug，但值得记一笔）

`HiveConnector.java` 内多处注释仍写 "Dormant/Inert until hms enters SPI_READY_TYPES — nothing builds it today"（如 `:247-248`、`:290`、`:302-303`）以及 `HiveConnectorMetadata.java:408-409` "today getTableHandle is never called for this connector"。这些注释在 hms 已进入 `SPI_READY_TYPES` 后已**过时 / 自相矛盾**（功能已 live 而非 dormant），属注释漂移，建议清理；但不影响功能正确性，也不重开本发现所指的功能缺失。

---

## 本类别小结

- **真问题**：无。本类别两条发现均无当前活跃的功能缺陷。
- **伪 / 已修**：
  - #27 的核心「hudi force_jni_scanner 回归」是**伪命题**（REFUTED）——SPI `HudiScanPlanProvider` 已完整 honor `force_jni_scanner`（读 session、COW 在 force_jni 下改走 JNI、烘焙 flag 进 split 供无 session 的 `populateRangeParams` 保持一致、跳过 native dict），并有专门单测 `HudiForceJniTest` 锁定。去掉伪回归后仅剩纯架构观察（多连接器各自实现 reader-type 决策 + 3 种线上编码 + thrift 不对称），因三种湖格式删除/读语义本质不同，属 nit 而非 bug。
  - #28 是**已修陈旧债**（STALE_FIXED）——detect-and-delegate 已在 #65473 与「hms 进 SPI_READY_TYPES」原子一起落地，报告预测的失效模式从未真正出现。报告本身诚实标注为「分阶段债」，判断质量高。
- **共性根因**：两条发现都源于「报告成文时间点」与「当前工作树」之间的代码演进（尤其 2026-07-20 前后的 #65473 与 hudi force_jni honor 实现）。这类发现应始终以当前工作树为基线复核，而非轻信 survey 描述。
- **与其他类别的关联**：#28 的 sibling 委派机制（`resolveSiblingOwner` + child-first classloader）与「iceberg-on-HMS 委派需补 e2e」（见 MEMORY 中 `hms-iceberg-delegation-needs-e2e`）相关——当前休眠单测只证路由，异构 HMS 目录的 INSERT/DELETE/MERGE/ALTER/EXECUTE e2e 回归仍待统一补齐；这是后续工作项，而非本发现的功能缺陷。#27 若真要收敛统一 ReaderType 枚举，会碰「fe-core 只出不进 / 禁 scaffolding」铁律，应作为独立设计任务而非挂在本发现名下。
