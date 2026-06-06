# P3-T08 设计 — `tableFormatType` 分流消费（design-only，单 `hms` catalog 多格式路由）

> 关联：[tasks/P3-hudi-migration.md](../P3-hudi-migration.md)（批 D / T08）、[D-005](../../decisions-log.md)（DLA 用 tableFormatType）、[D-019](../../decisions-log.md)（hybrid）、[HANDOFF 关键认知 3](../../HANDOFF.md)。
> 直接输入：[research/spi-multi-format-hms-catalog-analysis.md](../../research/spi-multi-format-hms-catalog-analysis.md)（6-reader code-grounded recon，本场未重复 recon，只核读 load-bearing 锚点）。
> 用户签字（AskUserQuestion，2026-06-05）：M2 路由 = **方案 B（per-table SPI provider）** → 本场记 **[D-020](../../decisions-log.md)**。
> **性质 = design-only**：不动 fe-core live 路径、不实现消费（实现 = 批 E/P7）、不碰 `SPI_READY_TYPES` / legacy / 非 hudi 连接器。

---

## Problem

批 D 目标：写清**单个 `hms` catalog 如何按 per-table `tableFormatType` 把表路由到 HUDI/HIVE/ICEBERG**，明确 fe-core 的消费契约与 SPI seam，作为 (a) 模型落地（批 E/P7）的入口设计。

legacy 用 `HMSExternalTable.dlaType`（per-table tag）+ 处处 `switch(dlaType)` 实现"同一 hms catalog 多格式"。SPI 侧 **只复刻了 tag 的产生，没复刻消费**（research §1/§6①）。T08 = 设计这个消费。

---

## Recon 结论（load-bearing 锚点本场已核读，非沿用近似行号）

### 1. keystone gap = `tableFormatType` 产而不用（firsthand 确认）
- `HiveConnectorMetadata.getTableSchema` per-table 探测并设 `ConnectorTableSchema.tableFormatType`（research §3.3，`HiveConnectorMetadata.java:134-154` / `detectFormatType:282-294`）——**产生端就位**。
- 但 `PluginDrivenExternalTable.initSchema`（`PluginDrivenExternalTable.java:79-109`）拿到 `tableSchema` 后**只迭代 `getColumns()`**（`:96-105`）、返回 `SchemaCacheValue(columns)`（`:107-108`），**从不调 `tableSchema.getTableFormatType()`**——格式信号在 fe-core 边界丢弃。✅ 确认 research §6①。
- `ConnectorTableSchema.getTableFormatType()`（`ConnectorTableSchema.java:58-60`）存在、是 final 不可变字段——载体就绪，缺消费。

### 2. 第二个 fe-core 消费缺口 = 身份/引擎名 per-catalog 而非 per-table（firsthand 新增）
- `PluginDrivenExternalTable.getEngine()`（`:195-215`）/ `getEngineTableTypeName()`（`:217-231`）**switch 的是 catalog type**（`jdbc`/`es`/`trino-connector`），多格式 `hms` catalog 一律落 `default`。→ 一个 hms catalog 里的 hudi/hive/iceberg 表对用户显示同一引擎名，与 legacy（per-table 引擎）不符。这是 M1 的第二处落点。

### 3. scan 侧 SPI 是 per-catalog 单 provider（firsthand 确认）
- `Connector.getScanPlanProvider()`（`Connector.java:40-42`）默认返 null、**per-catalog 一个**；`HiveConnector` 恒返 `HiveScanPlanProvider`（research §6③）。
- 但 `ConnectorScanPlanProvider.planScan(session, handle, columns, filter)`（`ConnectorScanPlanProvider.java:62-66`，+5-arg limit 重载 `:82-89`）**入参带 per-table `ConnectorTableHandle handle`**——即 per-table 信息在 scan 规划时**可得**（handle 已携 `HiveTableHandle.tableType`）。这是 M2 三方案都能落脚的前提。
- `ConnectorMetadata`（`ConnectorMetadata.java:37-44`）**当前无** `getScanPlanProvider(handle)`——方案 B 的新增点。

### 4. 关键拆解：M1（身份消费）⊥ M2（scan 路由）
本设计的核心分析贡献——keystone gap 拆成两个**可分离**子问题：

| | M1 身份消费 | M2 scan 路由 |
|---|---|---|
| 是什么 | fe-core 读 `tableFormatType` 做 per-table **引擎名/表身份/information_schema** | 单 `hms` connector 为非-Hive 表产出 **Hudi/Iceberg scan plan** |
| 落点 | `PluginDrivenExternalTable`（initSchema 缓存 + getEngine 消费）| SPI seam（A/B/C 三方案分歧处）|
| 是否随 A/B/C 变 | **否**——三方案 M1 设计相同 | **是**——这是 D-020 的命题 |
| fe-core 是否需懂格式语义 | 否（opaque string，逐字上报）| 否（经 handle 路由，热路径不读字符串）|

→ M1 在所有方案中一致；A/B/C 只在 M2 分歧。**这是把"keystone"可控化的关键**。

---

## 决策：M2 = 方案 B（[D-020]，用户签字）

研究浮现三条互斥路由方案（research §8）。本场逐条 code-grounded 评估后由用户拍板 **B**：

| 方案 | 机制 | SPI churn | fe-core 是否长格式分派 | 网关依赖代价 | 裁决 |
|---|---|---|---|---|---|
| **A** 连接器内 router | `Connector.getScanPlanProvider()` 返回一个 `planScan` 按 `handle.getTableType()` 委派的 router | **零**（现 SPI 即可，planScan 已带 handle）| 否 | hive→hudi/iceberg 依赖边 | 备选 |
| **B** ✅ per-table SPI provider | 新增 **backward-compat default** `ConnectorMetadata.getScanPlanProvider(handle)`；fe-core 优先用它、回落 `Connector` 的 | 一个 default 方法（D-009 合规）| 否 | 同 A（网关 impl 仍需多格式依赖）| **选定** |
| **C** fe-core 发现期分派 | fe-core 读 `tableFormatType` 建 format-specific 表对象（≈legacy DLAType→多态 DlaTable）| —（fe-core 侧）| **是**（与 import-gate/D-003/D-006 瘦 fe-core 北极星相悖）| — | 否决 |

**B 选定理由**（用户决策）：把 per-table 选 provider 升为**一等 SPI 契约**（最干净的 per-table 语义），优于 A 把路由藏进连接器 router；同时以**向后兼容 default 方法**落地（满足 [D-009]：本计划只新增 default、不破签名），不构成 breaking change。代价（网关 impl 需多格式依赖）A/B 同担，非 B 独有。C 因 fe-core 回退到 per-format 分派、违背瘦 fe-core 目标被否决。

> **与 D-005 的关系（须留痕）**：[D-005] 定"用 `tableFormatType` 区分 + fe-core 据此 dispatch 到对应 `PhysicalXxxScan`"。其**区分符**结论 D-020 完全沿用；但"dispatch 到 `PhysicalXxxScan`"措辞写于 2026-05-24，**早于 P1 的 scan-node 统一**（`visitPhysicalFileScan`→单 `PluginDrivenScanNode` + per-range format，P1-T03/T04）——SPI 路径已无 per-format `PhysicalXxxScan`。D-020 = 在统一后的 SPI 架构下**操作化** D-005 的区分符消费，scan dispatch seam 由"fe-core→PhysicalXxxScan"改为"`ConnectorMetadata.getScanPlanProvider(handle)`→per-table provider"。D-020 不推翻 D-005，是其机制细化。

---

## M1 设计 — `PluginDrivenExternalTable` 消费 `tableFormatType`（design-only）

> fe-core 侧，**所有 M2 方案通用**。实现 = 批 E。

1. **缓存 per-table 格式（与 schema 同生命周期）**：`initSchema`（`:93` 之后）读 `tableSchema.getTableFormatType()`，随 schema 一并缓存。**推荐**新 `PluginDrivenSchemaCacheValue extends SchemaCacheValue`（plugin 私有，不污染其他 external table 的 `SchemaCacheValue`），携 `Optional<String> tableFormatType`。备选：(b) 用时经 `metadata.getTableSchema(handle)` 重取（无状态但多 round-trip）；(c) transient 字段（缓存淘汰/反序列化丢失，否决）。
2. **身份/引擎名 per-table 化**：`getEngine()` / `getEngineTableTypeName()` 在 catalog type 为多格式族（如 `hms`）时，改用缓存的 `tableFormatType` 作 per-table 引擎名。**fe-core 保持格式无关**——`tableFormatType` 作 **opaque 连接器选定串逐字上报**（连接器选规范值），**禁止** fe-core 长出 `switch("HUDI"→...)`。
3. **能力门控不进 fe-core**：legacy 按 dlaType 门控 time-travel/MTMV/snapshot；SPI 侧这些已是连接器职责（`ConnectorMetadata` MVCC default opt-out=T06、time-travel fail-loud=T04 在 `visitPhysicalHudiScan`）。→ M1 **不需要** fe-core 按格式门控，进一步减 fe-core 格式知识。
4. **热路径不读字符串**：scan 路由走 M2（经 handle），**不**经 fe-core 读 `tableFormatType` 字符串再分支——M1 的 fe-core 字符串消费**仅服务身份/上报**，热路径零格式 switch。

---

## M2 设计 — 方案 B per-table provider seam（design-only）

> 实现 = 批 E（+ iceberg 部分依赖 P6/M3）。

1. **SPI 新增**（`fe-connector-api`，backward-compat default）：
   ```
   // ConnectorMetadata
   default ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
       return null;   // 默认回落 per-catalog Connector.getScanPlanProvider()
   }
   ```
   默认 null → 现有所有连接器（jdbc/es/trino/iceberg/独立 hudi/hive）**零影响**（满足 [D-009]）。
2. **fe-core scan 路径**（唯一 scan 侧改动）：`PluginDrivenScanNode.getSplits()`（research `:356-378`）由 `connector.getScanPlanProvider()` 改为**优先** `metadata.getScanPlanProvider(currentHandle)`、为 null 时**回落** `connector.getScanPlanProvider()`（保留现行为）。
3. **hms 网关实现**：注册 `"hms"` 的连接器 override `getScanPlanProvider(handle)`，按 `handle.getTableType()`（`HiveTableType{HIVE|HUDI|ICEBERG|UNKNOWN}`）返 Hive/Hudi/Iceberg provider；metadata 侧同理委派 `Hudi/IcebergConnectorMetadata`。→ 引入 hms-网关模块对 `-hudi`/`-iceberg` 的依赖边（A/B 同担的结构代价）。
4. **per-range 格式仍是 BE 选 reader 的最终依据**：各 provider 产出的 `ConnectorScanRange.getTableFormatType()`（Hive→`"hive"`/Hudi→`"hudi"`）→ `TTableFormatFileDesc.setTableFormatType`，BE 每 range 建对应 reader（与 legacy 等价，research §3.4 / 批 0 结论）。M2 只决定"哪个 provider 规划 split"，per-range 契约不变。

---

## 边界 / 缩界（Rule 12 不静默）

- **本场零代码**：以上 M1/M2 均设计，**不动** fe-core/SPI/连接器任何 live 文件。
- **Iceberg-on-hms 经 SPI 依赖 P6/M3**：`fe-connector-iceberg` 现**无 `ScanPlanProvider`** 且 pom **未依赖 `fe-connector-api`**（research §6④/§2.3）。B 的 `getScanPlanProvider(handle)` 对 ICEBERG 表落地需 P6 先补 `IcebergScanPlanProvider` + api 依赖。批 E/P7 落地 hms 多格式时：ICEBERG 表在 P6 前**回落 legacy `IcebergScanNode` 或 fail-loud**，不静默误扫为 Hive。
- **格式探测共享化（M5）非本场**：fe-core `HMSExternalTable.makeSureInitialized` 与 SPI `HiveTableFormatDetector` 两份逻辑的 drift 防护（抽共享层）留 P7。
- **gate 不动**：`SPI_READY_TYPES` 不含 hms/hudi/iceberg（`CatalogFactory.java:52`），整族走 legacy；B 落地后仍需批 E 翻闸 + cutover + image 兼容（R-001）。

---

## Implementation Plan（批 E/P7，非本场）

> 登记依赖序，供批 E 接手；本场不执行。

1. **M1**：`PluginDrivenSchemaCacheValue` + `initSchema` 缓存 `tableFormatType` + `getEngine/getEngineTableTypeName` per-table 化（fe-core，opaque 串）。
2. **M2-SPI**：`ConnectorMetadata.getScanPlanProvider(handle)` default null（`fe-connector-api`，default 方法）。
3. **M2-fe-core**：`PluginDrivenScanNode.getSplits` 优先 metadata-per-table provider、回落 connector-per-catalog。
4. **M2-网关**：hms 连接器 override `getScanPlanProvider(handle)` + metadata 委派；加 `-hudi`/`-iceberg` 依赖边。
5. **M4**：hms 探测为 HUDI 的表交 Hudi 路径（+ ICEBERG 待 P6/M3）。
6. **翻闸/cutover/删 legacy/集群验证**：T09–T11（批 E），含 image 兼容（R-001）。

---

## Risk Analysis

- **本场零 live 风险**：design-only，gate 关，无代码改动。
- **B 的 SPI 表面演进**：新 default 方法是向后兼容（D-009），但把"scan provider 来源"从 per-catalog 单点变为 per-table 优先+回落，**所有连接器的 scan plumbing 经此分叉**——批 E 实现时需回归全连接器（jdbc/es/trino 走 null 回落路径必须等价）。已登记。
- **网关依赖边**：hms→hudi/iceberg 耦合模块 build/release（A/B 同担）；批 E 落地前评估是否反而触发 M2 的 (A) 更轻。**本设计记录 B 为方向，实现期可据 iceberg 接入复杂度复核**（D-020 关联 open question）。
- **D-005 机制措辞陈旧**：已在 D-020 留痕 supersede，避免下游按 `PhysicalXxxScan` 旧措辞误实现。

---

## Test Plan

- **本场无单测**（design-only，零代码）。
- **批 E 落地时**（登记，R12 不静默跳过）：
  - fe-core 单测：`PluginDrivenExternalTable` per-table `getEngine` = 缓存 `tableFormatType`；多格式 hms catalog 下 hudi/hive/iceberg 表引擎名各异。
  - SPI 回归：现有连接器（jdbc/es/trino）`getScanPlanProvider(handle)` 返 null → 回落 per-catalog provider，行为等价（防 B 的 plumbing 分叉回归）。
  - 端到端（翻闸后）：单 hms catalog 混合 Hive+Hudi(+Iceberg) 表，per-table 走对的 scan/reader，vs legacy parity。

---

## Decisions / Deviations

- **[D-020]**（本场新增，用户签字）：M2 路由 = 方案 B（`ConnectorMetadata.getScanPlanProvider(handle)` per-table default），design-only，实现批 E/P7。**细化 D-005**（沿用 tableFormatType 区分符；机制由"fe-core→PhysicalXxxScan"更新为 per-table provider seam，因 P1 已统一 scan-node）。
- 无新 DV：B 与 D-005/D-009 一致，D-005 机制措辞更新已收于 D-020 留痕（非偏差，是决策细化）。
- Open（转批 E/P7，承自 research §10）：Iceberg-on-hms 委派 vs 回落 legacy（依赖 P6/M3）；连接器生命周期双创建（`PluginDrivenExternalCatalog:87-145`，`HmsClient` 是否重复建）；探测 drift 共享化（M5）。
