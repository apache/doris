# P4 翻闸完整性审计 — MaxCompute 全操作 SPI 路由 / legacy 零可达（静态分发面）

> **任务 0**（用户 2026-06-08 新增）：「确认所有 maxcompute 的操作，都走到新的 SPI 框架上，不允许回退到老的代码上。」
> **方法**：4 路 **clean-room 并行 subagent**（read / write / DDL / metadata）逐 op trace「FE 入口 → SPI 实现」+「legacy 零可达」；主线独立核 foundational（CatalogFactory / PluginDrivenExternalCatalog）+ 2 项对抗交叉核查（GSON replay、batch SPI default）。agents **不喂**历史「已修/已坏」结论（[[clean-room-adversarial-review-pref]]），主线持先验、事后交叉核对 2026-06-07 domain-6 裁决。
> **范围**：24 op（读 6 / 写 6 / DDL 6 / 元数据 6）。**与 🅰 live e2e 并列为 🅱 Batch-D 删 legacy 的两大解锁门：本审计 = 静态分发面，🅰 = 运行时真值面。**
> **来源**：4 subagent 报告（read=aa148bc6 / write=a5852ff0 / ddl=afb77712 / metadata=a1b491a8）+ 主线核码。

---

## 结论（TL;DR）

**24/24 op 全 ROUTE✅ — 0 FALLBACK / 0 GAP。** `max_compute` 的每一类操作在运行时均经 PluginDriven SPI 框架；**无任何静默回退到 legacy `MaxCompute*` 路径**。所有 legacy 删除候选（HANDOFF 列 8 个 + 审计新增 ~9 个）全部确认运行时死（live dispatch + GSON replay 两路均闭）。

➡️ **静态分发面门 = PASS。** Batch-D 删 legacy 在**静态轴解锁**；执行仍 gated on 🅰 **live e2e**（运行时真值面，CI 跳）。本审计**不**替代 e2e。

➡️ 本结论**再确认**了 2026-06-07 复审 domain-6 的「dispatch 基本干净 / legacy 死而存」裁决（`P4-maxcompute-full-rereview-2026-06-07.md:138`），但以 4 路独立 clean-room + 逐 op file:line 证据重建，不信任何「已修」标签（Rule 8/12）。当年两度被证伪的，是 INSERT OVERWRITE 网关 / 分区裁剪 / DROP DB FORCE / CREATE DB 预检等**行为 parity**（非 dispatch 可达性），且**均已在 P0/P1-4/P2/P3 + G 系列修复并在此确认 dispatch-clean**。

---

## 0. 决定性机制（linchpin，4 路独立收敛于此）

整张审计塌缩为**一个事实**：

> **`max_compute` 运行时的 catalog / db / table 对象恒为 `PluginDrivenExternal{Catalog,Database,Table}`，绝不为 legacy `MaxComputeExternal*`。** 故代码中每一处 legacy 分支（皆为 `instanceof MaxComputeExternalCatalog` / `instanceof MaxComputeExternalTable` / `instanceof PhysicalMaxComputeTableSink` / `instanceof UnboundMaxComputeTableSink` 守卫）在运行时**结构性为 false**，紧邻的 PluginDriven 分支接住该 case 抵达 SPI。

证据链（主线 + 4 agent 交叉确认）：
- **Catalog**：`CatalogFactory.java:51-52` `SPI_READY_TYPES ∋ "max_compute"` → `:105-113` 建 `PluginDrivenExternalCatalog`；legacy `MaxComputeExternalCatalog` **不在** `:134-161` fallback switch，全 fe-core main **零 `new MaxComputeExternalCatalog`**。
- **DB**：`PluginDrivenExternalCatalog.buildDbForInit:482-486` 强制 `InitCatalogLog.Type.PLUGIN` → 建 `PluginDrivenExternalDatabase`（`ExternalCatalog:950-951`）；`case MAX_COMPUTE`（`ExternalCatalog:938-939`）不可达。
- **Table**：`PluginDrivenExternalDatabase.buildTableInternal:41` 建 `PluginDrivenExternalTable`；legacy `MaxComputeExternalTable` 仅 `MaxComputeExternalDatabase.buildTableInternal:43` 建（该 db 从不为 mc 创建）。两类均直接 `extends ExternalTable`，类型不相交。
- **Replay/反序列化**：`GsonUtils.java:411 / :463 / :484` 三注册 `registerCompatibleSubtype(PluginDrivenExternal{Catalog,Database,Table}.class, "MaxComputeExternal{Catalog,Database,Table}")` → 老镜像的三类 legacy 类型串**全部反序列化为 PluginDriven**。replay 路同样不实例化 legacy。（[[catalog-spi-gson-migrate-all-three]] 三注册齐备；第二参为字符串字面量、不 import legacy 类，删类后仍有效、应保留。）
- **Txn**：`PluginDrivenExternalCatalog.initLocalObjectsImpl:118` 置 `transactionManager = new PluginDrivenTransactionManager()`。

---

## 1. 读路径（6/6 ROUTE✅）

| Op | 判定 | FE 入口 (file:line) | SPI 实现 (file:line) | legacy 可达？(判据) |
|---|---|---|---|---|
| 表扫描（ScanNode 选型） | ROUTE✅ | `PhysicalPlanTranslator:753`（`instanceof PluginDrivenExternalTable` **先匹配**）→ `PluginDrivenScanNode.create:756` | `PluginDrivenScanNode:91`/ctor`:150` → `MaxComputeScanPlanProvider.planScan:178` | 否 — `new MaxComputeScanNode`@`translator:800` 在 `else if instanceof MaxComputeExternalTable` 下、类型不可达 |
| 分区裁剪（P1-4） | ROUTE✅ | `PruneFileScanPartition:64`（`supportInternalPartitionPruned`=true @`PluginDrivenExternalTable:205`）→ `translator:761` 转发 `SelectedPartitions` → `setSelectedPartitions:158` | `resolveRequiredPartitions:172` → `getSplits:409` → `planScan(...requiredPartitions):180` → `toPartitionSpecs:211/262`（喂 ODPS read session） | 否 — 同门；裁剪到零 `:410-412` 短路空 split（镜像 legacy） |
| 谓词下推（G0/G2） | ROUTE✅ | `PluginDrivenScanNode.convertPredicate:322` + `buildRemainingFilter:791` | `MaxComputeScanPlanProvider.convertFilter:273/295` → `MaxComputePredicateConverter.convert:87` | 否 — legacy 谓词转换在不可达的 legacy node 内 |
| limit-split（P3-9） | ROUTE✅ | `getSplits:398` `tryPushDownLimit:363` / `effectiveSourceLimit:425` | `MaxComputeScanPlanProvider.shouldUseLimitOptimization:441`（三闸）→ `planScanWithLimitOptimization:375` | 否 — 连接器局部；session var `enable_mc_limit_split_optimization` @`:426`（默认 OFF） |
| batch-mode（P3-11） | ROUTE✅ | `PluginDrivenScanNode.isBatchMode:455` / `numApproximateSplits:507` / `startSplit:525` | `shouldUseBatchMode:491` + `supportsBatchScan:250`（`getFileNum()>0`）→ `planScanForPartitionBatch:560`（SPI default `ConnectorScanPlanProvider:166-174` **委托 6 参 planScan**，已核非 no-op） | 否 — legacy batch 路在不可达 node 内；异步走 `ExtMetaCacheMgr.getScheduleExecutor` |
| CAST 剥壳（F9） | ROUTE✅ | `buildRemainingFilter:791` → `metadata.supportsCastPredicatePushdown:798` | `MaxComputeConnectorMetadata.supportsCastPredicatePushdown:332`=**false** → 剥壳保 BE-only `:799-810`（`pruneConjunctsFromNodeProperties:650` 复入） | 否 — mc 无 legacy 谓词处理执行 |

---

## 2. 写路径（6/6 ROUTE✅）— 历史 3 blocker 重灾区，本审计确认 dispatch-clean

A/B 分叉 = `UnboundTableSinkCreator` 单一 `instanceof curCatalog`（3 overload 一致）：mc 为 `PluginDrivenExternalCatalog` → `UnboundConnectorTableSink`（`:68`）；legacy `UnboundMaxComputeTableSink` 仅 `instanceof MaxComputeExternalCatalog`（`:66/105/146`）下建、不可达。

| Op | 判定 | FE 入口 (file:line) | SPI 实现 (file:line) | legacy 可达？(判据) |
|---|---|---|---|---|
| INSERT INTO（sink+executor） | ROUTE✅ | `UnboundTableSinkCreator:68`；executor `InsertIntoTableCommand:593/616` | `BindSink.bindConnectorTableSink:911` → `LogicalConnectorTableSink:927` → impl 规则 `…ToPhysicalConnectorTableSink:36` → `PhysicalPlanTranslator.visitPhysicalConnectorTableSink:645` → `PluginDrivenTableSink:679`；`PluginDrivenInsertExecutor:616` | 否 — `PhysicalMaxComputeTableSink`/`MaxComputeTableSink`/`MCInsertExecutor` 守在 `instanceof PhysicalMaxComputeTableSink`（`translator:593`、`InsertIntoTableCommand:562/588`），该物理 sink 从不产出；legacy impl 规则（`RuleSet:233/281`）仅匹配 `LogicalMaxComputeTableSink`、从不创建 |
| **INSERT OVERWRITE**（网关+下层，NG-1） | ROUTE✅ | 网关 `InsertOverwriteTableCommand.allowInsertOverwrite:318`；下层 `:218` + 重插 `insertIntoPartitions:438` | 网关 → `PluginDrivenExternalTable` 分支 `:325` → `pluginConnectorSupportsInsertOverwrite:337` → `MaxComputeConnectorMetadata.supportsInsertOverwrite:310`=**true**；重插 `PluginDrivenInsertCommandContext.setOverwrite(true):447` → `PluginDrivenTableSink:234` → `MaxComputeWritePlanProvider:92 isOverwrite` → `builder.overwrite(true):168` | 否 — `:324` MaxComputeExternalTable 分支 + `:417` legacy overwrite 均需 legacy 类、不可达。**网关不挡死**（连接器返 true） |
| 事务 begin/commit/block-id（GC1） | ROUTE✅ | `BaseExternalTableInsertExecutor:68`（`transactionManager = catalog.getTransactionManager()`）；block-id RPC `FrontendServiceImpl.getMaxComputeBlockIdRange:3680` | `PluginDrivenTransactionManager`（`PluginDrivenExternalCatalog:118`）；`PluginDrivenInsertExecutor.beginTransaction:82-88`（`usesConnectorTransaction`=true @`MaxComputeConnectorMetadata:344`）→ `:361` 建 `MaxComputeConnectorTransaction:363`；全局 `PluginDrivenTransactionManager.begin:80 putTxnById`；`:3694 getTxnById` → `allocateWriteBlockRange:133` | 否 — legacy `MCTransaction` 从不注册进全局 registry |
| sink 必需物理属性（local-sort/并行，P0-2） | ROUTE✅ | impl 规则 `…ToPhysicalConnectorTableSink:36`；`RequestPropertyDeriver` 消费 | `PhysicalConnectorTableSink.getRequirePhysicalProperties:142`（动态分区 hash-distribute + `MustLocalSortOrderSpec:178-188`，由连接器分区能力门控） | 否 — `PhysicalMaxComputeTableSink.getRequirePhysicalProperties` 该物理 sink 从不产出 |
| bind 投影（P0-3） | ROUTE✅ | `BindSink:173` | `bindConnectorTableSink:911`；full-schema 重排 `requiresFullSchemaWriteOrder:941`；`selectConnectorSinkBindColumns:971` | 否 — `bindMaxComputeTableSink:864` 仅 `UnboundMaxComputeTableSink`（`:171`）触发、从不创建 |
| post-commit refresh（P3-12） | ROUTE✅ | `InsertIntoTableCommand:616` | `PluginDrivenInsertExecutor.doAfterCommit:190`（swallow-and-warn，DV-018） | 否 — `MCInsertExecutor.doAfterCommit` 不可达 |

---

## 3. DDL 路径（6/6 ROUTE✅）

`PluginDrivenExternalCatalog` override 四 DDL（`createTable:267` / `createDb:336` / `dropDb:377` / `dropTable:406`），均 `connector.getMetadata(session).*`；`metadataOps` **恒 null** 但只路由到死的「not supported」base 分支（四 op 全 override），replay `afterX` helper 有显式 plugin-path else（`ExternalCatalog:1023-27/1049-52/1085-88/1143-46`）。

| Op | 判定 | FE 入口 (file:line) | SPI 实现 (file:line) | legacy 可达？(判据) |
|---|---|---|---|---|
| CREATE TABLE | ROUTE✅ | `CreateTableCommand:91` → `Env:3752 catalogIf.createTable` | `PluginDrivenExternalCatalog:267` → `MaxComputeConnectorMetadata:389` | 否 — `CreateTableInfo:391/920 instanceof MaxComputeExternalCatalog`=FALSE（`:393/:922` PluginDriven）；`MaxComputeMetadataOps` 仅绑于从不实例化的 legacy catalog `:232` |
| CTAS | ROUTE✅ | `CreateTableCommand:103`（create）+ `:110`（insert） | create 同上；insert `UnboundTableSinkCreator:69 UnboundConnectorTableSink` | 否 — `UnboundTableSinkCreator:66` 死；IF-NOT-EXISTS 短路 `PluginDriven:290-294` 返 true → `:104` 跳 insert |
| DROP TABLE | ROUTE✅ | `DropTableCommand:89` → `Env:5035 catalogIf.dropTable`（**无 instanceof**） | `PluginDrivenExternalCatalog:406` → `MaxComputeConnectorMetadata:449` | 否 — 多态分发命中 override；legacy 需 null metadataOps |
| CREATE DATABASE | ROUTE✅ | `CreateDatabaseCommand:69` → `Env:3645 catalogIf.createDb`（无 instanceof） | `PluginDrivenExternalCatalog:336` → `MaxComputeConnectorMetadata:471`；IF-NOT-EXISTS 远端 `databaseExists:350`（`supportsCreateDatabase:466`） | 否 |
| DROP DATABASE FORCE | ROUTE✅ | `DropDatabaseCommand:76` → `Env:3671 catalogIf.dropDb`（无 instanceof） | `PluginDrivenExternalCatalog:377` → `MaxComputeConnectorMetadata:478`；`force` 透传；级联删表 `:480-493`（ODPS 不自级联，镜像 legacy） | 否 |
| CREATE CATALOG 校验（G6） | ROUTE✅ | `CatalogMgr:277/559` → `CatalogFactory:106/169` + `PluginDrivenExternalCatalog:158` → `ConnectorFactory:97` | `MaxComputeConnectorProvider.validateProperties:59` + `preCreateValidation`（PluginDriven `:174`） | 否 — legacy `MaxComputeExternalCatalog.checkProperties:388` 不可达（类从不实例化） |

---

## 4. 元数据路径（6/6 ROUTE✅）

每处 legacy 分支为 `instanceof MaxComputeExternalCatalog/Table` 守卫、运行时 FALSE，紧邻 PluginDriven 分支接住。

| Op | 判定 | FE 入口 (file:line) | SPI 实现 (file:line) | legacy 可达？(判据) |
|---|---|---|---|---|
| list databases | ROUTE✅ | `PluginDrivenExternalCatalog.listDatabaseNames:216` | `MaxComputeConnectorMetadata.listDatabaseNames:95` | 否 — legacy catalog 不实例化 |
| list tables | ROUTE✅ | `listTableNamesFromRemote:222` | `listTableNames:105` | 否 — 同 |
| get schema | ROUTE✅ | `PluginDrivenExternalTable.initSchema:118` | `MaxComputeConnectorMetadata.getTableSchema:130` → `PluginDrivenSchemaCacheValue:175` | 否 — `MaxComputeExternalMetaCache` 仅经 `MaxComputeExternalTable:122` 触达（从不建）；`ExternalMetaCacheRouteResolver:75 instanceof`=FALSE → `ENGINE_DEFAULT:89` |
| DESCRIBE / isKey（P3-10） | ROUTE✅ | `initSchema` → `ConnectorColumnConverter.convertColumns:67` | `MaxComputeConnectorMetadata.buildColumn:178-181`（`isKey=true`） | 否 — 同 get schema |
| **SHOW PARTITIONS** | ROUTE✅ | `ShowPartitionsCommand.handleShowPartitions:458`（`instanceof MaxComputeExternalCatalog`=FALSE）→ `:460` | `handleShowPluginDrivenTablePartitions:312` → `MaxComputeConnectorMetadata.listPartitionNames:237` | 否 — `handleShowMaxComputeTablePartitions:292` / `MaxComputeExternalCatalog.listPartitionNames:258` 不可达 |
| **partitions() TVF** | ROUTE✅ | `MetadataGenerator.partitionsMetadataResult:1315`（FALSE）→ `:1317`；TVF analyze `PartitionsTableValuedFunction:204`（FALSE）→ `:210` | `dealPluginDrivenCatalog:1359` → `listPartitionNames:237` | 否 — `dealMaxComputeCatalog:1344` 不可达 |

---

## 5. legacy 删除候选 disposition（Batch-D 静态前置门 = 全 PASS）

下列类/方法在**全部 4 域审计的并集**上对 `max_compute` **运行时零可达**（live dispatch + replay 双闭）。HANDOFF 列 8 个 + 审计新增（标 🆕）：

| legacy 工件 | 运行时状态 | 死因（判据） |
|---|---|---|
| `MaxComputeExternalCatalog` | 死 | 从不 `new`；`GsonUtils:411` compat → PluginDriven |
| `MaxComputeExternalDatabase` 🆕 | 死 | `buildDbForInit` 强制 PLUGIN；`GsonUtils:463` compat |
| `MaxComputeExternalTable` | 死 | `buildTableInternal:43` 从不触达；`GsonUtils:484` compat。残引 `translator:598/799`、`BindSink:866`、`source/MaxComputeScanNode`、`MCInsertExecutor` 均 instanceof-死分支 |
| `MaxComputeMetadataOps` | 死 | 仅绑于 `MaxComputeExternalCatalog:232`（从不实例化） |
| `MaxComputeExternalMetaCache` 🆕 / `MaxComputeSchemaCacheValue` 🆕 | 死 | 仅经 legacy MetaCache/Table 触达 |
| `source/MaxComputeScanNode` / `MaxComputeSplit` 🆕 | 死 | `translator:800` else-if 死分支 |
| `MCTransaction` | 死 | 从不注册进全局 txn registry |
| `PhysicalMaxComputeTableSink` / `MaxComputeTableSink`(planner) | 死 | 该物理 sink 从不产出 |
| `bindMaxComputeTableSink`（BindSink 方法） | 死 | 仅 `UnboundMaxComputeTableSink:171` 触发 |
| `UnboundMaxComputeTableSink` 🆕 / `LogicalMaxComputeTableSink`+impl 规则(`RuleSet:233/281`) 🆕 | 死 | 仅 `instanceof MaxComputeExternalCatalog` 下建 / 仅匹配 LogicalMaxComputeTableSink |
| `MCInsertExecutor` 🆕 | 死 | executor 从不为 mc 实例化 |
| `allowInsertOverwrite` MC 分支(`:324`) | 死 | instanceof MaxComputeExternalTable 不可达 |

⚠️ **Batch-D 删除须知**：上列均**运行时死、但编译期仍被 instanceof 守卫 / RuleSet 注册 / 残 import 引用**。删 legacy 类须**连同其已死分支/注册原子删除**否则不编译（横切复核 `MetadataGenerator`/`PartitionsTableValuedFunction`/`translator`/`BindSink`/`InsertOverwriteTableCommand`/`UnboundTableSinkCreator`/`RuleSet` 的 reverse-ref）。**唯独 `GsonUtils:411/463/484` 三 compat 行用字符串字面量、不 import legacy 类 → 删类后仍有效、应保留**（老镜像反序列化兼容）。

---

## 6. 范围外 / 开放项（非本审计否决项；均为行为面、非路由面）

本审计 = **静态 FE 分发面**。下列不在范围、不影响「零 legacy 回退」结论，但为 🅱 删 legacy 真正完成所需：
- **🅰 live e2e（真实 ODPS）= 运行时真值面门**，仍是翻闸真正完成门（CI 跳）。所有 DV 真值闸（DV-013..022 等）须 live 验。
- **BE 侧执行**：JNI scanner 消费 `MaxComputeScanRange` / sink BE 端写 / `onComplete` 真实 ODPS commit / overwrite BE 是否真 honor `builder.overwrite(true)` — 跨 FE→BE，本审计仅 trace FE dispatch。
- **converter 全类型 parity**：`ExprToConnectorExpressionConverter` 是否逐 Expr kind 忠实翻译，未逐一 diff legacy（路由已定，行为待 converter 级 parity 测）。
- 这些与本审计**正交**：即便其中有行为差异，也不构成「回退到 legacy 代码」（legacy 代码不执行）。

---

## 7. 方法与可信度

- **4 路 clean-room 并行 subagent**（general-purpose），各仅得架构事实 + op 清单，**不得**历史「已修/已坏」结论 → 独立判断、避免开发先验带偏（[[clean-room-adversarial-review-pref]]）。
- **四路独立收敛于同一 linchpin**（catalog/db/table 恒 PluginDriven，legacy 守卫结构性 FALSE）= 强交叉验证。
- **主线对抗交叉核查**：① 独立核 `CatalogFactory` + `PluginDrivenExternalCatalog` 全文（foundational）；② GSON 三注册（replay 闭环）；③ batch SPI default 委托（非 no-op）；④ 对照 2026-06-07 domain-6 裁决一致。
- **可信度：高**。单一决定性事实可证；逐 op file:line 均经直读。
- **不信任何「已修」标签**（Rule 8/12）：当年两度证伪的是行为 parity（已修），本轮独立证 dispatch 可达性本身 clean。

**裁决：静态分发面完整性门 = PASS。零 legacy 运行时回退。Batch-D 删 legacy 静态轴解锁，gated on 🅰 live e2e。**
