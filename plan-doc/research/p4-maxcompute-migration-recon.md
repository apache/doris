# P4 maxcompute 迁移 — code-grounded recon

> 产出于 P4 启动（2026-06-06）。方法：5 路只读 Explore subagent code-grounded 调研 + 主线 firsthand 核读 load-bearing 锚点。
> 用途：research-design-workflow 的 research note；scope fork 的事实底座。引用此文写 `tasks/P4` 设计备忘。

---

## 0. 头条结论（与 master plan §3.5 假设的偏差）

**maxcompute 会写（live write/transaction/DDL 路径，且在热区）——它不是 trino-connector（P2）。**

- master plan §3.5 把 P4 当成「搬类 + 翻闸 + 删旧」的直线迁移，但 recon 揭示真正的工作与风险都在**写路径**。
- **模型本身是 clean standalone**（自有 `max_compute` catalog type + 自有 `CatalogFactory` case，**无** hudi 那种寄生/`tableFormatType` 区分符陷阱）→ 翻闸机制本身干净。
- 但**一旦翻闸**（`max_compute` 进 `SPI_READY_TYPES`），catalog 变 `PluginDrivenExternalCatalog`、表变 `PluginDrivenExternalTable`，则写路径里 15 处 `instanceof MaxComputeExternal*` **全部失配**、INSERT/DDL 断 → **不先把写路径走 SPI 就不能翻闸**。
- 写路径所需 SPI 当前**不存在**：P0 给了 E4（`ConnectorTransaction`/`ConnectorWriteOps.beginTransaction` default-throw），但 fe-core 写编排调的是 maxcompute 专有方法（`updateMCCommitData`、`allocateBlockIdRange`、`beginInsert/finishInsert`），SPI 未抽象。

→ **P4 是一个 scope fork（见 §9），需用户签字**，与 P3 的 D-019 同性质。

---

## 1. 连接器模块现状（`fe-connector-maxcompute`）= 只读骨架

13 文件 / ~2145 LOC。读路径基本可用，写/DDL/分区**全缺**。

| SPI 面 | 状态 | 锚点 / 备注 |
|---|---|---|
| Provider getType("max_compute")/create | ✅ | `MaxComputeConnectorProvider:32/37` |
| Connector getMetadata/getScanPlanProvider/testConnection/close | ✅ | `MaxComputeDorisConnector:110/117/123/165` |
| Metadata listDatabaseNames/databaseExists/listTableNames/getTableHandle/getTableSchema/getColumnHandles | ✅ | `MaxComputeConnectorMetadata`（委托 `McStructureHelper`）|
| ScanPlanProvider.planScan（双 overload）| ✅ | `MaxComputeScanPlanProvider:166/173`；谓词下推在 planScan 内（`MaxComputePredicateConverter` 264 LOC），**非** applyFilter hook |
| **createTable/dropTable/createDatabase/dropDatabase** | ❌ default-throw | DDL 全缺（legacy `MaxComputeMetadataOps` 565 LOC 有）|
| **listPartitions/listPartitionNames/listPartitionValues** | ❌ 返空 | 翻闸后 SHOW PARTITIONS / TVF 会断（legacy 走 `getOdpsTable().getPartitions()`）|
| **WriteOps / Transaction（E4）** | ❌ 全缺 | 主线 grep 实证连接器零写/事务实现 |
| applyFilter/applyProjection（hook）| ❌ 返空 | 下推改在 planScan，非缺陷 |
| 单一 stub | — | `MaxComputeScanPlanProvider:370` `checkOnlyPartitionEquality()` 恒 false（保守关 limit-opt，非 bug）|

META-INF/services 已注册。

---

## 2. legacy fe-core（`datasource/maxcompute/`）= 10 文件 / ~2978 LOC，全 MOVE

| 文件 | LOC | 角色 | 处置 |
|---|---|---|---|
| MaxComputeExternalCatalog | 458 | catalog；ODPS client、partition/table listing | MOVE |
| MaxComputeExternalDatabase | 47 | db wrapper | MOVE |
| MaxComputeExternalTable | 336 | table；schema init、类型映射、分区列 | MOVE（读写共用——见 §4）|
| **MCTransaction** | 236 | **ODPS Storage API 写 session：beginInsert/finishInsert/commit** | MOVE（写路径核心）|
| MaxComputeExternalMetaCache | 115 | schema/partition 缓存 | MOVE |
| MaxComputeSchemaCacheValue | 67 | 缓存值 | MOVE |
| **MaxComputeMetadataOps** | 565 | **DDL：CREATE/DROP TABLE/DB** | MOVE |
| McStructureHelper | 298 | db/table/partition 发现（接口+2 impl）| **DEDUP**（见 §6）|
| source/MaxComputeScanNode | 809 | 谓词下推、split 生成 | MOVE |
| source/MaxComputeSplit | 47 | split holder | MOVE |

---

## 3. 反向引用 = ~36 处（21 mechanical / 15 live-logic）——doc 旧称「12」失真

> P2 trino 仅 ~2 处全 mechanical；maxcompute 因深度耦合写/事务/分区，量级与性质都更重。

**MECHANICAL（21，可折进 PluginDriven 分支 / SPI 注册）**：`CatalogFactory:147` 工厂、`ExternalCatalog:939` db 工厂、`GsonUtils` 3 注册、`UnboundTableSinkCreator` 3 路由、`BindRelation:540`/`Alter:617`/`CreateTableInfo:390/912` case、`ShowPartitionsCommand:202`/`PartitionsTableValuedFunction:173`/`PartitionValuesTableValuedFunction:115` allow-list、`ExternalMetaCacheRouteResolver:75`、`ExternalMetaCacheMgr:183/310`、`TableIf` 枚举、`InitCatalogLog:41`、`DatasourcePrintableMap` 等。

**LIVE-LOGIC（15，需 SPI 扩展或保留专有 handler）——集中在写路径**：

| 区 | 站点 | 性质 |
|---|---|---|
| 事务（热）| `Coordinator:2539` updateMCCommitData / `FrontendServiceImpl:3697-3702` allocateBlockIdRange(RPC) / `LoadProcessor:240` updateMCCommitData | **查询/RPC 热区**，cast `MCTransaction` 调专有方法 |
| 事务 | `MCTransactionManager:27`、`MCInsertExecutor:65` beginInsert | 写编排 |
| sink | `BindSink:1084`、`PhysicalPlanTranslator:596` 建 `MaxComputeTableSink`、`MaxComputeTableSink:67` 读专有 config | 写计划 |
| DDL/命令 | `InsertIntoTableCommand:563`、`InsertOverwriteTableCommand:320` | 命令路由 |
| 读内省 | `ShowPartitionsCommand:287/415` handleShowMaxComputeTablePartitions、`PartitionsTableValuedFunction:200` getOdpsTable().getPartitions()、`MetadataGenerator:1310` dealMaxComputeCatalog、`PhysicalPlanTranslator:777` 建 MaxComputeScanNode | 读侧专有 |

> 主线已 firsthand 核读确认：`FrontendServiceImpl:3697-3702`、`Coordinator:2539`、`LoadProcessor:240` 确为 live `MCTransaction` cast。

---

## 4. 写路径 = 真正的 keystone（为何不能简单 hybrid 也不能简单翻闸）

- `MaxComputeExternalTable` **读写共用**：scan（读）与 sink/insert（写）都引用它。
- 插件模型下 fe-core **无法 import** 连接器内的类（classloader 隔离）。所以 legacy `MaxComputeExternalTable` 一旦迁入插件，fe-core 写路径（`MaxComputeTableSink`/`MCInsertExecutor`/`Coordinator`/`FrontendServiceImpl`）就**不能再引用它** → 必须先把写编排经 SPI 重新表达。
- 但**只要 gate 关**（`max_compute` 不在 `SPI_READY_TYPES`），catalog 仍是 legacy、连接器模块 dormant、legacy 写路径原封不动 → **hybrid 可行**（硬化 dormant 读连接器 + 测试，不翻闸、不碰 legacy 写）。这正是 P3 批 A–D 的形态。
- 写 SPI 抽象（`updateCommitData`/`allocateBlockIdRange`/`begin/finishInsert`/commit）当前**不存在**，是 full 迁移的前置设计；**P5 paimon 同样会写**（`PaimonMvccSnapshot` + 写路径），该 SPI 可 P4/P5 共用。

---

## 5. 翻闸 / gson / 枚举编辑点（已 pin，镜像 trino/es/jdbc）

1. `CatalogFactory:52` `SPI_READY_TYPES` 加 `"max_compute"`；删 `:146-149` legacy case。
2. `GsonUtils` registerCompatibleSubtype：`MaxComputeExternalCatalog→PluginDrivenExternalCatalog`（~:405-412）、`MaxComputeExternalTable→PluginDrivenExternalTable`（~:478-483）；保留 :397/:472 普通注册（image 兼容）。
3. `PluginDrivenExternalCatalog.legacyLogTypeToCatalogType`（~:347）：`MAX_COMPUTE` 自动 lowercase→`"max_compute"`，**无需** trino 那种连字符特例。
4. `PluginDrivenExternalTable.getEngine()/getEngineTableTypeName()`（~:203-231）：加 `case "max_compute"`（参 es/jdbc）。
5. `TableIf.TableType.MAX_COMPUTE_EXTERNAL_TABLE`（TableIf:220）+ `InitCatalogLog.Type.MAX_COMPUTE`（:41）：保留作 GSON/兼容。

---

## 6. McStructureHelper 去重（P1-T02 deferred → P4）

- fe-core 副本 298 LOC vs 连接器副本 337 LOC = **已分叉**（连接器 +39 LOC，superset）。
- fe-core 副本仅被 `MaxComputeExternalCatalog:229` 内部用，无外部 import。
- 处置：连接器副本胜出；迁移后删 fe-core 副本。

---

## 7. 测试基线

- 连接器 `fe-connector-maxcompute`：**0 测试**。
- legacy fe-core：2（`MaxComputeExternalMetaCacheTest`、`source/MaxComputeScanNodeTest`，JUnit4）。be-java-extensions：1（手写 fake）。
- 兄弟连接器镜像样板：hudi 5 / trino 4 / hive 2 / hms 1（**JUnit5 + 手写替身，无 mockito**；checkstyle 含 test 源、禁 static import）。

---

## 8. ODPS SDK classloader 隔离（R-004）

- SDK 仅在 fe-core（`com.aliyun.odps.*`：Odps/Account/TableTunnel/Storage API）；连接器模块只用类型 stub（OdpsType/TypeInfo）。
- `MaxComputeExternalCatalog` 持 per-catalog `odps`/`settings` 实例；`REGION_ZONE_MAP` static final（安全）；**无** ThreadLocal / 全局 Odps 单例。
- 裁决：**无明显 classloader 陷阱**；建议翻闸前在插件 harness 做一次防御性连通测试。

---

## 9. SCOPE FORK（待用户签字）

| 方案 | 范围 | 风险 | 交付 |
|---|---|---|---|
| **B. Hybrid（推荐，镜像 P3/D-019）** | 硬化 dormant 读连接器（补 listPartitions、schema parity、limit-opt 复核）+ 连接器测试基线；gate 关、legacy 写路径不动 | 低（gate 关，零 live 风险）| 读侧 de-risk + 测试网；**不**翻闸、**不**删 legacy |
| **A. Full P4** | 设计+建写/事务 SPI（抽象 MCTransaction 专有方法）+ 迁读写 + 重构 Coordinator/FrontendServiceImpl/sinks + 翻闸 + 删 legacy | 高（动查询/RPC 热区 + 新 SPI 设计）| maxcompute 完整收口 |
| **C. 写-SPI RFC 先行** | 先把「连接器写/事务 SPI」作为独立设计产出（P4 maxcompute + P5 paimon 共用），再做 full P4 | 中（设计前置，跨阶段摊销）| 共享写 SPI + 之后 full |

**推荐 B**：与 P3 一致、合用户「caution over speed」、gate 关零风险。代价：交付偏「准备」（不含 cutover），且写 SPI 工作迟早要做（P5 也需）。
若用户要现在就投资写 SPI（P4+P5 共用），则 C→A。

---

## 10. 沿用坑（来自 HANDOFF/PROGRESS）

- rebase 后 fe-core stale 生成 `DorisParser` → cannot find symbol：**clean fe-core**（非代码 bug）。
- import-gate 只禁 connector→fe-core 单向、只扫 `*/src/main/java`；跨模块 parity 用 golden-value。
- checkstyle 含 test 源、禁 static import、test 阶段不跑 → 单独 `mvn -pl <module> checkstyle:check`。
- ⚠️ PROGRESS/HANDOFF 仍写「P3 PR 已开（CI 中）」= 已 merge（`5c240dc7a34`），P4 kickoff 时一并校正。
