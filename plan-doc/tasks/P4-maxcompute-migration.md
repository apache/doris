# P4 — maxcompute 迁移（首个 full adopter + 翻闸）

> 设计 + 批次计划（**待用户批准**）。批准后按批次独立落地、独立 commit。
> 维护规则见 [README §4](../README.md)；协作规范见 [AGENT-PLAYBOOK.md](../AGENT-PLAYBOOK.md)。
> 事实底座：[research/p4-maxcompute-migration-recon.md](../research/p4-maxcompute-migration-recon.md)（2026-06-06，注：recon §1/§3 计数 **早于 W-phase**，本文已据当前代码 re-grep 校正）。

---

## 元信息

- **状态**：🚧 进行中（**设计已批准 [D-023]**；**Batch A+B+C 全完成**（T01–T04 dormant + T05 image-compat + T06a 写接线/UT + **T06b flip ✅** `SPI_READY_TYPES += "max_compute"`，gate 全绿 [D-027]），下一 = **Batch D**（清反向引用 + 删 legacy 子系统 + **drop fe-core odps 依赖**，**待用户 live ODPS 验证后做**）；R-004 part-2 live 待用户跑（翻闸完成门））
- **启动日期**：2026-06-06（设计批准）
- **目标完成**：分批，每批一 session（估 5 批 / 11 task）
- **阻塞（前置）**：W-phase（W1–W7）✅ 已完成 —— 共享写接线 seam（W4 事务桥 + W5 opaque-sink）就位
- **阻塞下游**：P5 paimon（复用写 SPI）/ P6 iceberg / P7 hive 的 full-adopter 模式以本阶段为样板
- **主 owner**：@me

---

## 阶段目标

把 `max_compute` 连接器从 fe-core legacy（`datasource/maxcompute/`）完整迁移到插件 SPI，并**翻闸**（`SPI_READY_TYPES += "max_compute"`），删除 legacy。这是**首个 full 迁移 + cutover**（vs P2 trino 只读 + P3 hudi hybrid-gate-closed）。

**为何是 full（非 P3 式 hybrid）**：scope 在 W-phase 已定（recon §9 fork → 用户选 **C→A**：先建共享写 SPI = W-phase[D-021]，再 full P4）。W-phase 已把写路径 keystone（recon §0/§4 标注的最大风险）解耦，full P4 现可行。

**对齐**：master plan §3.5；写-RFC [§12「P4 maxcompute」](./designs/connector-write-spi-rfc.md)。

---

## 关键事实（本设计 session code-grounded 核读 / re-grep，2026-06-06）

1. **连接器模块** `fe/fe-connector/fe-connector-maxcompute/`（pkg `org.apache.doris.connector.maxcompute`，13 文件）：读/元数据/scan ✅；**写 SPI 全缺**（无 `getWritePlanProvider` / `beginTransaction` / `ConnectorWriteOps` / `ConnectorTransaction`）；**DDL 缺**（仅 `McStructureHelper` 低层 `createTableCreator`/`dropTable`，无 SPI 层 `ConnectorTableOps.createTable`）；**分区 listing 缺**。
2. **legacy** `fe-core/.../datasource/maxcompute/` = 10 文件 / **3004 LOC**（含 `MCTransaction` 262、`MaxComputeMetadataOps` 565、`MaxComputeScanNode` 809、`MaxComputeExternalCatalog/Database/Table`、MetaCache/SchemaCacheValue、fe-core `McStructureHelper` 副本 298）。连接器**已有**读侧等价（metadata/scan-provider/client-factory/structure-helper/type-mapping/predicate-converter）→ legacy 在 cutover **删除**（非搬运）；只有 **DDL + 写/事务 + 分区** 三块功能需先**港入**连接器。
3. **`MCTransaction` 公开面**（待港）：`addCommitData(byte[])`✅(W2 已加) · `supportsWriteBlockAllocation`✅ · `allocateWriteBlockRange`✅ · `beginInsert(ExternalTable, Optional<InsertCommandContext>)` · `getWriteSessionId` · `finishInsert` · `commit` · `rollback` · `getUpdateCnt` · `updateMCCommitData(List<TMCCommitData>)`（legacy typed）。
4. **`TMaxComputeTableSink`**（`gensrc/thrift/DataSinks.thrift:586`，18 字段）已定义：`session_id`/`write_session_id`(15)/`block_id_start`(8)/`block_id_count`(9)/`static_partition_spec`(10)/`partition_columns`(14)/`txn_id`(18)/`properties`(16) —— W5 留的 write-context seam 字段齐备。
5. **反向引用 re-grep（post-W-phase）= ~19 站点**（recon §3 旧称 ~36，差额=W-phase 灭 3 热点 txn 站 + recon 多算注册站；**穷举留 Batch D 入口门**）：
   - **W-phase 已灭**（grep 证）：`Coordinator` / `LoadProcessor` / `FrontendServiceImpl` **零** `MCTransaction`。
   - **live（少数，建 MC 专有对象）**：`PhysicalPlanTranslator:795`(建 MaxComputeScanNode) · `ShowPartitionsCommand:415` · `CreateTableInfo:912` · `BindSink:1084` · `PartitionsTableValuedFunction:200`(getOdpsTable().getPartitions) · `MetadataGenerator:1310` · `MCInsertExecutor:64/75`(cast MCTransaction)。
   - **mechanical（折进 PluginDriven/SPI 分支）**：`CatalogFactory:146` · `ExternalCatalog:938`(db) · `ExternalMetaCacheRouteResolver:75` · `ShowPartitionsCommand:203` · `InsertOverwriteTableCommand:320` · `CreateTableInfo:390` · `UnboundTableSinkCreator:66/105/146` · `PartitionsTableValuedFunction:173` · `PartitionValuesTableValuedFunction:115` + recon §3 注册站（GsonUtils×3 / ExternalMetaCacheMgr:183/310 / TableIf enum / InitCatalogLog:41 / DatasourcePrintableMap / BindRelation:540 / Alter:617）。

---

## 验收标准

- [ ] MC **读**路径翻闸后经 SPI（`PluginDrivenScanNode`）行为不变（golden / 手测）。
- [ ] MC **写**（INSERT / INSERT OVERWRITE）翻闸后经 W4 事务桥 + W5 opaque-sink；commit 载荷 `TBinaryProtocol` 等价（`CommitDataSerializer` 红线）；block-id 分配正确。
- [ ] MC **DDL**（CREATE/DROP TABLE+DB）翻闸后经 SPI `ConnectorTableOps`。
- [ ] **SHOW PARTITIONS** / `partitions` TVF / `partition_values` TVF 翻闸后经 SPI `listPartitions*`。
- [x] `max_compute` 进 `SPI_READY_TYPES`；`CatalogFactory` case 删；**GSON image 兼容**（旧 image 可加载，registerCompatibleSubtype）。**（T06b 翻闸 ✅）**
- [ ] fe-core **零** `instanceof MaxComputeExternal*`、**零** `MCTransaction`（grep 空）。
- [ ] `datasource/maxcompute/` 整目录删；`McStructureHelper` fe-core 副本删（**收口 P1-T02**）。
- [ ] 连接器单测绿（JUnit5 手写替身，无 mockito）；checkstyle 0；import-gate 绿。
- [ ] **R-004**：ODPS SDK 在插件 classloader 下连通（翻闸前防御测）。

---

## 任务清单

> ID 永不复用。状态：⏳ pending / 🚧 / ✅ / ❌ / 🚫deleted。**逐批独立 commit**。

| ID | 任务 | 批次 | 状态 | 备注 |
|---|---|---|---|---|
| P4-T01 | 连接器 **DDL**：impl `ConnectorTableOps` create/drop table+db（港 `MaxComputeMetadataOps` create/drop/truncate `Impl`，**消费 P0 `ConnectorCreateTableRequest`** 而非 fe-core `CreateTableInfo`）| **A** gate 关 | ✅ | `MaxComputeConnectorMetadata` impl createTable/dropTable/createDatabase/dropDatabase + `MCTypeMapping.toMcType` 反向类型映射；连接器 `McStructureHelper` 原语已具备。**含修 fe-core 转换器 CHAR/VARCHAR 长度 [DV-010]**。守门全绿（compile + checkstyle 0 + import-gate + `ConnectorColumnConverterTest` 9/0F0E）|
| P4-T02 | 连接器 **分区**：impl `listPartitions/listPartitionNames/listPartitionValues`（港 ODPS `getPartitions`，直取无自有 cache）| **A** gate 关 | ✅ | `MaxComputeConnectorMetadata` impl 三方法：names→`PartitionSpec.toString(false,true)`（镜像 legacy catalog:283/table:201）；`listPartitions` filter 忽略返全量（values 由 `keys()`/`get(k)`，props=emptyMap）；`listPartitionValues` 按入参列序 `spec.get(col)`。**OQ-4 定：不建自有 cache，直取 ODPS**。守门全绿（compile + checkstyle 0 + import-gate）|
| P4-T03 | 连接器 **写/事务 SPI**：`ConnectorWriteOps.beginTransaction` + `ConnectorTransaction`（港 `MCTransaction`：`addCommitData` 反序列化 `TMCCommitData`、block 分配、commit/rollback、getUpdateCnt）| **B** gate 关 | ✅ | 新建 `MaxComputeConnectorTransaction` + `beginTransaction`，over W4 委派；txn id 经新增 `ConnectorSession.allocateTransactionId()`（[D-024] fork1）；写 session 创建挪 T04（[D-024] fork2）；block 上限常量化 + 异常 `DorisConnectorException`（[DV-011]）；`TBinaryProtocol` 红线守。守门全绿（fe-connector-maxcompute+api+fe-core compile + checkstyle 0 + import-gate 0）。设计 [P4-T03 doc](./designs/P4-T03-write-txn-design.md)|
| P4-T04 | 连接器 **写计划**：`Connector.getWritePlanProvider` → `planWrite` 产 `TMaxComputeTableSink`（填 W5 write-context seam：txn_id/write_session_id/static_partition_spec；港 legacy `MaxComputeTableSink` config-read）| **B** gate 关 | ✅ | 新建 `MaxComputeWritePlanProvider.planWrite`（**OQ-2 = Approach A**：finalizeSink 一处建 ODPS 写 session + `setWriteSession` 绑 txn + 盖 `txn_id`/`write_session_id`，无运行期注入）；`MaxComputeDorisConnector.getSettings()`（D-3 抽出，scan/write 共用，镜像 legacy 单 settings）+ `getWritePlanProvider()`；`supportsInsert()`=true（D-4，beginInsert/finishInsert 留 throwing-default 待 Batch C）；**fe-core seam（D-2a）**：`PluginDrivenTableSink.bindViaWritePlanProvider(insertCtx)` 读 overwrite+静态分区填 handle + `PluginDrivenInsertCommandContext.staticPartitionSpec`（非基类，避 `MCInsertCommandContext` shadow）。`block_id` 不盖（运行期 T03）；`partition_columns` 取 ODPS 表列（**DV-012**）。**5 决策签字 [D-025]**。守门全绿（compile BUILD SUCCESS + checkstyle 0 + import-gate 0，真实 EXIT）。单测延 P4-T10 |
| P4-T05 | **翻闸接线**：GsonUtils `registerCompatibleSubtype`（catalog :397 / **db :452** / table :472 → PluginDriven）+ `PluginDrivenExternalTable.getEngine`/`getEngineTableTypeName` 加 `case "max_compute"` + `legacyLogTypeToCatalogType`（MAX_COMPUTE→lowercase，无连字符特例）| **C** | ✅ | **实现 gate-green（待 commit）**：三 GSON 注册齐迁 compat（**db :452 折入**——漏迁则翻闸后 `MaxComputeExternalDatabase.buildTableInternal:44` cast 抛 ClassCastException）+ 删 3 unused import + 引擎名 case（getEngine=null / getEngineTableTypeName=MAX_COMPUTE_EXTERNAL_TABLE，镜像 legacy）+ `legacyLogTypeToCatalogType` 注释（默认分支已出 "max_compute"，不加 case）。UT `PluginDrivenExternalTableEngineTest` +2 max_compute 例 9/9。gate：compile/checkstyle 0/import-gate 0（真实 EXIT）。4-agent 复核 2 告警判非问题（getMetaCacheEngine 假阳 / getMysqlType 同 ES）。保留 `TableIf.MAX_COMPUTE_EXTERNAL_TABLE`/`InitCatalogLog.MAX_COMPUTE` 作 image 兼容。[D-026 §3.4] |
| P4-T06 | **翻闸**：`CatalogFactory.SPI_READY_TYPES += "max_compute"` + 删 `CatalogFactory` case（:146）+ **插件 harness ODPS 连通性防御测（R-004）** | **C** **live cutover** | ✅ | **T06a 写接线 W-a..d+静态分区/overwrite 绑定（G1–G5）+R-004 隔离测+UT** 已 commit；**T06b flip 落地**（SPI_READY_TYPES += "max_compute" + 删 case + import + 注释；gate 全绿 [D-027]）。2 SPI 新增登记 §20 E11。**R-004 part-2 live 用户跑、过方算翻闸完成** |
| P4-T07 | 清 **mechanical** 反向引用（折进既有 PluginDriven/SPI 分支）| **D** | ⏳ | **闭包已 verify**（[Batch D 移除设计](./designs/P4-batchD-maxcompute-removal-design.md)，84 ref / OQ-3 穷举 re-grep 满足）；执行**待 live 验证后** |
| P4-T08 | 清 **live** 反向引用（`PhysicalPlanTranslator:795` / `ShowPartitionsCommand:415` / `CreateTableInfo:912` / `BindSink:1084` / `PartitionsTableValuedFunction:200` / `MetadataGenerator:1310`）+ **验 `MCInsertExecutor` 成死代码** | **D** | ⏳ | OQ-1 已 verify（仅 dead `instanceof` 门建，grep-empty 步确认）；执行待 live 验证 |
| P4-T09 | **删 legacy**（21 文件）：`datasource/maxcompute/`（10）+ 写/txn plumbing（`MaxComputeTableSink`/`Logical`/`PhysicalMaxComputeTableSink`/`UnboundMaxComputeTableSink`/`MCInsertExecutor`/`MCInsertCommandContext`/`LogicalMaxComputeTableSinkToPhysical…Rule`/`MCTransactionManager`）+ 2 legacy 测 + **drop fe-core odps 依赖**（pom 两 `odps-sdk-*` 块）| **D** | ⏳ | 收口 P1-T02；闭包见 Batch D 设计；执行待 live 验证 |
| P4-T10 | **连接器测试基线**（仿 hudi 5 文件，JUnit5 手写替身）：metadata/schema · scan-plan · predicate · **write-txn(commit golden, TBinaryProtocol)** · DDL | **E** | ⏳ | checkstyle 含 test 源、禁 static import |
| P4-T11 | **文档同步 + 开 PR**（5 步 doc-sync；含**修 PROGRESS stale「P3 PR CI中」→ 已合 `5c240dc7a34` #64143**、校正 recon §10）| **E** | ⏳ | PR title `[P4-Txx]`；本阶段 D-NNN 入 decisions-log |

---

## 批次依赖 / 翻闸前置门

```
A(DDL+分区, gate 关) ─┐
                      ├─→ C(翻闸, live) ─→ D(清引用+删legacy) ─→ E(测+PR)
B(写/事务, gate 关) ──┘
```

- **A、B 可并行**（均 gate 关、dormant、互不依赖）；**两者全绿 + R-004 防御测过**才允许进 C（翻闸）。
- **C 是唯一 live 切点**：翻闸瞬间 catalog→`PluginDrivenExternalCatalog`、table→`PluginDrivenExternalTable`，读/写/DDL/分区/show 全切 SPI。故 A+B 必须先达功能 parity，否则翻闸即断。
- **D 在翻闸后**：此时 live 反向引用已失配（instanceof 不再命中），SPI 分支接管；清理 + 删 legacy 是收尾。
- 每批独立 commit；守门循环：compile（慢，后台）+ checkstyle（绝对 `-f`）+ import-gate，**读真实 BUILD/MVN_EXIT/CS_EXIT 行**（坑 3）。

---

## 风险 / 开放问题

- **R-004（ODPS SDK classloader 隔离）**：recon §8 裁定「无明显陷阱」但建议翻闸前在插件 harness 做防御性连通测 → 编入 **P4-T06 入口门**。
- **OQ-1（MCInsertExecutor 旁路）**：翻闸后 `InsertIntoTableCommand:563`/`InsertOverwriteTableCommand:320` 的 plugin-driven 路由是否完全不再经 `MCInsertExecutor`（→ MCInsertExecutor:64/75 cast 成死代码）？**Batch B 验证**。
- ~~**OQ-2（write-context 填充）**~~ **✅ 已解并实现（P4-T04）**：**Approach A** — `planWrite` 在 finalizeSink 一处建 ODPS 写 session + 绑事务 + 盖 `txn_id`/`write_session_id`，无运行期注入 hook（legacy `MCInsertExecutor.beforeExec` 注入消失）。fe-core seam（D-2a）填 `PluginDrivenTableSink.bindViaWritePlanProvider(insertCtx)` 读 overwrite+静态分区。**binding 期填充（设 overwrite/静态分区进 `PluginDrivenInsertCommandContext`）仍 dormant，归 Batch C/D**（坑3）；翻闸前 INSERT OVERWRITE PARTITION 静态分区不可用 = 设计意图（dormant）。
- **OQ-3（反向引用穷举）**：本 session re-grep 得 ~19（含全部 live），但 category-C 注册站点（gson/enum/metacache 等）未穷举 → **P4-T07 入口先完整 re-grep**。
- **OQ-4（连接器缓存层）**：✅ **已定（P4-T02）**：**不建**连接器自有 cache，分区直取 ODPS（镜像 legacy catalog `getPartitions` 直取路径；fe-core SPI meta-cache 覆盖 schema；Rule 2 不投机）。perf 回归再议。

---

## 阶段日志（倒序）

### 2026-06-07
- **P4-T06b 翻闸落地（Batch C 完成）+ Batch D 移除范围 recon/设计（[D-027]，2 决策用户签字）**：用户「开始下一步（T06b）+ 追加 fe-core 去 maxcompute jar 依赖」。**翻闸**：`CatalogFactory` `SPI_READY_TYPES += "max_compute"`(:52) + 删 `case "max_compute"`(原 :146-149) + 删 unused `MaxComputeExternalCatalog` import + 注释去 max_compute。gate 全绿（compile BUILD SUCCESS/MVN_EXIT=0 + checkstyle 0/CS_EXIT=0 + import-gate 0，真实 EXIT）。**recon（并行 re-grep + 对抗验证，OQ-3 入口门满足）**：去 fe-core odps 依赖 = 删整套 legacy（**21 文件**：`datasource/maxcompute/` 10 + 写/txn plumbing 8 + 2 测）+ 清 **~30 文件 / 84 ref**（32 import + 43 dead branch）+ keep 集（image/plan/thrift compat）+ pom drop 两 `odps-sdk-*` 块；`feCoreOdpsResidualAfterDeletion`=∅；fe-core 仍 transitive 见 odps-sdk-core（fe-common 留）。镜像 trino `524097e38d3`+`c4ac2c5911d`。**2 决策**：(D-1) flip 先行、移除 + pom drop **待用户 live ODPS 验证后**做（保 flip 独立可回退）；(D-2) fe-core 仅删直接 odps 声明（transitive-via-fe-common 留，用户选 Direct-only）。**2 SPI 新增登记 §20 E11**（D-026 预授）。Batch D turnkey 闭包 → [designs/P4-batchD-maxcompute-removal-design.md](./designs/P4-batchD-maxcompute-removal-design.md)。**下一 = 用户跑 `OdpsLiveConnectivityTest`（4 个 `MC_*` 环境变量）+ 手测 smoke → 绿后执行 Batch D**。

### 2026-06-06
- **Batch C 翻闸设计完成 + 用户签字 [D-026]（design-only，零代码）**：用户选 "Design Batch C first"。4 路 Explore re-verify recon 锚点 + 主线核读 executor/txn 生命周期 → 出 [P4-T05/T06 翻闸设计](./designs/P4-T05-T06-cutover-design.md)（verified file:line + 5 gap G1–G5 + 写生命周期顺序 + R-004 两分测 + ordered TODO）。**recon 校正**：GsonUtils 真锚 `:397`/`:472`（非 ~405/~478）；`legacyLogTypeToCatalogType` 默认分支已出 `"max_compute"`（无需加 case）；live executor=`PluginDrivenInsertExecutor`（现走 JDBC insert-handle 模型，对 MC `getWriteConfig`/`beginInsert`/`finishInsert` 全 throwing-default=直跑必抛）；`PluginDrivenTransactionManager.begin(connectorTx):71-77` 未 `putTxnById`（G3）；`UnboundConnectorTableSink` 不携静态分区（G4）；legacy `MCInsertExecutor` 证 `transactionType()=MAXCOMPUTE`。**3 决策签字**：D-1 capability signal=新增 `ConnectorWriteOps.usesConnectorTransaction()` flag（MC=true，否决 writePlanProvider 代理/复用 ConnectorWriteType）；D-2 两 commit、flip 末（`[P4-T06a]` 接线 dormant + `[P4-T06b]` flip）；D-3 静态分区/overwrite 绑定入 cutover（避翻闸回归）。**2 SPI 新增**（default-preserving）：`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction`（impl 时 E11）。**下一 = 实现 T05（dormant）→ T06（live, 两 commit）**。
- **P4-T04 写计划实现完成（Batch B 收尾，gate 关、dormant、零 live 风险）= Batch A+B 全完成**：新建 `MaxComputeWritePlanProvider implements ConnectorWritePlanProvider`，`planWrite` 走 **OQ-2 = Approach A**（finalizeSink 一处：建 ODPS Storage API 写 session→`writeSession.getId()` → `session.getCurrentTransaction()`→`MaxComputeConnectorTransaction.setWriteSession(wsid, tableId, settings)` 绑事务 → 盖 `TMaxComputeTableSink` 静态字段 + `static_partition_spec`(原样 map) + `partition_columns`(ODPS 表列) + `write_session_id` + `txn_id`(=`tx.getTransactionId()`)；**无运行期注入 hook**，legacy `MCInsertExecutor.beforeExec` dance 消失）。**5 决策主线定/签字 [D-025]**：D-1 Approach A；D-2a 含 fe-core seam fill；**D-3 抽 `MaxComputeDorisConnector.getSettings()`**（关键证据：legacy catalog 单 `settings` 字段同供 scan+write，故抽出是忠实港非投机重构；scan provider :146-162 构造上移、共用）；**D-4 `supportsInsert()`=true** 余最小化（`beginInsert`/`finishInsert`/`getWriteConfig` 留 throwing-default，MC sink 经 planWrite、commit 经 `ConnectorTransaction.commit()`，实际 executor 调用面待 Batch C）；D-5 静态分区作 `getWriteContext()` col→val map。**fe-core seam（D-2a）**：`PluginDrivenTableSink.bindViaWritePlanProvider` 改收 `Optional<InsertCommandContext>`、读 `isOverwrite()`+`getStaticPartitionSpec()` 填 handle；`staticPartitionSpec` 加在 **`PluginDrivenInsertCommandContext`（非基类）**——因 `MCInsertCommandContext` 已自带 `staticPartitionSpec`+getter 且 shadow 基类 `overwrite`，加基类会成 override/shadow 缠结；plugin-driven seam 只见 `PluginDrivenInsertCommandContext`，post-migration hive/iceberg 复用同类（仍满足复用）。binding 期填充（设 overwrite/静态分区）仍 dormant，归 Batch C/D（坑3，已核 `InsertIntoTableCommand:598` 传空 ctx）。**写前 javap 核**（坑10）：`TableWriteSessionBuilder.withMaxFieldSize(long)`/`.partition(PartitionSpec)`/`.overwrite(boolean)`/`.withDynamicPartitionOptions`/`.buildBatchWriteSession()` throws IOException、`DynamicPartitionOptions.createDefault()`、`PartitionSpec(String)`、`getId()`(via `Session`) 全确认；写路径 ArrowOptions = **MILLI/MILLI**（≠ scan MILLI/MICRO）。**偏差 [DV-012]**：`partition_columns` 取 `odpsTable.getSchema().getPartitionColumns()`（ODPS 列）vs legacy `targetTable.getPartitionColumns()`（fe-core Column）——源不同值同。守门全绿（`-pl :fe-connector-maxcompute,:fe-core -am` compile BUILD SUCCESS/MVN_EXIT=0、checkstyle 0、import-gate 0，真实 EXIT 核验）。单测延 **P4-T10**（planWrite golden）。**T04 不新增 SPI 面**（W1 全建）。**下一步 = Batch C 翻闸**（唯一 live 切点，前置 A+B 全绿 ✅ + R-004 防御测）。
- **P4-T04 写计划设计定稿（用户签字，零代码）**：4 路 subagent recon（SPI 写面 / W5 接线 / legacy 写逻辑+executor 生命周期 / thrift+连接器脚手架）+ 主线核读 `PluginDrivenTableSink` → **解 OQ-2**。**executor 序** = `beginTransaction`(txn_id 译前生)→translate→`finalizeSink`/`bindDataSink(insertCtx)`→`beforeExec`→coordinator ⇒ `planWrite` 跑在 finalizeSink、txn_id 已在 + 写 session 可就地建 → **Approach A：planWrite 一处建 session+`getCurrentTransaction().setWriteSession`+盖 `txn_id`/`write_session_id`，无运行期注入 hook**。**5 决策签字**：D-1 Approach A；**D-2 含 fe-core seam fill**（`PluginDrivenTableSink.bindViaWritePlanProvider` 收 insertCtx 填 handle overwrite+静态分区；`PluginDrivenInsertCommandContext`/基类 +`staticPartitionSpec` map）；D-3 抽 `connector.getSettings()`；D-4 `supportsInsert`=true+最小 no-op；D-5 静态分区编码进 `getWriteContext()`。`block_id` 不在 planWrite（运行期 T03）；`partition_columns` 取 ODPS table 列（DV-012 待登）。设计 [P4-T04 doc](./designs/P4-T04-write-plan-design.md)。**实现挪下一 fresh session**（split-session 节奏，用户签字）。**T04 不新增 SPI 面**（W1 已全建）。
- **P4-T03 连接器写/事务 SPI 完成**（Batch B 启，gate 关、dormant、零 live 风险）：新建 `MaxComputeConnectorTransaction implements ConnectorTransaction`（港 legacy `MCTransaction` 写生命周期：`addCommitData` `TDeserializer(TBinaryProtocol)`→`TMCCommitData` 累积【commit 协议红线】、block 分配 CAS+上限校验、`commit` 港 `finishInsert`(restore session + `session.commit`)、rollback/close/getUpdateCnt）+ `MaxComputeConnectorMetadata.beginTransaction`，over W4 委派。**两 fork 用户签字 [D-024]**：(1) txn id 经新增 SPI `ConnectorSession.allocateTransactionId()`（fe-core `ConnectorSessionImpl` override `Env.getNextId`）分配——尊重 [D-015]，补 id-less 连接器机制（E11 登记）；(2) ODPS 写 session 创建挪 T04 planWrite（T03 纯事务容器，`writeSessionId`/`tableIdentifier`/`settings` 槽由 T04 填）。**偏差 [DV-011]**：block 上限 fe-core `Config`(20000)→连接器常量、`UserException`→`DorisConnectorException`（import-gate 禁 `common.*`）。**JDBC 仅半样板**（无 `ConnectorTransaction`），MC 首个有状态事务 adopter。守门全绿（fe-connector-maxcompute+fe-connector-api+fe-core compile BUILD SUCCESS/MVN_EXIT=0 + checkstyle 0 + import-gate 0，真实 EXIT 核验）。**单测延至 P4-T10**（write-txn golden、TBinaryProtocol round-trip）。**下一步 = P4-T04 写计划**（planWrite 产 `TMaxComputeTableSink` + OQ-2 write-context）。
- **P4-T02 连接器分区 listing 完成**（Batch A 收尾，gate 关、dormant、零 live 风险）：`MaxComputeConnectorMetadata` impl SPI `listPartitionNames`/`listPartitions`/`listPartitionValues`，三方法均直取 `structureHelper.getPartitions(odps, db, tbl)`：names = `PartitionSpec.toString(false, true)`（镜像 legacy `MaxComputeExternalCatalog:283`/`MaxComputeExternalTable:201`）；`listPartitions` filter **忽略**返全量、values 由 `PartitionSpec.keys()`/`get(k)` 抽、props=emptyMap（镜像 legacy SHOW PARTITIONS 不裁剪）；`listPartitionValues` 按入参 `partitionColumns` 列序取 `spec.get(col)`。**OQ-4 定**：不建连接器自有 cache，直取 ODPS（Rule 2 不投机）。**保真说明**：legacy 双路径分歧（catalog:266 无 emptiness guard / table:200 有 `!partitionColumns.isEmpty()` guard），SPI 锚 catalog SHOW PARTITIONS 路径故**不加** guard。写前验过 ODPS `PartitionSpec` 真实 API（`Set<String> keys()`/`String get(String)`/`toString(boolean,boolean)`，odps-sdk-commons 0.45.2-public）。守门全绿（连接器 compile BUILD SUCCESS/MVN_EXIT=0 + checkstyle 0/CS_EXIT=0 + import-gate 0，真实 EXIT 核验）。**测试**：按计划延至 P4-T10 连接器测试基线（无 mockito 手写替身），T02 gate=compile+checkstyle+import（R12 不静默）。
- **P4-T01 连接器 DDL 完成**（Batch A，gate 关、dormant、零 live 风险）：`MaxComputeConnectorMetadata` impl SPI `createTable(ConnectorCreateTableRequest)` / `dropTable` / `createDatabase` / `dropDatabase`（忠实港 legacy `MaxComputeMetadataOps` 的 create/drop/validate/schema-build/lifecycle/bucket 逻辑，**消费 P0 request 而非 fe-core `CreateTableInfo`**）；新增 `MCTypeMapping.toMcType(ConnectorType)` 反向类型映射（按 `PrimitiveType.toString()` 名 switch，递归 ARRAY/MAP/STRUCT，不支持类型抛 `DorisConnectorException`）。连接器 `McStructureHelper` 已含全部 ODPS 原语（`createTableCreator`/`dropTable`/`createDb`/`dropDb`），无需新建。**附带修 fe-core 共享转换器 CHAR/VARCHAR 长度丢失 [DV-010]**（用户 AskUserQuestion 签字）+ 回归测 `testCharVarcharLengthPreserved`。**保真说明**：legacy 的拒 auto-inc/aggregated 列校验无法表达（`ConnectorColumn` 无该标志，nereids 上游已拒），已丢弃。守门全绿（连接器 compile + checkstyle 0 + import-gate + fe-core `ConnectorColumnConverterTest` 9/0F0E，真实 EXIT 核验）。**坑**：守门 maven `-pl` 须用 `:fe-connector-maxcompute`（冒号=artifactId）；裸名 `fe-connector-maxcompute` 被当相对路径解析 → reactor not found。
- **设计已批准**（[D-023]）：用户批准 5 批 / 11 task 计划。同步跟踪文档（PROGRESS §一/§三/§四/§六/§七、decisions-log D-023、connectors/maxcompute、HANDOFF），修 PROGRESS §三 stale「P3 PR CI中」→ 已合 `5c240dc7a34`。**下一 session = Batch A**（P4-T01 DDL + P4-T02 分区，gate 关）。未动代码。
- **设计 session**：读 HANDOFF/PROGRESS/AGENT-PLAYBOOK + maxcompute recon + 写-RFC §12；re-grep 反向引用（post-W-phase ~19，证 W-phase 灭 3 热点 txn 站）；核 `MCTransaction` 面 / `TMaxComputeTableSink` / 连接器 SPI 缺口 / legacy LOC。产出本 P4 设计 + 5 批 11 task 计划。

---

## 关联

- Master plan：[§3.5](../00-connector-migration-master-plan.md)
- 写-RFC：[§12 P4 maxcompute](./designs/connector-write-spi-rfc.md)
- recon：[p4-maxcompute-migration-recon.md](../research/p4-maxcompute-migration-recon.md)（§1 连接器现状 / §3 反向引用 / §5 翻闸点 / §9 scope fork）
- 决策：D-021（scope=C 写 SPI 先行）/ D-022（写 SPI A/B1/C1/D/E）→ **本阶段批准时补 D-NNN「P4 = full adopter / option A」**
- 偏差：DV-009（W5 opaque-sink 实做 vs 旧措辞）；P1-T02（McStructureHelper 去重 deferred → 本阶段 P4-T09 收口）
- 风险：R-004（ODPS classloader）
- 连接器：[maxcompute](../connectors/maxcompute.md)

---

## 当前阻塞项

- **Batch D 执行前置门**（[D-027]）：用户跑 `OdpsLiveConnectivityTest`（live ODPS 连通，设 4 个 `MC_*` 环境变量）+ 手测 smoke（SELECT / CREATE·DROP TABLE+DB / SHOW PARTITIONS / partitions·partition_values TVF / INSERT / INSERT OVERWRITE [PARTITION]）绿 = 翻闸完成门。**绿后**执行 Batch D（清 ~30 反向引用 + 删 21 legacy 文件 + drop fe-core odps 依赖），闭包见 [Batch D 移除设计](./designs/P4-batchD-maxcompute-removal-design.md)。
