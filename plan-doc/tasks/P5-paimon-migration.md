# P5 — paimon 迁移（full adopter + 翻闸；复用 P4 写/事务 + cutover 样板）

> 设计 doc。事实底座见 `research/p5-paimon-migration-recon.md`（14-agent code-grounded recon + cross-cut 对抗复审）。
> 本 doc 含：old→new 映射、批次计划、有序 TODO、**开放决策（待用户签字）**。维护规则见 [README §4](../README.md)。

---

## 元信息

- **状态**：🟢 进行中（**B0–B7 全完成并合入 `branch-catalog-spi`** —— 测基建/flavor/normal-read/DDL/sys-tables+MVCC/MTMV桥/时间旅行/**翻闸** + P6 全路径 clean-room review 的全部 deviation fix，全部 squash 进 **PR #64446 / `38e7140ce56`**（随后 `e9c5b3e70ce` 修编译）。paimon 现已在 `SPI_READY_TYPES`，FE 走 SPI 路径。**仅剩 B8 = P5-T29 删 legacy（+ B9 = P5-T30 回归）**。下一批 = **P5-T29**，见下文 §P5-T29 执行计划 + §当前阻塞项。B0–B7 见任务表与阶段日志）
- **启动日期**：2026-06-09（recon+设计）
- **目标完成**：B8/B9 后即收官（P5 阶段最后一块主体工作）
- **阻塞**：无（B7 翻闸已合入 #64446；P5-T29 无硬阻塞，但 D 项 maven scope 须先与用户对齐方案 A/B，见 §P5-T29 执行计划）
- **阻塞下游**：P5 是首个 lakehouse full-adopter 样板（E5/E6/E7/E10 已首次落地并合入）；其 SPI 新面（E7 sys-table hook、E10 MTMV 桥、E5 wiring）将被未来 iceberg/hudi 翻闸复用
- **主 owner**：@morningman / TBD

---

## 阶段目标

把 fe-core `datasource/paimon/`（28 文件）+ `metacache/paimon/`(3) + `property/metastore/*Paimon*`(7) + 反向引用迁入 `fe-connector-paimon`，按 maxcompute full-adopter 样板翻闸（paimon 进 `SPI_READY_TYPES`）并删 legacy。覆盖用户指定 5 功能区：

1. **普通表读取** — 补完已有 scan 骨架 + 翻闸（最接近 MC 样板）。
2. **系统表读取** — 新建 E7 sys-table SPI hook + 通用 `PluginDrivenSysExternalTable`（greenfield，首个消费者）。
3. **procedure** — **零可迁，doc-only no-op**（fe-core 无 paimon procedure；现即拒）。
4. **DDL** — 迁 `PaimonMetadataOps` + 6 flavor 装配 + 翻闸编辑点。
5. **mtmv** — fe-core 新建 `PaimonPluginDrivenExternalTable` 桥（E10 无 SPI 面 + 首个 E5 消费者）。

Master plan [§3.6](../00-connector-migration-master-plan.md)；策略 = full adopter + 翻闸（复用 P4 写/事务 SPI + cutover 流程）。

---

## 关键事实（本设计 session code-grounded 核读，2026-06-09）

- paimon **不在** `SPI_READY_TYPES`（`CatalogFactory.java:52` = {jdbc,es,trino-connector,max_compute}），仍走 built-in case（`:142`）。firsthand 核实。
- GSON **7 处** paimon 注册（5 catalog `GsonUtils.java:390-396` + db `:450` + table `:471`，全 `registerSubtype`）。firsthand 核实。
- `PluginDrivenExternalTable`（`:62`）**不** implements MTMV/Mvcc 任何接口；有 `getPartitionColumns:218` + `getNameToPartitionItems:246`。firsthand 核实。
- `ConnectorPartitionInfo.getLastModifiedMillis()`（`:90`，6-arg ctor `:53`）**已存在** → 分区级 MTMV staleness 载体现成。firsthand 核实。
- 5 个 `fe-connector-paimon-backend-*` 模块 = **空壳**（仅 gitignore `.flattened-pom.xml`，零 src）。连接器现走单 Catalog（`PaimonConnector.java:75-83` stub）。
- 连接器 `PaimonConnectorMetadata` 已实现 read 7 方法；DDL/partition/MVCC/sys-table 全落 SPI 默认。**0 测试**。
- procedure 区 fe-core 零 paimon 实现；`expire_snapshots`=iceberg、`CALL paimon.sys.migrate_table`=Spark（两假阳性）。

---

## old → new 映射（按功能区，详见 recon §3）

| 功能区 | fe-core 旧 | 新归宿 | SPI 点 | 动作 |
|---|---|---|---|---|
| 普通读 | `PaimonScanNode`/`PaimonSource`/`PaimonSplit` | `PaimonScanPlanProvider`+`PaimonScanRange`+通用 `PluginDrivenScanNode` | E3 | migrate+删 legacy |
| 普通读 | `source/PaimonPredicateConverter`+`PaimonValueConverter`（重复）| 连接器 `PaimonPredicateConverter`（修 session-TZ）| E3 pushdown | delete-duplicate |
| 普通读 | `PaimonExternalMetaCache`+`metacache/paimon/*`(3) | 连接器内 cache | 无 SPI（连接器内）| new+删 legacy |
| 普通读 | `PaimonScanMetricsReporter`/`PaimonMetricRegistry` | 无（连接器禁 import profile）| 无 | **drop**（MC 无先例，登记 profile 回归）|
| sys-table | `PaimonSysTable`+`PaimonSysExternalTable` | 通用 `PluginDrivenSysExternalTable`(报 PLUGIN_EXTERNAL_TABLE)+连接器 E7 impl | **E7（新）** | migrate+delete-duplicate |
| sys-table | `SysTable`/`SysTableResolver`（通用名解析）| 留 fe-core 通用 + 扩 findSysTable 委托 | 通用 bridge | keep-generic |
| procedure | （无）| （无）| E2 absent | **no-op doc** |
| DDL | `PaimonMetadataOps` | `PaimonConnectorMetadata` DDL 方法（连接器远端 + `PluginDrivenExternalCatalog` override edit-log）| E1+ConnectorSchemaOps | migrate |
| DDL | `AbstractPaimonProperties`+5 flavor+`PaimonPropertiesFactory` | `PaimonConnector.createCatalog` flavor switch（+每-flavor authenticator）| 连接器内 | migrate（见 D1）|
| DDL | `DorisToPaimonTypeVisitor` | `PaimonTypeMapping` 反向（吃 ConnectorType）| E1 | migrate（保留 legacy gap）|
| DDL | 5 catalog+factory+db+table（GSON 壳）| `PluginDrivenExternalCatalog/Database/Table`（+MTMV 子类）| GSON compat | keep-generic+原子齐迁 |
| mtmv | `PaimonExternalTable`(MTMVRelated/Base/Mvcc) | fe-core 新 `PaimonPluginDrivenExternalTable` 桥 | **E10（新）+E5** | new |
| mtmv | `PaimonMvccSnapshot`/`PaimonSnapshot`/`PaimonPartitionInfo` | fe-core MvccSnapshot 包 `ConnectorMvccSnapshot`+分区 map；snapshotId via E5 | E5 | migrate（拆解）|

---

## 验收标准

- [ ] paimon ∈ `SPI_READY_TYPES`；built-in case 删；`CreateTableInfo.pluginCatalogTypeToEngine` 加 `paimon→ENGINE_PAIMON`；GSON 7 注册原子转 compat。
- [ ] 普通表读取 parity（谓词下推行正确性、分区裁剪行数、native ORC/Parquet vs JNI、deletion-vector、SELECT * 无谓词）vs 旧 `PaimonScanNode`，before/after 回归绿。
- [ ] 系统表 `$snapshots/$files/$partitions/$manifests/$schemas/$binlog/$audit_log` SELECT + DESCRIBE 经 SPI 路径正确（binlog/audit_log 强制 JNI 行正确）。
- [ ] DDL：CREATE/DROP TABLE（分区+主键+location）、CREATE/DROP DATABASE（HMS 带 props vs filesystem 拒）、DROP DB FORCE 级联、no-ENGINE CREATE TABLE、重启后 5 flavor GSON tag edit-log replay 绿。
- [ ] **MTMV**（D2 取「实现」时）：单分区变更只刷该分区（timestamp staleness）+ 全表 snapshotId 变更刷全表；单-pin 不变式测（读路径与 MTMV 各方法观同一 snapshotId+分区集）。**OR**（D2 取「fail-loud 延后」时）：MTMV-base/时间旅行命中 SPI paimon 表显式报错，**禁静默读 latest**。
- [ ] procedure：`CALL paimon.x` / `ALTER ... EXECUTE` 翻闸后仍报错（no-op 守护）；doc 钉死两假阳性。 — **doc 部分 ✅ B6/T26**（两假阳性已钉、seam 已记、0 可迁 firsthand 核实）；「翻闸后仍报错」= B7 live-e2e 验。
- [ ] session-TZ 时间戳谓词非 UTC session 不丢行（修 `PaimonPredicateConverter:284`）。
- [ ] FE→BE serialized-Table round-trip smoke（built jars）；连接器 paimon-core 版本 == be-java-extensions/paimon-scanner + preload-extensions。
- [ ] 连接器 UT（无 mockito/无 fe-core）+ checkstyle 0 + import-gate 净；删 legacy 后 `grep paimon fe-core/src` 仅 GSON compat 壳。
- [ ] live e2e（真实 paimon 各 flavor 环境，用户跑，硬门）。

---

## 任务清单

> ID 永不复用。批次依赖见下节。type：C=code / T=test / D=doc。

| ID | 任务 | 批次 | type | 状态 | 备注 |
|---|---|---|---|---|---|
| P5-T01 | 建 `fe-connector-paimon` 测试模块 + 注入式 SDK seam（`PaimonCatalogOps` 接口包远端 Catalog 调用，MC `McStructureHelper` 范式，no-mockito recording fake）| B0 | C+T | ✅ | seam=5 读方法（B0 只读，DDL 待 B1-B3 扩）；`PaimonConnectorMetadata` 6 调用点齐迁；9 UT 钉 databaseExists try/catch + getColumnHandles reload-fallback + 1 env-gated live smoke |
| P5-T02 | parity baseline（vs 旧 `PaimonScanNode`：谓词/分区/native·JNI/deletion/SELECT*，doc [`research/p5-paimon-parity-baseline.md`](../research/p5-paimon-parity-baseline.md)）+ FE→BE round-trip smoke（offline `PaimonTableSerdeRoundTripTest`，CI 非 env-gated）+ **pin paimon-core 版本三方对齐**（R-007 注释落 `fe/pom.xml` `<paimon.version>`） | B0 | T | ✅ | 翻闸前后跑；gap 见 doc §3 |
| P5-T03 | `PaimonConnector.createCatalog` flavor 装配（switch on `paimon.catalog.type`→paimon `metastore` opt：warehouse/options/重建 Hadoop·HiveConf/**authenticator=`ConnectorContext.executeAuthenticated`**；全 5 flavor）| B1 | C | ✅ | 新 `PaimonCatalogFactory`（纯 buildCatalogOptions/buildHadoopConfiguration/buildHmsHiveConf/buildDlfHiveConf/requireOssStorageForDlf）；线程 ConnectorContext；DriverShim 经 getEnvironment 替 JdbcResource；hms/dlf live-e2e 门见下 |
| P5-T04 | 拷 HMS/REST/DLF/JDBC + credential/storage 属性键入 `PaimonConnectorProperties`（禁 import fe-core）| B1 | C | ✅ | 全 flavor key 常量，多别名 `String[]` |
| P5-T05 | 扩 `PaimonConnectorProvider.validateProperties`（flavor 合法性 + 每-flavor 必需属性，`IllegalArgumentException` fail-fast）| B1 | C | ✅ | → `PaimonCatalogFactory.validate`；rest 同样必需 warehouse（legacy parity，纠偏 recon）|
| P5-T06 | 修 transient-Table **reload fallback**：`PaimonScanPlanProvider` 加 `catalogOps` 注入 + 包私 `resolveTable`（transient null→`catalogOps.getTable(Identifier)` 重建），planScan + getScanNodeProperties 两 site 都护 | B2 | C | ✅ | **BLOCKER**；镜像 `getColumnHandles:160-171` fallback；2 直测 `resolveTable`（FakePaimonTable.newReadBuilder 抛故不能端到端跑 planScan）|
| P5-T07 | `PaimonPredicateConverter` **parity-correct TZ**（NTZ 保 UTC、LTZ 不下推、不可转降级空、保 FLOAT/CHAR 不下推）+ `PaimonConnectorMetadata.supportsCastPredicatePushdown()=false` | B2 | C | ✅ | **D4：不 session-TZ**（纠偏 [[catalog-spi-connector-session-tz-gotcha]] 对 paimon 的误用；legacy 用固定 GMT/UTC）|
| P5-T08 | 实现 `PaimonConnectorMetadata.listPartitionNames/listPartitions/listPartitionValues`（填 `ConnectorPartitionInfo` 含 lastModifiedMillis=`Partition.lastFileCreationTime()`、rowCount/sizeBytes、raw spec partitionValues，partitionName=legacy-name 解析显示名经 paimon `DateTimeUtils.formatDate`）+ 扩 seam `listPartitions(Identifier)`（+ `RecordingPaimonCatalogOps`/`FakePaimonTable.options()` 测扩）| B2 | C | ✅ | **D5：B2 实现连接器 SPI 但不接 FE**（`partition_columns` key 翻 + FE 消费 + MTMV 喂 = B5 前置）；**`getProperties` 不实现**（firsthand：fe-core 零消费方 + MC 不 override + 凭据泄漏风险 → 留 emptyMap stub，纠偏 plan「retain props map」）|
| P5-T09 | **文档化纯谓词裁剪**（不 override 6-arg `planScan`；paimon `withFilter`+SDK 内部裁分区/文件，scan-correct）+ Javadoc note（镜像 MC「intentionally NOT overridden」）| B2 | C | ✅ | **D5：不 override 6-arg**；EXPLAIN partition=N/M 显示损失=已知 cosmetic gap |
| P5-T10 | 连接器内 cache（替 `PaimonExternalMetaCache`）**延后**；REFRESH seam 已核 | B2 | D | ✅ | **D6：B2 延后**（cache+invalidation SPI=`default-no-op invalidateTable`+`onRefreshCache` clear+RefreshManager wiring = B8/翻闸前置；REFRESH CATALOG/TABLE 均不达 connector 已证伪 plan 前提）|
| P5-T11 | `PaimonTypeMapping` 加 Doris→paimon 方向（吃 ConnectorType；保留 legacy gap：无 TINYINT/SMALLINT/LARGEINT/TIME、char→VarChar(MAX)、DATETIME→plain Timestamp）| B3 | C | ✅ | `toPaimonType` switch on `getTypeName()`，byte-parity `DorisToPaimonTypeVisitor.atomic:82-108`（map-key `.copy(false)`、struct id `AtomicInteger(-1)`、gap→`DorisConnectorException`）；nested-nullability SPI 结构性丢失（`ConnectorType` 无 per-child flag，上游已丢，moot） |
| P5-T12 | `PaimonSchemaBuilder`（ConnectorCreateTableRequest→paimon Schema：primary-key/comment/location→CoreOptions.PATH、partitionKeys from IDENTITY spec；bucket 经 options passthrough）| B3 | C | ✅ | port `toPaimonSchema:231-256`；2 故意 safer 偏差：comment `properties["comment"]`优先否则 fallback `request.getComment()`（legacy 只读 prop 丢 COMMENT 子句）、PK drop-blank（legacy 不 drop）；非-identity transform→throw |
| P5-T13 | 实现 `createTable`/`dropTable`（远端 + per-flavor authenticator；保留 latent remote-vs-local 名 bug 不修）| B3 | C | ✅ | override request-overload `createTable` + handle-based `dropTable`（idempotent ignoreIfNotExists=true）；**D7=B：每 DDL call 包 `context.executeAuthenticated`**（读路径不包）；remote-vs-local 名 bug 在 SPI 层 moot（请求单名 from `db.getRemoteName()`）；`PluginDrivenExternalCatalog` 已 override FE 侧 |
| P5-T14 | 实现 `supportsCreateDatabase=true`+`createDatabase`（HMS-only-props gate 读 `session.getCatalogProperties()`）+`dropDatabase(force)` enumerate-loop | B3 | C | ✅ | gate 读注入 `catalogProperties`（= session.getCatalogProperties 同 map，更简）BEFORE auth；`dropDatabase(force)` = enumerate-loop **AND** native cascade（legacy `performDropDb:147-163` belt-and-suspenders，非 MC enumerate-only）；createDatabase ignoreIfExists=false（FE 已 short-circuit）；MC parity `:466/478` |
| P5-T15 | DDL 离线 UT（createDb gate / dropDb force 级联 / createTable schema / IF NOT EXISTS / type gap）| B3 | T | ✅ | 分布 4 新测类（`PaimonTypeMappingToPaimonTest`10 / `PaimonSchemaBuilderTest`10 / `PaimonConnectorMetadataDdlTest`9 / `PaimonConnectorMetadataDbDdlTest`11）+ `RecordingConnectorContext`（failAuth 钉 auth-wrap）+ `RecordingPaimonCatalogOps` DDL 扩；no-mockito，WHY+MUTATION |
| P5-T16 | **新 E7 SPI**：`ConnectorMetadata.listSupportedSysTables`(default emptySet) + `getSysTableHandle`(default empty)；保 MC/jdbc/es/trino 不受影响 | B4 | C | ✅ | greenfield，签名须慎；**D-039**：复用 live `SysTableResolver` 机制（非 RFC §10，[DV-023]）；2 个 `ConnectorTableOps` default no-op，MC/jdbc/es/trino 不受影响 |
| P5-T17 | paimon 实现 E7：名取 `SystemTableLoader.SYSTEM_TABLES`；`getSysTableHandle` 走 4-arg `Identifier(db,tbl,"main",sysName)`；handle 带 sysName+forceJni；reload fallback | B4 | C | ✅ | branch="main" 限制保留+文档；名取 `SystemTableLoader.SYSTEM_TABLES`；复用现有 `getTable(Identifier)` seam 喂 4-arg sys Identifier；sys handle 加 `sysTableName`+`forceJni`（binlog/audit_log）+ lowercase 规范化；共享 `PaimonTableResolver`（metadata+scan 一处 sys-aware reload）|
| P5-T18 | 通用 fe-core `PluginDrivenSysExternalTable extends PluginDrivenExternalTable`(报 PLUGIN_EXTERNAL_TABLE) + `NativeSysTable` factory；override `PluginDrivenExternalTable.getSupportedSysTables/findSysTable` 委托连接器 | B4 | C | ✅ | 路由经 `PluginDrivenScanNode`，**报 PLUGIN_EXTERNAL_TABLE 非 PAIMON**；`PluginDrivenExternalTable` 集中 handle 获取入 `resolveConnectorTableHandle` seam（4 site），sys 子类 override 之喂 sys handle；`getSupportedSysTables` 委托连接器 `listSupportedSysTables`；sys 表 transient 不持久化/不 GSON 注册 |
| P5-T19 | `PaimonScanPlanProvider` 加 forceJni 分支（binlog/audit_log + 非 DataTable sys 全走 JNI）+ 通用节点 fail-loud 拒 sys 表 scan-params/time-travel；核 BE sys-table `TTableDescriptor`(HIVE_TABLE?) | B4 | C | ✅ | binlog/audit_log 走 native = 行错（静默）→ `shouldUseNativeReader(forceJni,...)` gate（ro 仍 native、metadata 表经 non-DataSplit JNI）；**核出 BE 描述符**：加 `buildTableDescriptor`→`HIVE_TABLE`（同修普通表 SCHEMA_TABLE fallback 遗留缺陷 [DV-024]）；`PluginDrivenScanNode` fail-loud 拒 sys 表 scan-params/time-travel；**最终复审 BLOCKER 已修**：`PluginDrivenScanNode.create` 改走 `resolveConnectorTableHandle` seam（原直调 `getTableHandle` 丢 forceJni）|
| P5-T20 | **首个 E5 消费者**：实现 `beginQuerySnapshot/getSnapshotAt/getSnapshotById`(返 `ConnectorMvccSnapshot(snapshotId)`，空表 -1)+声明 `SUPPORTS_MVCC_SNAPSHOT/TIME_TRAVEL`；sys 表不得透出 time-travel | B4 | C | ✅ | **inert until B5**（`PluginDrivenExternalTable` 非 MvccTable、零 fe-core 消费方；翻闸 gated on B5 故安全）；snapshot 经新 seam（`latestSnapshotId`/`snapshotIdAtOrBefore`/`snapshotExists`，SDK 在 `CatalogBackedPaimonCatalogOps`、fake 在 Recording）；sys handle→`Optional.empty`；**SPI 契约 empty-if-none vs legacy throw**（已 doc，B5 消费方 surface 用户错误）；`PaimonConnector.getCapabilities` 声明二 flag |
| P5-T21 | ~~GAP-LISTPART-AT-SNAPSHOT~~ → **D-041 drop**：仅「list at begin-query pin（MTMV=latest）」= 现有 `catalogOps.listPartitions`；不建 at-snapshot SDK seam（legacy 时间旅行用 EMPTY 分区，超 parity）；接受两-SDK-call 微 race（doc 记）| B5a | D | ✅ | D-041；并入 T22 loadSnapshot |
| P5-T22 | **通用** `PluginDrivenMvccExternalTable extends PluginDrivenExternalTable` implements MTMVRelatedTableIf+MTMVBaseTableIf+MvccTable（**源无关 D-042**，NO paimon 类）+ catalog 按 `SUPPORTS_MVCC_SNAPSHOT` capability 选择实例化 + GSON 注册 + 连接器 `getTableSchema` emit `partition_columns`（替 `partition_keys`）；`loadSnapshot`（beginQuerySnapshot latest pin + materialize rendered 分区**一次**→通用 MVCC wrapper）| B5a | C | ✅ | **D-042/D-040**；done（双审 PASS） |
| P5-T23 | 通用类 MTMV 方法（全 SPI-delegate）：**loadSnapshot**（漏项）/getTableSnapshot **两重载**(→MTMVSnapshotIdSnapshot(**真 pinned snapshotId** 非 -1))/getPartitionSnapshot(→MTMVTimestampSnapshot(lastModifiedMillis)，缺抛 AnalysisException)/getAndCopyPartitionItems(**读 pin**)/getPartitionType(**LIST**)/getPartitionColumnNames(**lowercase**)/getPartitionColumns(Optional)(**已 base 继承**)/isPartitionColumnAllowNull(true)/beforeMTMVRefresh(no-op)/getNewestUpdateVersionOrTime(**绕 pin** latest max lastModifiedMillis) | B5a | C | ✅ | recon 纠偏②见 D-040~042 节 |
| P5-T24 | 通用 fe-core MVCC snapshot wrapper（包 `ConnectorMvccSnapshot` + 物化 `nameToPartitionItem`/`nameToLastModifiedMillis`/`listedCount`）；downcast 留 fe-core 内；**剔除 `PaimonPartition`**（$partitions sys 行 decoy 未用）| B5a | C | ✅ | recon 纠偏⑦；holistic cleanup 删 `listedCount`，`isPartitionInvalid` 改两 name-keyed map 比对（exact legacy parity，修 dup-rendered-name 假 UNPARTITIONED） |
| P5-T25 | `isPartitionInvalid` parity = **port legacy per-partition try/catch + size 比对**（base `TablePartitionValues` 转换失败**抛非 skip**，不能复用）→ size 不匹配 UNPARTITIONED；通用 `getNameToPartitionItems` override 从 **rendered partitionName 解析值**（修 RAW epoch-day 丢行，源无关，base 留 MC）；MTMV 单-pin 不变式测 + UT | B5a | C+T | ✅ | recon 纠偏③⑤ |
| P5-T31 | **scan-side snapshot pin（BLOCKER）**：`PaimonTableHandle` 加 snapshotId/scan-options；`PluginDrivenScanNode` 线程 pin 入 planScan；连接器 **两 resolveTable 站点**（`PaimonScanPlanProvider.planScan` + `getScanNodeProperties` JNI serialized_table）`table.copy(opts)`；改 handle 流 grep 全调用方 | B5a | C+T | ✅ | latest 一致读；B5b time-travel 复用（3 pin 站点 done） |
| P5-T32 | **AS-OF time-travel**：通用 loadSnapshot 解析 `TableSnapshot`（TIME→getSnapshotAt(millis，非数字本地-TZ parse)/VERSION+digital→getSnapshotById/VERSION+非数字→tag T33）；连接器 empty→子类译 `UserException`（empty-vs-throw surface） | B5b | C+T | ✅ 连接器(B5b-2a)+fe-core(B5b-3)；inert until B7 | D-040；用 T20 现有 SPI + T31 |
| P5-T33 | **tag time-travel（新 SPI）**：连接器 `getSnapshotByTag`（`tagManager().get`）→ `ConnectorMvccSnapshot` 带 `scan.tag-name` prop；scan 应用 SCAN_TAG_NAME | B5b | C+T | ✅ 连接器(B5b-2a)+fe-core(B5b-3)；inert until B7 | D-040 |
| P5-T34 | **branch time-travel（新 SPI）**：连接器经 Identifier branch 分量 / branch-table load（`branchManager().branchExists` 校验）；scan 读 branch 表 | B5b | C+T | ✅ 连接器(B5b-2c)+fe-core(B5b-3)；inert until B7 | D-040；branch 独立 schema/snapshot；详 HANDOFF |
| P5-T35 | **incremental `@incr`（新 SPI）**：port ~180 行 `validateIncrementalReadParams` + paimon `incremental-between`/`-timestamp`/`-scan-mode` 键**入连接器**；fe-core 仅传 raw doris incr param map；scan 应用 copy opts | B5b | C+T | ✅ 连接器(B5b-2b)+fe-core(B5b-3)；inert until B7 | D-040；与 tableSnapshot 互斥 |
| P5-T26 | **procedure DOC no-op**：连接器档 E2 改「NOTHING TO PORT」（非「后续」）；钉死两假阳性（Spark migrate_table / iceberg expire_snapshots）；记未来 seam 位置（`ExecuteActionFactory:59-62` + 可选 `ConnectorProcedureOps`/E2 P6）；可选负回归（CALL/EXECUTE 仍报错）| B6 | D | ✅ | 零 code。B6 firsthand 核实：legacy `datasource/paimon/`+连接器 **0** procedure/action 文件；闭式 reject **双路**——`ALTER…EXECUTE`→`ExecuteActionFactory:59-62`（paimon=`PluginDrivenMvccExternalTable extends ExternalTable`→`else if(instanceof ExternalTable)`→`DdlException`），`CALL paimon.x`→`CallFunc:42-43`（闭式 switch default→`AnalysisException`）。doc 早于设计期已闭环（recon §3.3、connectors/paimon.md E2 行）。**neg-regression 归 B7 live-e2e**（验收 :72；结构已 guard，离线 UT 冗余故不加）|
| P5-T27 | **翻闸**：paimon 入 `SPI_READY_TYPES:52` + 删 built-in case `:142` + `pluginCatalogTypeToEngine` 加 `paimon→ENGINE_PAIMON`（`:937-944`）+ 删 `PhysicalPlanTranslator` PAIMON 分支(`:781`)+import(`:71`)| B7 | C | ✅ 已合入 #64446 | **⚠️ 翻闸面 > 此 4-site 文档**：2026-06-11 9-agent 分类 + firsthand 证实**文档外 2 必修**（① `UserAuthentication:57-63` 加 PluginDrivenSysExternalTable unwrap = sys-表 auth 回归；② `PluginDrivenExternalTable.getEngine()/getEngineTableTypeName()` 加 `case "paimon"` = engine-名回归）+ `CreateTableInfo:395` 硬编码 MaxCompute 消息须修。余 site 全 LEGACY_DEAD/GENERIC_OK。**全 edit-set + 分类见 HANDOFF + [[catalog-spi-p5-b7-cutover-scope]]**。真正完成门 = B7 live-e2e（用户跑） |
| P5-T28 | **翻闸 GSON 原子**：5 catalog 名 + db + table 全转 `registerCompatibleSubtype`→PluginDriven*（table→**通用** `PluginDrivenMvccExternalTable`，D-042，非 paimon 专类）；加 5 flavor tag replay 测 | B7 | C+T | ✅ 已合入 #64446 | 漏 db→ClassCastException。`GsonUtils:391-397/451/472`+删 7 import `:169-175`。**+ 用户签 D-045 = restore SHOW PARTITIONS 5 列 / D-046 = restore SHOW CREATE TABLE LOCATION+PROPERTIES**（full parity，非 MC 缩减；签 D-047=Hybrid SPI）→ **见 T36/T37 ✅** |
| P5-T36 | **D-045 restore SHOW PARTITIONS 5 列**：SPI `ConnectorPartitionInfo` 加 typed `long fileCount`（7-arg ctor，3-arg 默认 UNKNOWN，equals/hashCode）；`ConnectorCapability.SUPPORTS_PARTITION_STATS`；`PaimonConnector` 声明 capability + `collectPartitions:891` 喂 `partition.fileCount()`；`ShowPartitionsCommand` capability-gated 5 列 handler + getMetaData（`hasPartitionStatsCapability` 同 gate 两站点；MaxCompute 保持 1 列）| B7 | C+T | ✅ 已合入 #64446 | D-047=Hybrid。列：Partition/PartitionKey(=表分区列名 comma-join，每行同)/RecordCount(=getRowCount)/FileSizeInBytes(=getSizeBytes)/FileCount(=getFileCount)。**NIT（保留）**：5 列路径应用 partition-name `filterMap`（WHERE Partition=...），legacy 5 列 handler 忽略——新行为更正确（同 1 列/HMS 路径），无 golden 用 WHERE。测：ConnectorPartitionInfoTest(3)+PaimonConnectorMetadataPartitionTest.listPartitionsCarriesFileCount+ShowPartitionsCommandPluginDrivenTest.testHandlerEmitsFiveColumns。4-lens 对抗 review clean |
| P5-T37 | **D-046 restore SHOW CREATE TABLE LOCATION+PROPERTIES**：连接器 `buildTableSchema:202` 把 `((DataTable)table).coreOptions().toMap()`（含 path）+ 注入 `primary-key` merge 入 schemaProps（+ plumb `table` 入参，2 call-site）；`PluginDrivenSchemaCacheValue` 加 `tableProperties`（4-arg 重载，3-arg 默认 emptyMap）；`PluginDrivenExternalTable.getTableProperties()`（剔 schema-control 键 partition_columns/primary_keys）；`Env.getDdlStmt` PLUGIN 分支(`:4927`)render LOCATION '<path>'+PROPERTIES，unwrap `PluginDrivenSysExternalTable`→source，**空-props gate**（MaxCompute 空→保持 comment-only）| B7 | C+T | ✅ 已合入 #64446 | D-047=Hybrid。byte-parity legacy（golden `test_paimon_table_properties.out`）。**凭据**：firsthand+4-lens credential-leak 证伪——table coreOptions 仅 path/write-only/file.format，catalog 凭据在 catalog 级（B2 已 neuter getProperties），不泄漏。**连接器侧 coreOptions merge 无离线 UT**（FakePaimonTable 非 DataTable）→ code-review + B9 live-e2e 覆盖。测（fe-core 侧）：PluginDrivenExternalTablePartitionTest.testGetTableProperties×2。仅 fix 4927（SHOW CREATE 走 `getDdlStmt(Command,...)` 重载；4507 重载 legacy 无 PAIMON LOCATION 故不改）|
| P5-T29 | **删 legacy + maven 依赖**（🎯 **下一个 session 的活**）：删 `datasource/paimon/`(**30**) + `metacache/paimon/`(3) + `systable/PaimonSysTable`(1) + 清 8 处反向引用死分支/import + 删 fe-core paimon maven 依赖；**硬前置**=迁出 `PaimonExternalCatalog` 常量(被 5 个 STILL-CONSUMED metastore-props 引)；**STILL-CONSUMED 不删**=`property/metastore/Paimon*`(7)；验 paimon-core FE classpath 恰一份（R-004/R-007 NoClassDefFound 守）| B8 | C | 🚧 **Batch1 ✅** | **Batch1(C1)=删 33 dead + 清 6 reverse-ref + 5 dead-test + inline `getPaimonCatalogType` 常量**，local-commit `7632a074e4b`（未 push，fe-core test-compile+checkstyle 绿、49 测过）。用户签 **Plan B**(fully paimon-free) + **D-PB1** strip-in-place(不物理搬 7 类) + **D-PB2** phased；**B1-strip 6 metastore-props + 迁 `PaimonVendedCredentialsProvider` + 删 5 maven dep 移到 Batch2**(docker-gated)。完整修订计划见 [design doc](./designs/P5-T29-paimon-legacy-removal-design.md) |
| P5-T30 | post-cutover 回归：SHOW PARTITIONS + partitions TVF（预接 FE 分发现返行）/DROP·CREATE DB·TABLE/no-ENGINE CREATE/edit-log replay/MTMV 增量刷/sys-table/session-TZ 谓词不丢行 | B9 | T | ⏳ | 翻闸前已跑过一轮（B7 硬门）；P5-T29 删 legacy 后**复跑一遍**确认无回归，可与 T29 §E 验证合并 |

---

## P5-T29 执行计划（B8 删 legacy + maven 依赖）— scope ledger（2026-06-20 在 `branch-catalog-spi` firsthand 核实）

> 🎯 这是 P5 阶段最后一块主体工作。对照基线 = [`reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md`](../reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md) §B8 deletion readiness ledger。样板 = **P4 #64300**（"make fe-core odps-free"，删文件+清反向引用+删 maven 依赖+`dependency:tree` 验证）。
> **⚠️ 这不是一次 `rm -rf datasource/paimon/`**——有 STILL-CONSUMED 子树 + 常量耦合前置，naive 删除断编译。

### A. DEAD —— 可删
- [ ] `datasource/paimon/`（**30** 文件，含 `source/`、`profile/`；catalog/factory/db/table、`PaimonExternalCatalog`、`PaimonExternalMetaCache`、`PaimonSysExternalTable`、legacy `source/PaimonScanNode`/`PaimonSplit`/`PaimonSource`、legacy 重复 `source/PaimonPredicateConverter`/`PaimonValueConverter`(P1-T02 推迟项现可收)）。
- [ ] `datasource/metacache/paimon/`（**3**：`PaimonTableLoader`/`PaimonPartitionInfoLoader`/`PaimonLatestSnapshotProjectionLoader`）。
- [ ] `datasource/systable/PaimonSysTable.java`（**1**）。
- [ ] 消费方死分支/import 清理（文件保留，只删 paimon 分支）：`ExternalMetaCacheMgr`（`paimon()`/`ENGINE_PAIMON` 路由 + `PaimonExternalMetaCache`）、`metacache/ExternalMetaCacheRouteResolver`（`ENGINE_PAIMON` 注册）、`catalog/Env`、`nereids/.../UserAuthentication`、`nereids/.../ShowPartitionsCommand`、`credentials/VendedCredentialsFactory`、`ExternalCatalog`（`buildDbForInit` 死分支）。**逐个 grep 确认是死分支再删。**
- [ ] 死测试：`ExternalMetaCacheRouteResolverTest`、`planner/PaimonPredicateConverterTest`（测 legacy 重复转换器）、`StatementContextTest`（paimon 用法）等——按编译失败/语义死亡逐个判。

### B. 硬前置（删 `datasource/paimon/` **之前**必做）
- [ ] **迁出 `PaimonExternalCatalog` 常量** `PAIMON_FILESYSTEM`/`PAIMON_HMS`（及其它被引常量）——被 **5 个 STILL-CONSUMED** `property/metastore/Paimon*MetaStoreProperties` 类 import（已核实）。须先搬到存活的家（metastore-props 模块常量持有者 / `fe-kerberos` / 新常量类），再删 catalog 类。
- [ ] scrub 悬空 javadoc `{@link PaimonSysTable}`（`PluginDrivenSysTable`/`NativeSysTable` 等），否则 strict checkstyle/javadoc 挂。
- [ ] 保 load-bearing dispatch ordering（PluginDriven 分支先于任何 legacy 分支）。
- [ ] **`ENGINE_PAIMON` 区分**：`metacache` 两处 DEAD（删）；**`nereids/.../info/CreateTableInfo.ENGINE_PAIMON`（`:123`，被 `:790/:937/:967/:1150` 用作翻闸后 engine 名 + distribution 校验）是 LIVE，保留。**

### C. STILL-CONSUMED —— **不在 P5-T29 删除范围**（删了断 cutover Kerberos 装配）
- `property/metastore/Paimon*MetaStoreProperties`（HMS/DLF/Rest/Jdbc/FileSystem，5）+ `AbstractPaimonProperties` + `PaimonPropertiesFactory`（共 7）+ 其测试 `Paimon*MetaStorePropertiesTest`。cutover `initPreExecutionAuthenticator`→Kerberos 装配 **LIVE**（P6 review R1）；属 metastore-storage-refactor 子线（D-016），主线 B8 不碰。

### D. Maven 依赖（用户明确点名「相关 maven 依赖」）— ⚠️ **核心冲突，须先定决策**
`fe/fe-core/pom.xml:543-563` 含 `paimon-core`/`paimon-common`/`paimon-format`/`paimon-s3`/`paimon-jindo`（+ `:576` s3 aws-bundle 注释，与 iceberg-aws 共享）。
- **关键事实**：C 项 STILL-CONSUMED `property/metastore/Paimon*` **直接 import `org.apache.paimon.*` SDK**（已核实 6 文件）→ **只要它们留 fe-core，fe-core 就不可能像 P4(odps-free) 完全 paimon-free。**
- 可能可删：`paimon-format`/`-s3`/`-jindo`（legacy reader/格式/IO 专用，随 `datasource/paimon/source` 删除而无消费方）；可能保留：`paimon-core`/`-common`（metastore-props 用）。真实可删集合由 `dependency:tree | grep paimon` + 编译敲定。
- **🔱 开放决策（建议下一 session 先 AskUserQuestion）**：是否把 `property/metastore/Paimon*` 一并迁出 fe-core 使其完全 paimon-free？
  - **方案 A（推荐，对齐 master plan B8 / D-016 scope）**：保留 STILL-CONSUMED metastore-props，**只删 DEAD 子树 + 部分 maven 依赖**（fe-core 保 paimon-core/common）。surface 小、与已签 B8 scope 一致。
  - **方案 B（更大，越界子线）**：连带迁出 metastore-props，fe-core 完全 paimon-free（对齐 P4 终态）；碰 metastore-storage-refactor 子线，宜单独立项或与子线 P2-T05 合并。

### E. 守门 / 验证（mirror P4 #64300）
- [ ] fe-core 编译 BUILD SUCCESS + checkstyle 0 + import-gate 净（`tools/check-connector-imports.sh`）。
- [ ] 连接器测试仍绿（删 legacy 不应触连接器）。
- [ ] `dependency:tree` 验 paimon-core 在 FE classpath 恰一份（R-004/R-007 NoClassDefFound / SDK 单例守）。
- [ ] regression-gated live-e2e（`enablePaimonTest=true`，用户跑）= 删后 5-flavor 读 + sys-table + MTMV + DDL 不回归（= B9/P5-T30，可合并）。
- [ ] 逐子树删 + 每批跑编译（参 master plan §3.9 / §4 playbook 第 13 步）。

---

## 批次依赖 / 翻闸前置门

```
B0 (test harness + parity baseline)  ──┐
                                        ├─> B1 (flavors+props+catalog) ──┬─> B2 (normal-read) ──┐
                                        │                                └─> B3 (DDL metadata)  ─┤
B6 (procedure doc no-op, 独立)          │                                                        ├─> B4 (sys-tables E7 + MVCC E5) ─> B5 (MTMV 桥)
                                        │                                                        │
                                        └────────────────────────────────────────────────────> B7 (cutover 原子) ─> B8 (删 legacy) ─> B9 (回归)
```
- **B7 翻闸 gated on B2+B3+B4+B5 全完**（否则 MTMV/MVCC/sys-table 静默回归）——**除非 D2 取「fail-loud 延后」则 B5 降为 fail-loud 守护**。
- **翻闸前置硬门**：① live e2e（真实 paimon 各 flavor，用户跑）② FE→BE round-trip smoke ③ B0 parity baseline 绿 ④ D1/D2 已签字。
- B6 独立可随时落（doc-only）。

---

## 🔱 开放决策（✅ 已签字 2026-06-09）

> 镜像 P4 recon §9 SCOPE FORK。两项用户级决策已签字：**D1=A、D2=A**（均推荐方案）。下文保留 fork 全貌作追溯。

### D1 — flavor 装配模型 → ✅ **采纳 A（单 Catalog + flavor switch，MC 一致）**

| 方案 | 范围 | 风险 | 推荐 |
|---|---|---|---|
| **A. 单 Catalog + flavor switch（MC 一致）** | `PaimonConnector.createCatalog` 内 switch on `paimon.catalog.type`，拷 warehouse/conf/authenticator 入模块 | 中（拷贝量 + Kerberos/S3/DLF/JDBC 正确性）| **✅ 推荐**：与 MC「拷常量入模块」一致、surface 小、无新模块 |
| B. 5 backend 模块 + ServiceLoader | 建 `fe-connector-paimon-api` + 4 backend + `PaimonBackendFactory` | 高（无 MC 先例、大 surface、空壳须从零建）| 仅当团队要忠于 legacy 拆分 |

两方案都须把 `StorageProperties`/`HMSBaseProperties`/`HadoopExecutionAuthenticator`（fe-core/fe-common，禁 import）**从属性 map 重建或拷最小封装**；**每-flavor authenticator 必须保**。

### D2 — MTMV + MVCC scope（翻闸 gating）→ ✅ **采纳 A（P5 内实现 MTMV/MVCC 桥，B7 翻闸 gated on B5）**

> cross-cut 硬结论：**禁**把翻闸当「full-adopter 完成」却无 MTMV 桥而静默读 latest。

| 方案 | 范围 | 翻闸门 | 推荐 |
|---|---|---|---|
| **A. P5 内实现 MTMV/MVCC 桥（B5）** | 落 `PaimonPluginDrivenExternalTable` + E5 wiring + GAP-LISTPART-AT-SNAPSHOT | B7 翻闸 gated on B5 | **✅ 推荐**：保留 legacy 全部 MTMV/时间旅行能力，真 full parity |
| B. 翻闸先行 + MTMV fail-loud 延后 | 翻闸只做普通读/DDL/sys-table；MTMV-base/time-travel 命中 SPI paimon 表**显式报错** | B7 不 gated on B5，但须 fail-loud 守护落地 | 若要尽快翻闸、可接受暂不支持 paimon-MTMV-base + 时间旅行 |

无论 A/B，**禁止**静默读 latest 回归（B 方案的 fail-loud 守护本身是必交付项）。

### D3 — 次要确认（非 fork，记录默认）

- sys-table rowcount：返 `UNKNOWN_ROW_COUNT`（对齐 iceberg，弃旧 `fetchRowCount` plan() 往返）—— 默认采纳。
- sys-table branch="main" 限制：保留（非 main 分支 sys 表暂不支持）—— 默认采纳 + 文档。
- 弃 `PaimonScanMetricsReporter`（连接器禁 import profile）→ EXPLAIN/profile paimon scan 指标回归 —— 登记为已知 behavior 回归。
- COUNT 下推 / cpp-reader / history-schema：初版翻闸**延后**（仅 perf/edge parity，correctness 不丢）—— 默认采纳。

### D4–D6 — B2 设计定夺（✅ 签字 2026-06-09；code-grounded understand 复审纠偏 plan 前提）

> B2 启动时 6-agent understand workflow + 主线 firsthand 核读，纠偏 **3 处 plan 前提**，用户签字 A/A/A（均推荐）。这 3 处 plan 备注（基于 recon）与 firsthand 代码冲突，按 Rule 7 不取平均、择 code 真相。

**D4 — T07 时间戳谓词 TZ → ✅ 采纳「parity-correct（不 session-TZ）」**
- **纠偏**：plan T07「session-TZ 化替固定 UTC」沿用 MC gotcha [[catalog-spi-connector-session-tz-gotcha]]，但 firsthand 核：legacy `paimon/source/PaimonValueConverter:149` 时间戳字面量用**固定 GMT/UTC** 转 epoch，且**无** `visit(LocalZonedTimestampType)`（LTZ 落 defaultMethod→null→**legacy 根本不下推 LTZ**）。连接器现 `PaimonPredicateConverter:284` 固定 `ZoneOffset.UTC` 对 NTZ **已 = legacy parity**；对 NTZ session-TZ 化会移位下推谓词 vs paimon UTC-based min/max stats → **假裁剪丢行**。MC≠paimon（时间戳存储语义不同）。
- **决定**：NTZ 保持 UTC（勿动 zone）；LTZ 改为**不下推**（`TIMESTAMP_WITH_LOCAL_TIME_ZONE` return null，补齐 legacy parity + 修当前 over-push 的潜在 LTZ 假裁剪）；加 `PaimonConnectorMetadata.supportsCastPredicatePushdown=false`（镜像 MC:331-334）。**不** session-TZ 化、**不**加 ZoneId 惰性解析。

**D5 — T08/T09 分区处理 scope → ✅ 采纳「minimal/safe」**
- **纠偏（latent bug）**：`PaimonConnectorMetadata.getTableSchema:133` 发 schema key `partition_keys`，但 fe-core `PluginDrivenExternalTable:181` 读 `partition_columns`（MC 正确发 `partition_columns:163`）→ **FE 现把所有 paimon 表当非分区**（SHOW PARTITIONS/TVF 空、`getNameToPartitionItems` 空、无 MTMV 分区喂）。paimon 经谓词下推（`ReadBuilder.withFilter`+SDK `scan.plan`）仍正确裁剪分区/文件，行数 parity 不丢。
- **决定（B2）**：T09 = **文档化纯谓词裁剪**（不 override 6-arg `planScan`；scan-correct，EXPLAIN partition=N/M 显示损失为已知 cosmetic gap）。T08 = 实现连接器侧 `listPartitions*`（连接器 SPI deliverable，B5/B9 复用）+ 扩 seam `listPartitions(Identifier)`，但 **B2 不翻 `partition_columns` key、不接 FE 消费**；**`getProperties` 不实现**（实现中 firsthand 纠偏：`ConnectorMetadata.getProperties()` 在 fe-core 零消费方 + MC 不 override + 返原始 props 会泄漏 ak/sk 凭据 → 留 emptyMap stub）（避免提前激活 FE 分区裁剪→ prune-dataloss 危险区 [[catalog-spi-nonpartitioned-prune-dataloss]] / [[catalog-spi-plugindriven-explain-override-gap]]，须与 requiredPartitions 处理同落）。**key 翻 + 6-arg planScan/requiredPartitions + MTMV 分区喂 = B5 前置硬门**（记入 B5）。
- **NTZ/LTZ 谓词与分区**：分区裁剪走纯谓词；上面 D4 的 LTZ 不下推不影响分区裁剪正确性（LTZ 极少做分区列）。

**D6 — T10 连接器 cache scope → ✅ 采纳「延后至翻闸」**
- **纠偏**：plan 前提「REFRESH CATALOG 经 `PluginDrivenExternalCatalog:530-534` 销毁 connector」**证伪**——firsthand：`RefreshManager.refreshCatalogInternal` 只调 `((ExternalCatalog)c).onRefreshCache(invalidCache)`（清 fe-core cache via `invalidateCatalog`，**connector 存活**）；`resetToUninitialized`→`onClose`(连接器销毁) 仅 `CatalogMgr.addCatalog`/MODIFY/DROP CATALOG 触发，**REFRESH CATALOG 不触**。REFRESH TABLE 也**永不达 connector**（`refreshTableInternal` 只 `unsetObjectCreated`+`ExtMetaCacheMgr.invalidateTableCache`，全留 fe-core）。`ConnectorMetadata` SPI 无任何 invalidate/refresh hook。
- **决定**：B2 **不加** connector cache（normal-read 无 cache 即正确；现每次 `catalogOps.getTable` 打活 catalog）。cache + invalidation SPI 设计（default-no-op `invalidateTable(...)` + `onRefreshCache` 触发的 connector clear hook + RefreshManager wiring）记为 **B8/删 legacy 前置**，随翻闸落地（fe-core SPI/RefreshManager wiring 与 cutover 同批更合理）。否则 naive cache 在 REFRESH CATALOG/TABLE 后供陈旧 Table/schema。

### D7 — B3 DDL authenticator scope → ✅ **采纳 B（legacy parity：每 DDL call 包 `executeAuthenticated`）**

> B3 启动时 6-agent understand workflow + 主线 firsthand 核读，纠偏 **1 处 plan 前提**（其余 T11-T15 plan 备注与 code 一致，含纠偏「PluginDrivenExternalCatalog 已 override FE 侧」**证实为真**——memory [[catalog-spi-cutover-fe-dispatch-gap]] 警告的 FE 分发缺口对 paimon DDL 不适用，MC 已证端到端通；缺口的真闸是 `SPI_READY_TYPES` 成员，属 B7/T27 非 B3）。

- **纠偏**：plan T13「per-flavor authenticator」沿用 legacy 直觉，但 firsthand：① MC DDL **完全不**用 authenticator（不同 auth 模型）；② legacy `PaimonMetadataOps` **每个**远端 DDL call 包 `executionAuthenticator.execute`；③ **B2 刚落地的 read 路径不 re-wrap**（靠 catalog 构建时 `PaimonConnector.createCatalogFromContext:166-172` 的一次 wrap）；④ `PaimonConnectorMetadata` 当前**不**收 `ConnectorContext`。
- **fork**：A=match B2 read 路径（不 per-call wrap，靠构建时 wrap，per-call authenticator 作单一 connector-wide live-e2e 项，已是 R-中）｜**B=legacy parity（thread `ConnectorContext` 入 metadata + 每 DDL op 包 `executeAuthenticated`）**。
- **用户签 B**：metadata-level wrap（保 seam 为纯 Catalog delegate）；4 个 DDL op（createTable/dropTable/createDatabase/dropDatabase）各包一次 `context.executeAuthenticated`（dropDatabase 的 enumerate-loop+cascade 整体一个 scope，UGI doAs 可重入，行为等价 legacy）；**read 路径仍不 wrap**（B 仅 DDL 域，未回改 B2）。`executeAuthenticated` 默认 no-op → 离线测不受影响，正确性须 live-e2e 验。
- **遗留不一致（记录）**：read 路径未 per-call wrap（B2 决策），DDL 已 wrap（D7=B）——若 live-e2e 证 Kerberized **读**也需 call-time doAs，则 read 路径须补 wrap（B2 回改，非 B3 范围）。归入翻闸前 live-e2e authenticator 门。

### D-040 / D-041 / D-042 — B5 设计定夺（✅ 签字 2026-06-10；understand 6-lens workflow + 主线 firsthand 核读纠偏）

> B5 启动 understand workflow（6 read-only lens 验 plan 前提）+ 主线 firsthand 核读，纠偏 T22-T25 多处 plan 前提，用户签 3 决策。下文为 B5 权威设计（覆盖原 T21-T25 plan 备注，按 Rule 7 择 code 真相）。

**D-040 — 时间旅行 scope → ✅ 采纳「全 parity（含 tag/branch/@incr）」**。legacy 支持 `FOR TIME/VERSION AS OF`(snapshot-id/timestamp/tag) + `@branch` + `@incr`(incremental-read)。T20 SPI 仅 `getSnapshotById`/`getSnapshotAt(timestamp)`——**tag/branch/incremental 零 SPI 面**，须新增。统一模型：全部归结为 **paimon scan-options map（`Table.copy(opts)`）或 branch-table load**；连接器解析、`ConnectorMvccSnapshot.properties`（或 handle）承载、scan-pin 线程入两 resolveTable 站点应用。`@incr` 的 ~180 行 `validateIncrementalReadParams` 移入**连接器**（产 paimon 键），fe-core 仅传 raw doris param map。empty-if-none → 子类译回 `UserException`（否则坏 version 静默 latest）。

**D-041 — T21 at-snapshot listPartitions → ✅ 采纳「drop，list at latest」**。legacy **从不**在非-latest snapshot 列分区（time-travel 路径用 `PaimonPartitionInfo.EMPTY`）；`Catalog.listPartitions(Identifier)` snapshot-agnostic，真 at-snapshot 须新 seam（`DataTable.newSnapshotReader().partitionEntries()`+`InternalRowPartitionComputer`）**超 legacy parity**。故 T21 仅「list at the begin-query pin 的 snapshot（MTMV=latest）」=现有 `catalogOps.listPartitions` 即可；接受两-SDK-call 微 race（legacy 单调用原子，doc 记之）。**不建新 SDK seam**。

**D-042 — MTMV/MVCC 实现归宿 → ✅ 采纳「通用 `PluginDrivenMvccExternalTable`（capability-selected），NO paimon-specific fe-core 类」**。用户否决 plan T22 的 `PaimonPluginDrivenExternalTable`：**违反 SPI 原则**（core 不应有特定源实现类，等于改名 legacy `PaimonExternalTable`）。改用**源无关** `PluginDrivenMvccExternalTable extends PluginDrivenExternalTable implements MTMVRelatedTableIf, MTMVBaseTableIf, MvccTable`（可复用 iceberg/hudi）；catalog 按连接器 `SUPPORTS_MVCC_SNAPSHOT` capability 选择实例化（jdbc/es/mc/trino 仍用裸 `PluginDrivenExternalTable` 不受影响=隔离爆炸半径）；新类 GSON 注册（drop 原「table→PaimonPluginDrivenExternalTable」B7 任务）。全方法 SPI-delegate，唯一 paimon-specific（date render / incremental 译 / tag·branch resolve）留**连接器**。

**recon 纠偏（code-truth，已并入下方 task 表）**：① `ConnectorMvccSnapshotAdapter` 是 **zero-callers 非 zero-ctor**（已建，直接用）。② T23 漏 `MvccTable.loadSnapshot` + 2-arg `getTableSnapshot(MTMVRefreshContext,Optional)` 重载（两个 abstract 必实现）；`getTableSnapshot` 返**真 pinned snapshotId** 非 `(-1)`（-1 仅空表）；`getPartitionColumns(Optional)` 已被 base 继承；`getPartitionType`→**LIST**；`getPartitionColumnNames`→lowercase。③ 子类 override `getNameToPartitionItems`：读 **pin**（base 忽略 snapshot 参 live-list latest）+ 从 **rendered partitionName** 解析值（`HiveUtil.toPartitionValues`，源无关、修 base 喂 RAW epoch-day 致 DATE 分区静默丢行/抛；同时覆盖普通 paimon 分区裁剪非仅 MTMV；base 留 MC 不动）。④ 6-arg `planScan`/`requiredPartitions` **非** paimon 需求（PaimonScanPlanProvider 纯谓词裁剪 scan-correct）；随 key-flip 的硬需求是 RAW→RENDERED。⑤ T25 `isPartitionInvalid` **不能复用 base `TablePartitionValues`**（转换失败**抛 CacheException 非 skip-and-count**）；须 port legacy `PaimonUtil.generatePartitionInfo` per-partition try/catch + size 比对→UNPARTITIONED。⑥ T19 guard **已 live**（translator `PhysicalPlanTranslator:793-796` 喂 snapshot/scanParams 给所有 FileQueryScanNode）；dormant 的是普通表 time-travel READ。⑦ T24 字段表剔除 `PaimonPartition`（$partitions sys-table 行 decoy 未用）；通用 wrapper 持 `ConnectorMvccSnapshot` + materialized `nameToPartitionItem`/`nameToLastModifiedMillis`/`listedCount`。⑧ L6 确认 `ConnectorPartitionInfo.lastModifiedMillis`(=`Partition.lastFileCreationTime()`,T08) byte-parity 正确；R-high 可靠性 parity-inherited 非 B5 新增。

**BLOCKER（不在原 task 表）= scan-side snapshot pin**：`ConnectorScanPlanProvider.planScan` 无 snapshot 参、`PaimonTableHandle` 无 snapshotId、**两个** resolveTable 站点（`planScan` + `getScanNodeProperties` JNI serialized_table）。不接 pin 则任何 time-travel + MTMV 一致读静默读 LATEST。修=handle 带 snapshotId/scan-options，两站点都 `table.copy(opts)`；改 fe-core handle 流必 grep 全 `resolveTable`/`getTableHandle`（同 B4 教训）。

**B5 拆 B5a→B5b（均 gate cutover B7；通用类 inert until B7 路由，同 T20 范式，unit-test 直构造）**：
- **B5a = MTMV core + 分区激活**（D2=A 心）：连接器 emit `partition_columns`（替 `partition_keys`）；通用 `PluginDrivenMvccExternalTable` + capability 工厂 + GSON；`loadSnapshot`（latest pin + materialize rendered 分区一次）；通用 fe-core MVCC snapshot wrapper（T24 corrected）；MTMV 方法全套（T23 corrected）；`isPartitionInvalid` parity + RAW→RENDERED override + 单-pin 测（T25 corrected）；scan-pin（latest 一致读）。
- **B5b = explicit time-travel 全 parity**：AS-OF(snapshot-id+timestamp，子类解析 TableSnapshot+empty→UserException) / tag(新 SPI) / branch(新 SPI，Identifier branch 分量/branch-table load) / incremental(validate+paimon 键移入连接器，fe-core 传 raw param map)；scan-options 经 `ConnectorMvccSnapshot.properties`/handle 线程两 resolveTable 站点。

### D-043 / D-044 — B5b 设计定夺（✅ 签字 2026-06-10；understand recon(4-lens workflow)+ 主线 firsthand 核读 PaimonExternalTable/PaimonScanNode/PaimonUtil + 全 SPI/schema-cache 路径）

> B5b 启动 read-only recon（legacy AS-OF/tag/branch/incremental + SDK seam + planner→loadSnapshot 流 + schema-cache 机制）+ 主线 firsthand 核读，纠偏后用户签 2 决策。下文为 B5b 权威设计（覆盖原 T32-T35 plan 备注简略版）。

**D-043 — schema-at-pinned-snapshot → ✅ 采纳「include in B5b（全 parity）」**。recon 证实**真 divergence**：legacy `getPartitionColumns(snapshot)` + `getFullSchema()` 经 `getPaimonSchemaCacheValue(MvccUtil.getSnapshotFromContext(this))` 解析 **pinned schemaId** 的 schema（schemaManager().schema(schemaId)），通用类返 LATEST。仅跨 schema 演进的旧 snapshot 才异（无演进则等价；time-travel 分区 items 恒 EMPTY 故 pruning 不受影响，唯列集/类型异）。**实现走轻量路径（避开 schema-cache 基建改动）**：① legacy 只 override `getSchemaCacheValue()`（no-arg，读 context pin），`getFullSchema`/`getPartitionColumns` 经其自动流通 → 通用类同样**只 override `getSchemaCacheValue()`**（context pin 在 → 返 snapshot 携带的 pinned schema，否则 super=latest）。② pinned schema **在 loadSnapshot 一次性解析**（连接器 `getTableSchema(session,handle,snapshot)` snapshot-aware 重载，按 snapshot.schemaId 解析）并**装入 `PluginDrivenMvccSnapshot`**（per-query，非 schemaId-keyed 跨查询 cache；perf-only 偏差，结果 parity，doc 记）。**不建** `PluginDrivenSchemaCacheKey`/keyed `initSchema`/`ExternalMetaCache` 改动。

**D-044 — time-travel SPI 形状 → ✅ 采纳「unified `resolveTimeTravel(spec)`」**。统一一个 SPI 方法 + `ConnectorTimeTravelSpec` 值类型（kind∈{SNAPSHOT_ID,TIMESTAMP,TAG,BRANCH,INCREMENTAL} + stringValue + digital + incrementalParams），连接器内部分发全模式（含 **timestamp 串在连接器解析** = paimon `DateTimeUtils.parseTimestampData(v,3,sessionTZ)` byte-parity，TZ 取 `ConnectorSession.getTimeZone()` [[catalog-spi-connector-session-tz-gotcha]]）。**fe-core 仅做源无关分发/抽取**（TableSnapshot type+digital 判定、TableScanParams tag/branch/incr 判定、`extractBranchOrTagName`、mutual-exclusion、empty→UserException 译）。现有 inert `getSnapshotAt(millis)`/`getSnapshotById(id)`（T20，零其他消费方）→ **被 resolveTimeTravel 取代，删之**。

**B5b SPI 面（净增）**：① `ConnectorTimeTravelSpec`（新值类型）。② `ConnectorMetadata.resolveTimeTravel(session,handle,spec)→Optional<ConnectorMvccSnapshot>`（default empty）。③ `ConnectorMvccSnapshot` 加 `schemaId`(long，default -1) 字段。④ `ConnectorTableOps.getTableSchema(session,handle,ConnectorMvccSnapshot)` snapshot-aware 重载（default → latest 重载；`getTableSchema` 实声明在 `ConnectorTableOps` 非 `ConnectorSchemaOps`）。⑤ `applySnapshot` 改：thread 全 `snapshot.getProperties()`（scan-options map）入 `handle.scanOptions`；properties 空 → fallback `scan.snapshot-id=snapshotId`(B5a latest-pin parity)。**incremental null-reset 键**（`scan.snapshot-id=null`/`scan.mode=null`）连接器侧**剔除**（fresh base table 上 no-op，且 `ConnectorMvccSnapshot.properties` builder 拒 null）；doc benign divergence。**branch SDK 机制**（`CoreOptions.BRANCH` scan-option vs catalog 3-arg branch-load）impl 时按 paimon 版本核定。

**B5b 拆 B5b-1→B5b-4（subagent-driven，build-on-previous，shared dirty tree，无 commit）**：
- **B5b-1 = SPI 面（纯 additive，树仍编译）**（fe-connector-api）：`ConnectorTimeTravelSpec` + `resolveTimeTravel` default + `ConnectorMvccSnapshot.schemaId` + `getTableSchema(...,snapshot)` 重载 + `applySnapshot` 全-properties 契约 doc。**不删** inert `getSnapshotAt/getSnapshotById`（删之会断 `PaimonConnectorMetadata` 的 `@Override` 编译；退役并入 B5b-2 一起改 api+connector+测）。值类型 + default 行为测。
- **B5b-2 = 连接器解析 + 退役死 seam**（fe-connector-paimon + fe-connector-api 删法）：`PaimonConnectorMetadata.resolveTimeTravel`（5 模式分发）+ port `validateIncrementalReadParams`(~180 行)+ PaimonUtil 解析助手（timestamp/tag/branch/isDigitalString）入连接器 + 新 `PaimonCatalogOps` seam(getSnapshotByTag/branchExists+load/schema-at-schemaId)+ `getTableSchema(snapshot)` impl + `applySnapshot` 全-properties + 扩 Recording/FakePaimonTable + **退役 `getSnapshotAt/getSnapshotById`（api+connector+测一并删，零消费方）**。全模式测 + incremental mutation-kill + tag/branch + schema-at-snapshot。
- **B5b-3 = fe-core 通用分发 + schema-at-snapshot**（fe-core）：`loadSnapshot` 替 placeholder（抽 spec + mutual-excl + empty→UserException + EMPTY 分区 maps + pinned schema 装入）+ `PluginDrivenMvccSnapshot` 加 schemaId+pinnedSchema + `getSchemaCacheValue()` context-pin override。分发/mutual-excl/empty/schema-at-snapshot 测。
- **B5b-4 = holistic**（merged build + 3-lens parity/adversarial/scope 复审 + cleanup）。

> **执行态（2026-06-10，未提交）**：B5b-2 实拆 **2a/2b/2c**。**B5b-1 ✅**（审 PASS）。**B5b-2a ✅**（snapshot-id/timestamp/tag + schema-at-snapshot + applySnapshot 全-properties + 退役 getSnapshotAt/ById；双审 + TZ-alias BLOCKER fix=fail-loud）。**B5b-2b ✅**（@incr 180 行 validate port；审 PASS_WITH_NITS，仅测覆盖 NIT 待补）。**B5b-2c ✅**（branch time-travel 连接器侧；implement→4-lens clean-room 并行复审[spec/adversarial/test-mutation/scope]→fix→主线独立 verify，全绿 `Tests run:179`，无 blocker。branch=**handle 身份变更**：`PaimonTableHandle.branchName`[纳入 identity] + `withBranch`[**绝不 copy transient base Table**=避静默读 base] + `Identifier(db,table,branch)` 3-arg load 集中于**单 seam `PaimonTableResolver.resolve`**[覆盖全 6+2 站点] + `branchExists` seam[FileStoreTable cast，非-FST→graceful false] + carry via **properties sentinel `CoreOptions.BRANCH.key()`**[applySnapshot 先于 generic 路检测→withBranch] + empty-branch schemaId=-1[legacy 0L，schema 同=benign]）。**B5b-3 ✅**（fe-core 通用分发 + schema-at-snapshot；implement→4-lens clean-room 并行复审[spec/adversarial/test-mutation/scope]+**逐 MAJOR/BLOCKER 对抗证伪**→test-hardening fix→主线独立 verify，全绿 `PluginDriven*` `Tests run:140 Failures:0 Errors:0`[MvccExternalTableTest 33]，checkstyle 0，**复审 0 产线 defect**。`loadSnapshot` 替 placeholder=源无关 `toTimeTravelSpec` 分发[TIME→timestamp/VERSION+digital→snapshotId/非digital→tag/isTag/isBranch/incrementalRead→getMapParams] + mutual-excl + `resolveTimeTravel` empty→`notFoundMessage`[snapshot/tag/branch byte-parity；timestamp text-diverge=doc'd] + **apply-before-getTableSchema**[branch-aware handle→at-snapshot schema] + EMPTY 分区 maps；`PluginDrivenMvccSnapshot` 加 nullable `pinnedSchema`+`getSchemaId`[3-arg ctor 委派 null=B5a latest 不变]；`getSchemaCacheValue()` override[context-pin→pinnedSchema else `getLatestSchemaCacheValue()` seam]；`PluginDrivenExternalTable.initSchema` 抽 `toSchemaCacheValue` 共享 helper[byte-identical]。错误走 `RuntimeException`[loadSnapshot 无 checked throws，仿 legacy/Iceberg precedent]）。**B5b-4 ✅**（holistic：merged-build 三模块全绿 + 3-lens clean-room 复审[parity/adversarial/scope，Workflow 并行+逐 BLOCKER/MAJOR 对抗证伪]→**查出 1 BLOCKER(RD-1)+1 MINOR(RD-2)**→用户签 fix-now+full-parity→implement→独立 fix-review[3-lens+falsify，**APPROVE 0 defect**]→final build 全绿。**RD-1（BLOCKER，已修）**=partitioned time-travel + 分区谓词→**0 行静默丢数**：连接器 B5 起发 `partition_columns`→FE 视 paimon 为分区表，但 time-travel pin 的分区-item map 恒 EMPTY→`PruneFileScanPartition` 对空 map 剪谓词→`isPruned=true`+空集→`PluginDrivenScanNode.getSplits` 短路(529-531)零 split。legacy 同样 EMPTY+isPruned=true，但 legacy `PaimonScanNode.getSplits` 无短路(走 SDK 谓词下推)故返行→**真 divergence**。修=`resolveRequiredPartitions` 加 `totalPartitionNum==0`(空 universe=time-travel pin)→scan-all(null)→planScan 跑(paimon 弃 requiredPartitions、纯谓词下推)；MaxCompute 真剪零(totalPartitionNum>0)仍短路=parity 不变。**RD-2（MINOR，已修，full-parity）**=`@incr` pin EMPTY 分区 map，legacy `@incr` 落 `getLatestSnapshotCacheValue`=LATEST 分区+LATEST schema→修=`loadSnapshot` INCREMENTAL 分支列 latest 分区+`pinnedSchema=null`(抽 `listLatestPartitions` helper 复用 materializeLatest，byte-identical)，余 4 kind 不变。另顺修 FIX-3(`PaimonScanPlanProvider` stale class-doc "FE 当 paimon 非分区")+`PaimonConnectorMetadata:436` 错注释。2 新意图测[empty-universe scans-all mutation-kill / @incr lists-latest mutation-kill]。**known divergence**(=D1/D2 既录)+1 新 doc'd：MaxCompute 零分区表 scan-all vs legacy 无条件短路=**row-equivalent**(planScan getFileNum<=0 返空)。详 `plan-doc/HANDOFF.md` + [[catalog-spi-p5-b5b-design]]。

---

## 风险 / 开放问题

- **R-高｜单-pin 不变式（MTMV）**：snapshotId 与分区集须同源；缺 GAP-LISTPART-AT-SNAPSHOT 则刷新 staleness keying 静默错位。
- **R-高｜`lastFileCreationTime()` 可靠性**：跨 HMS/DLF/REST/JDBC/filesystem catalog 是否填值 = SDK 行为，**源码不可验**；为 0/未设则 getPartitionSnapshot 出错时间戳→分区永不/永刷。须 live 验。
- **R-高｜MTMV 静默回归**：见 D2，禁静默读 latest。
- **R-中｜每-flavor authenticator 丢**：Kerberized HMS/HDFS DDL 运行时炸，无离线测覆盖。
- **R-中｜paimon-core 版本漂移** FE 连接器 vs BE scanner：InstantiationUtil 跨版本静默破；须 pin + round-trip smoke。
- **R-中｜classloader parent-first（R-004）**：paimon SDK 单一共享实例；多 flavor/版本 catalog 共存依赖 SDK 容忍度（开放）。
- **R-中｜GSON 7 注册**：漏 db→ClassCastException replay。
- **开放｜REFRESH TABLE seam**：`PluginDrivenExternalCatalog` 仅 REFRESH CATALOG 销 connector；REFRESH TABLE 是否触连接器 cache 未核 → 可能需 `invalidateTable` SPI。
- **开放｜BE sys-table `TTableDescriptor`**：旧发 HIVE_TABLE，PluginDriven 默认 SCHEMA_TABLE；须核 BE paimon-scanner 期望。
- **开放｜`isPartitionInvalid` parity**：基类 `TablePartitionValues` 是否静默丢失败转换计数。
- **开放｜JDBC flavor DriverShim/URLClassLoader** 在 parent-first 连接器 loader 下的归属。（B1：DriverShim 已移植，driver_url 经 `ConnectorContext.getEnvironment()` 解析；下条记安全门）
- **R-高｜翻闸门（B1 落地，live-e2e 必验）｜hms/dlf metastore-client 跨 classloader**：连接器只编译 `HiveConf`（hive-common）+ `Configuration`（hadoop-common），**不打包** Thrift `IMetaStoreClient`/`HiveMetaStoreClient`（paimon-hive-connector 的 hive-exec/metastore=test scope；DLF `ProxyMetaStoreClient`）。翻闸时须由 FE host `hive-catalog-shade`(3.1.x) 提供；plugin child-first 下 host 的 `Configuration`/`HiveConf`(3.1.x) 与 plugin bundled(hadoop 3.4.2/hive 2.3.9) 可能身份冲突。翻闸前 live 验：真实 HMS `metastore=hive` 经 plugin 建 catalog 不抛 `NoClassDefFoundError: .../IMetaStoreClient` / `LinkageError` / `ClassCastException`（编译态 ABI 子集已证良性：paimon-3.1 引用的 HiveConf ConfVars/方法在 2.3.9 全存在）。修向（翻闸时定夺）：bundle 自洽 hive 栈并强制 `org.apache.hadoop.hive.` parent-first 用 host shade，或加 metastore-client dep 让整栈单 loader 解析。
- **R-中｜翻闸门｜jdbc driver_url FE 安全 allow-list 未接**：legacy `JdbcResource.getFullDriverUrl` 的 `jdbc_driver_url_white_list`/`jdbc_driver_secure_path`/jar 名校验连接器未复现（禁 import fe-core）。翻闸前须经 `ConnectorContext` hook（参 `sanitizeJdbcUrl`）路由；paimon 未入 `SPI_READY_TYPES` 故当前不可触达。
- **R-中｜翻闸门｜HMS 外部 hive-site.xml 文件加载延后**：`buildHmsHiveConf` 只吃 `hive.*`/auth 键 + storage，未加载 `hive.conf.resources` 指向的 hive-site.xml（legacy 用 fe-core `CatalogConfigFileUtils`，禁 import）。kerberos sasl.enabled/service-principal/auth_to_local 已移植；UGI doAs 经 `executeAuthenticated` FE 注入。

---

## 阶段日志（倒序）

### 2026-06-20（阶段里程碑 · B5–B7 翻闸 + P6 clean-room review + **全部合入 #64446**；本 session = 文档对账，0 产线代码）
- **B0–B7 全完成并 squash-合入 `branch-catalog-spi`** —— **PR #64446 / `38e7140ce56`**（"[refactor](catalog) P5 paimon: migrate to catalog SPI + cutover"），随后 `e9c5b3e70ce`（修编译/HANDOFF）。涵盖：B5 MTMV 桥（通用 `PluginDrivenMvccExternalTable`，D-040/041/042）、B5b 显式时间旅行全 parity（AS-OF/tag/branch/@incr，D-043/044；RD-1 partitioned time-travel 0 行丢失 BLOCKER 已修）、B6 procedure no-op（doc）、**B7 翻闸**（paimon 入 `SPI_READY_TYPES` + GSON 原子转 compat + `PhysicalPlanTranslator` 删 PAIMON 分支 + `UserAuthentication`/`getEngine` 补接 + **D-045/046/047 = restore SHOW PARTITIONS 5 列 / SHOW CREATE TABLE LOCATION+PROPERTIES**，T36/T37）。
- **P6 全路径 clean-room 对抗 review（6 维度 + 7 缺口线，2 波）完成** → 报告 [`reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md`](../reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md)。结论：**2 BLOCKER 都是 B8 删除护栏（非运行时 bug）**（R1=legacy metastore-props + `PaimonExternalCatalog` 常量 LIVE；R2=`property/storage/*Properties` 跨连接器共享 → **B8 不能整包删，须分阶段**）；**2 MAJOR 真活读路回归**（C1 MinIO、C2 HDFS XML，均已修）；其余 parity。发现项各自 fix task，**全部完成并合入 #64446**：C1 MinIO / C2 HDFS XML / R3-residual / R1-table / C4+R2+R3-catalog / 5 个 deviation→fix（A3 self-split-weight / A2 predicates-from-paimon / B-MC2 schema-at-memo / A1 split-weight / B-R2-be schema-dict-memo）。
- **当前 legacy 残留（待 P5-T29 删）**：`datasource/paimon/`(30) + `metacache/paimon/`(3) + `systable/PaimonSysTable`(1) + 8 处反向引用文件 + fe-core paimon maven 依赖(5)。STILL-CONSUMED `property/metastore/Paimon*`(7) 保留。详见 §P5-T29 执行计划。
- **本 session 净产出**：对账 stale 跟踪文档（PROGRESS 停在 B4、本 doc 停在 B5b、HANDOFF 停在历史工作分支）→ 全部刷到「迁移+翻闸已合入、下一步 P5-T29」状态，使下一 session 可直接开工。0 产线代码。

### 2026-06-10（B4 实现：sys-tables E7 + MVCC E5，T16-T20；understand workflow 纠偏 → 用户签 D-039；subagent-driven 5 dispatch + 双审/fix-loop + 3-lens final holistic（1 BLOCKER 修））

- **understand workflow（6-agent read-only）纠偏 2 处 plan 前提**：① **RFC §10 stale**（其 `$`-后缀-via-`getTableHandle` E7 设计从未落地；live fe-core 用 `SysTableResolver`+`NativeSysTable`+`TableIf.getSupportedSysTables/findSysTable`）→ 用户签 **D-039**（复用 live 机制，[DV-023]）；② **T20 MVCC inert until B5**（E5 方法已存在 default-no-op，但 `PluginDrivenExternalTable` 非 `MvccTable`、零 fe-core 消费方、capability 零 reader；翻闸 gated on B5 故 inert capability 安全）→ 用户签「T20 留 B4 作连接器 groundwork」。另核出 **BE 描述符**：legacy paimon（普通+sys）发 `HIVE_TABLE`，而连接器无 `buildTableDescriptor` override → 普通表走 `SCHEMA_TABLE` fallback（[DV-024]，B2 遗留缺陷，B4/T19 一处修）。
- **T16**（greenfield E7 SPI）：`ConnectorTableOps.listSupportedSysTables`+`getSysTableHandle`（default no-op）。**T17**：`PaimonConnectorMetadata` 实现之（名取 `SystemTableLoader.SYSTEM_TABLES`，4-arg sys `Identifier(db,tbl,"main",sysName)` 复用 `getTable` seam，sys handle 加 `sysTableName`+`forceJni`(binlog/audit_log)+lowercase 规范化）；**fix-loop**：抽共享 `PaimonTableResolver`（metadata+scan 一处 sys-aware reload，修 scan-twin 丢 sys-Table BLOCKER）+ Java-serialization round-trip 测。**T18**（fe-core）：`PluginDrivenExternalTable` 集中 handle 获取入 `resolveConnectorTableHandle` seam（4 site）+ `getSupportedSysTables` 委托连接器；通用 `PluginDrivenSysTable extends NativeSysTable` + `PluginDrivenSysExternalTable`（报 `PLUGIN_EXTERNAL_TABLE`，transient 不持久化/不 GSON 注册）。**T19**：`shouldUseNativeReader(forceJni,…)` gate（ro 仍 native、metadata 表经 non-DataSplit JNI）+ `buildTableDescriptor`→`HIVE_TABLE`（同修 [DV-024]）+ `PluginDrivenScanNode` fail-loud 拒 sys 表 scan-params/time-travel。**T20**：`beginQuerySnapshot/getSnapshotAt/getSnapshotById`（snapshot seam，sys→`Optional.empty`，空表→-1，SPI 契约 empty-if-none vs legacy throw 已 doc）+ `PaimonConnector.getCapabilities`=`SUPPORTS_MVCC_SNAPSHOT/TIME_TRAVEL`。
- **3-lens final holistic review**：PARITY=READY、SCOPE=READY、ADVERSARIAL=1 **BLOCKER**——`PluginDrivenScanNode.create` 直调 `metadata.getTableHandle(remoteName)` **绕过** seam → sys 表得普通 handle（forceJni=false）→ binlog/audit_log 走 native 静默错行（inert today，翻闸后 live）。**已修**：`create` 改走 `table.resolveConnectorTableHandle(session,metadata)`（普通表字节等价、sys 表得 sys handle），TDD red→green。其余 MINOR 已接受/出范围（`force_jni_scanner` session var=B2 边界；SDK-delegation 仅 live-e2e 可验；"Plugin" vs "Paimon" 文案；META-INF/ 勿提交）。
- **验证（主线 firsthand）**：import-gate 0；连接器 `Tests run: 124, Failures: 0, Errors: 0, Skipped: 1`(live)；fe-core `PluginDriven*Test` 98-100 绿；checkstyle 0；**无 cutover/B5 泄漏**（paimon 未入 `SPI_READY_TYPES`、GsonUtils/CatalogFactory/PhysicalPlanTranslator 零改、`PluginDrivenExternalTable` 仍非 MvccTable）。**唯一 fe-connector-api 改动 = T16 两 default no-op**。**B4 未提交**（用户控）。
- **B5/翻闸 reconcile（dormant）**：① T20 inert，B5 须落 `PluginDrivenExternalTable`→MvccTable + `beginQuerySnapshot` 调用 + `ConnectorMvccSnapshotAdapter` 构造；② T19 sys-table fail-loud guard 依赖 B5 把 scan-params/snapshot 接到 `PluginDrivenScanNode`（现可能 dormant）；③ `buildTableDescriptor` HIVE_TABLE + MVCC SDK-delegation（DataTable cast/`earlierOrEqualTimeMills`/`tryGetSnapshot`）须 live-e2e 验。

### 2026-06-10（B3 实现：DDL metadata，T11-T15；understand workflow 纠偏 1 处 → 用户签 D7=B；subagent-driven 3 dispatch + 双审 + 3-lens final holistic = 全 READY）
- **6-agent understand workflow + 主线 firsthand 核读**：T11-T15 plan 备注大体与 code 一致；**纠偏 1 处** → 用户签 **D7=B**（DDL authenticator = legacy parity，每 DDL call 包 `executeAuthenticated`，见开放决策 D7）。另**证实** plan「PluginDrivenExternalCatalog 已 override FE 侧」为真（FE 4 个 DDL 分发 createTable:300/createDb:355/dropDb:387/dropTable:439 已通用接 SPI，MC 已证；闸是 `SPI_READY_TYPES` 成员=B7，非 B3 缺口）。understand workflow 中 2 个 agent 返回退化 stub，由其余 4 agent 全覆盖（cross-verified）。
- **D1（T11+T12）**：`PaimonTypeMapping.toPaimonType(ConnectorType)`（reverse 方向，byte-parity `DorisToPaimonTypeVisitor.atomic`，gap→`DorisConnectorException`，map-key `.copy(false)`，struct id `AtomicInteger(-1)`）+ 新 `PaimonSchemaBuilder.build(request)`（port `toPaimonSchema`，2 故意 safer 偏差：comment fallback、PK drop-blank；非-identity transform→throw）。20 新测。
- **D2（seam + auth + T13）**：`PaimonCatalogOps` 加 4 DDL 方法（+ `CatalogBackedPaimonCatalogOps` delegations + `RecordingPaimonCatalogOps` fake，paimon `Catalog` 签名经 javap 核）；**thread `ConnectorContext` 入 `PaimonConnectorMetadata`（3-arg ctor，无 2-arg；`PaimonConnector.getMetadata` 传 context）**；`createTable`（override request-overload，`PaimonSchemaBuilder` build 在 wrap 外 → schema-fail raw）+ `dropTable`（handle-based idempotent）各包 `executeAuthenticated`，异常→`DorisConnectorException`；read 路径**不** wrap。9 DDL 测（含 2 个 failAuth auth-wrap mutation 测 + schema-build-fail-propagation 测）。
- **D3（T14）**：`supportsCreateDatabase=true` + `createDatabase`（HMS-only-props gate，flavor 读注入 `catalogProperties`，gate 在 auth 前；ignoreIfExists=false）+ 4-arg `dropDatabase(force)`（enumerate-loop + native cascade 整体一个 auth scope）。11 DB-DDL 测（含 force-cascade 顺序、force-空库、HMS-gate 3 例、auth-wrap）。
- **验证（主线 firsthand 复跑）**：`Tests run: 96, Failures: 0, Errors: 0, Skipped: 1`（1=live）+ BUILD SUCCESS + checkstyle 0 + import-gate 0 + **无 fe-core/fe-connector-api/fe-connector-spi 改动**（B3 纯连接器侧，FE override 已通用存在）+ 无 B7 cutover 泄漏（SPI_READY_TYPES/GSON/pluginCatalogTypeToEngine/PhysicalPlanTranslator 未碰）。每 dispatch implement→spec-review→quality-review 双审（均 mutation-verified），final 3-lens holistic（parity / adversarial / scope-build）= 全 READY，仅 NIT（已修 `PaimonSchemaBuilder` 类 javadoc「byte-for-byte」过度声明 → 「functional port + 2 documented divergences」）。
- **B3 改动未提交**（用户决定何时 commit）。B2+B3 改动同处 dirty tree（B2 normal-read 仍未提交）。

### 2026-06-10（B2 实现：normal-read，T06-T10；subagent-driven 3 dispatch + 双审 + final holistic = READY）
- **6-agent understand workflow + 主线 firsthand 核读** 纠偏 **3 处 plan 前提** → 用户签 D4/D5/D6（A/A/A，见开放决策）：T07 非 session-TZ、T08/T09 minimal/safe、T10 cache 延后。
- **T06（BLOCKER）**：`PaimonScanPlanProvider` 加 `PaimonCatalogOps` 注入（`getScanPlanProvider` 镜像 `getMetadata:86`）+ 包私 `resolveTable`（transient null→`catalogOps.getTable` 重建，byte-identical `getColumnHandles:160-171`），planScan + getScanNodeProperties **两 site 都护**（recon 只点 :95，复审补出 :204 同 NPE）。2 直测 `resolveTable`（`FakePaimonTable.newReadBuilder` 抛 → 不能端到端跑 planScan，测 helper 直接）。
- **T07（parity-correct TZ）**：拆 NTZ/LTZ——`TIMESTAMP_WITHOUT_TIME_ZONE` 保固定 UTC（= legacy `PaimonValueConverter:149` GMT；session-TZ 会移位 vs paimon UTC min/max stats → 假裁剪丢行）、`TIMESTAMP_WITH_LOCAL_TIME_ZONE` 改 `return null`（= legacy 无 `visit(LocalZonedTimestampType)` → 不下推，修当前 over-push）；加 `supportsCastPredicatePushdown=false`（镜像 MC:331-334）。新 `PaimonPredicateConverterTest`（NTZ 值钉 UTC millis / LTZ·FLOAT·CHAR 不推 / INT control）+ cap 测。
- **T08（partition listing，dormant）**：扩 seam `listPartitions(Identifier)`；实现 `listPartitionNames/listPartitions/listPartitionValues`（legacy `generatePartitionInfo:169-187` byte-parity：spec 迭代 + legacy-name DATE 经 **paimon `org.apache.paimon.utils.DateTimeUtils.formatDate`**（legacy 同款，无漂移）、DATE 检测经 `DataTypeRoot.DATE`；6-arg `ConnectorPartitionInfo` raw spec values + `lastFileCreationTime`/`recordCount`/`fileSizeInBytes`；filter 忽略 doc）。共享 `collectPartitions`+`resolveTable`（`getColumnHandles` 重构复用，byte-identical，B0 测仍绿）。**`getProperties` 不实现**（纠偏：fe-core 零消费方 + MC 不 override + 凭据泄漏 → 留 emptyMap stub）。新 `PaimonConnectorMetadataPartitionTest`(5) + 扩 `FakePaimonTable.options()`/`RecordingPaimonCatalogOps`。
- **T09**：`PaimonScanPlanProvider` 类 Javadoc 钉纯谓词裁剪（6-arg `planScan` 故意不 override；EXPLAIN partition=0/0 = 已知 B5 显示 gap）。**T10**：D6 决策 + 文档（cache + invalidation SPI 延后 B8），无 code。
- **验证（主线 firsthand 每任务复跑）**：`Tests run: 56, Failures: 0, Errors: 0, Skipped: 1`（1=live）+ checkstyle 0 + import-gate 0 + 无 fe-core 改动 + `partition_keys` schema key 未翻。每任务 implement→spec-review→quality-review 双审 PASS（spec/quality 均 mutation-verified），final holistic review = READY。
- **B5 reconcile 项（非 B2 风险，dormant）**：`listPartitionValues` 返 **RAW** spec 值（如 DATE epoch-day `19723`），legacy `partition_values()` TVF 返 **RENDERED**（`2024-01-01`，经解析 partitionName）；B5 接 TVF + 翻 `partition_columns` key 时须核 raw-vs-rendered 一致性。

### 2026-06-09（B1 实现：flavor 装配，全 5 flavor，单 Catalog）
- **用户签字 all-5-flavors**（非分阶段）；内部 2-dispatch 落地（dispatch1=offline core+paimon-core 3 flavor 活线；dispatch2=hms/dlf 活线+pom deps），每 dispatch implement→spec-review→quality-review→fix-loop→re-review，主线 firsthand 复跑构建。
- **T04**：`PaimonConnectorProperties` 加全 flavor key 常量（HMS/REST/JDBC/DLF，多别名 `String[]`）。
- **T05**：`PaimonConnectorProvider.validateProperties` override → `PaimonCatalogFactory.validate`（flavor 合法性 + 每-flavor 必需键 fail-fast：全 flavor warehouse / hms uri / jdbc uri+driver_class-if-driver_url / rest dlf-token requireIf / dlf ak·sk + endpoint-or-region）。
- **T03**：新 `PaimonCatalogFactory`（纯 `buildCatalogOptions` flavor→`metastore` 映射[filesystem/hive/rest/jdbc] + 每-flavor opts + `paimon.*` 透传排除 storage 前缀；纯 `buildHadoopConfiguration`/`buildHmsHiveConf`/`buildDlfHiveConf`；`requireOssStorageForDlf`）；`PaimonConnector` 线程 `ConnectorContext`，`createCatalog` 全 5 flavor 活线（filesystem/jdbc=Options+Configuration，rest=Options-only，hms/dlf=HiveConf；全 `context.executeAuthenticated` 包裹；JDBC DriverShim 经 `getEnvironment` 替 `JdbcResource`）。
- **pom**：加 `paimon-hive-connector-3.1` + `hadoop-common` + `hive-common`（compile，managed 版）；**弃 hive-catalog-shade** 避 fastutil 冲突（[[catalog-spi-fastutil-hive-shade-classpath]]）。
- **2 新 blocker 已解（非 plan 预见）**：① JDBC `JdbcResource` 禁 import → `ConnectorContext.getEnvironment()`(`jdbc_drivers_dir`/`doris_home`)；② storage `Configuration` 由 fe-core `StorageProperties` 构建（禁）→ minimal 重建（`fs.*`/`dfs.*`/`hadoop.*` + `paimon.s3.*`→`fs.s3a.` normalize）。
- **验证（主线 firsthand）**：`Tests run: 43, Failures: 0, Errors: 0, Skipped: 1`（PaimonCatalogFactoryTest 31 + ConnectorMetadataTest 9 + serde 2 + 1 live skip）+ BUILD SUCCESS + checkstyle 0 + import-gate 0。final holistic review = READY。
- **纠偏**：recon「rest Options-only 无 warehouse 必需」证伪——legacy `AbstractPaimonProperties` warehouse `@ConnectorProperty` required 默认 true 且 rest 未 override → rest 同样必需 warehouse（已改齐 legacy parity）。
- **翻闸(B7)硬门新增（详见「风险/开放问题」+ parity doc；live-e2e 必验，pre-cutover 离线不可测）**：① hms/dlf Thrift metastore client（`IMetaStoreClient`/`HiveMetaStoreClient`，DLF `ProxyMetaStoreClient`）连接器**不打包**，翻闸时由 FE host `hive-catalog-shade` 提供 + plugin child-first 下 `Configuration`/`HiveConf` 跨 loader 身份隐患须验（无 NoClassDefFound/LinkageError/ClassCastException）；② jdbc `driver_url` 的 FE 安全 allow-list（white-list/secure-path/jar 名校验）未接（须经 `ConnectorContext` hook，paimon 未入 SPI_READY_TYPES 故未触达）；③ HMS 外部 hive-site.xml 文件加载延后（legacy 用 fe-core `CatalogConfigFileUtils`，连接器禁 import）；④ 每-flavor live `createCatalog` + jdbc driver 注册离线不可测。

### 2026-06-09（B0 实现：测试基建 + parity baseline）
- **T01**：抽 `PaimonCatalogOps` 注入式 seam（5 读方法，B0 只读）over 远端 Catalog；`PaimonConnectorMetadata` 6 调用点齐迁（读路径字节级不变，`Catalog` import 仅留两 NotExist catch）；`PaimonConnector` 装配；建测试模块 = no-mockito `RecordingPaimonCatalogOps` + `PaimonConnectorMetadataTest`（9 UT，钉 `databaseExists` try/catch 与 `getColumnHandles` reload-fallback，各带 WHY+MUTATION）+ `FakePaimonTable`（28 非读方法 fail-loud）+ env-gated `PaimonLiveConnectivityTest`。
- **T02**：① R-007 三方版本已对齐（`${paimon.version}=1.3.1` 单源 `fe/pom.xml:399`，FE 连接器 + BE paimon-scanner + preload-extensions 同源）→ 落不变式注释（非改版本）。② offline FE→BE serde round-trip smoke `PaimonTableSerdeRoundTripTest`：真 `FileSystemCatalog`/`LocalFileIO`@TempDir → 真 `FileStoreTable` → 连接器 encode（InstantiationUtil+STD Base64）→ BE-side decode（镜像 `PaimonUtils.deserialize` URL-first/STD-fallback）→ 断 rowType/partition/primary keys；CI 跑非 env-gated。③ parity-baseline doc [`research/p5-paimon-parity-baseline.md`](../research/p5-paimon-parity-baseline.md)：33 p0 + 6 p2 + 3 MTMV + fe-core `PaimonScanNodeTest` 清单、翻闸自动 before/after 门模型、4 真 gap + live-e2e 计划。
- **验证**：连接器 `Tests run: 12, Failures: 0, Errors: 0, Skipped: 1`（1 skip=live）+ BUILD SUCCESS + checkstyle 0 + import-gate 净（主线 firsthand 复跑）。每任务 spec+quality 双审 PASS；主线追加 3 处准确性修正（beDecode 镜像 BE 真分支 / 第二测试重命名 / doc §3.1 legacy 已有 fe-core UT）后复绿。
- **纠偏**：recon「无 parity baseline」证伪——41 套回归已存在且翻闸自动变 after 门，真 gap 是连接器侧 UT（谓词转换/native·deletion/sys-forced-JNI）+ live-e2e。

### 2026-06-09（recon + 设计，0 产线代码）
- 14-agent code-grounded recon + cross-cut 对抗复审；产 `research/p5-paimon-migration-recon.md` + 本 doc。
- firsthand 核实 4 个 load-bearing 锚点（SPI_READY_TYPES / GSON 7 注册 / PluginDrivenExternalTable 无 MTMV / ConnectorPartitionInfo.lastModifiedMillis 存在）。
- 证伪 3 个先验：① Base64-blocker（BE 有 STD fallback `PaimonUtils:42-47`）② FE-dispatch-全缺（DROP/CREATE·DROP DB/SHOW PARTITIONS/TVF 已部分预接）③ 6-flavor-工厂-已建（backend 模块空壳）。
- **用户签字 D1=A（单 Catalog + flavor switch）、D2=A（P5 内实现 MTMV/MVCC 桥）**；不再阻塞，可启动 B0→B9。

---

## 关联

- Master plan 章节：[§3.6](../00-connector-migration-master-plan.md)
- RFC 章节：[§（写/事务 SPI）](../01-spi-extensions-rfc.md)、`tasks/designs/connector-write-spi-rfc.md`
- 样板：[P4 maxcompute](./P4-maxcompute-migration.md)（full-adopter + cutover）；recon `research/p4-maxcompute-migration-recon.md`
- 决策：**D-037（P5-D1 flavor=单 Catalog + switch，本 doc §开放决策 D1）**、**D-038（P5-D2 MTMV/MVCC P5 内实现，本 doc §开放决策 D2）** 已签字；D-005（HMS flavor 走 tableFormatType）、D-006（cache 放连接器内）
- 风险：R-004（classloader）、R-007（FE/BE 共享 jar）、R-012（snapshotId 类型）
- 连接器：[paimon](../connectors/paimon.md)
- recon：[p5-paimon-migration-recon](../research/p5-paimon-migration-recon.md)
- parity baseline（T02，翻闸前后 before/after 门 + gap 清单 + live-e2e 计划）：[p5-paimon-parity-baseline](../research/p5-paimon-parity-baseline.md)

---

## 当前阻塞项

- **无硬阻塞**。B0–B7（迁移 + 翻闸）+ P6 clean-room review 全部 deviation fix **已合入 `branch-catalog-spi`（#64446 / `38e7140ce56`，+ `e9c5b3e70ce` 修编译）**。翻闸 live-e2e（5-flavor，B7 硬门）已随合入跑过。**下一 session = P5-T29（B8 删 legacy + maven 依赖）+ P5-T30（B9 回归）**，见上文 §P5-T29 执行计划。
- **唯一须先对齐的事**：P5-T29 §D 的 **maven scope 方案 A vs B**（fe-core 是否完全 paimon-free；建议先 AskUserQuestion）。其余按 §A–E checklist 逐子树删 + 每批跑编译即可。
- 复用资产（删除时勿误删）：fe-core 通用桥 `PluginDrivenMvccExternalTable`/`PluginDrivenSysExternalTable`/`PluginDrivenSysTable`/`NativeSysTable`（未来 iceberg/hudi 复用）；连接器侧 `PaimonCatalogFactory`/`PaimonCatalogOps`/`PaimonTableResolver`/`PaimonTypeMapping`/`PaimonSchemaBuilder`/测基建——这些都在 `fe-connector-paimon`，**不是**删除目标。STILL-CONSUMED `property/metastore/Paimon*`（fe-core）保留。
