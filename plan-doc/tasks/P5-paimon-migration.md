# P5 — paimon 迁移（full adopter + 翻闸；复用 P4 写/事务 + cutover 样板）

> 设计 doc。事实底座见 `research/p5-paimon-migration-recon.md`（14-agent code-grounded recon + cross-cut 对抗复审）。
> 本 doc 含：old→new 映射、批次计划、有序 TODO、**开放决策（待用户签字）**。维护规则见 [README §4](../README.md)。

---

## 元信息

- **状态**：🟢 进行中（**B3 已完成 2026-06-10**：T11-T15 DDL metadata，连接器 96/0/0/1 绿、checkstyle 0、import-gate 0、**无 fe-core/SPI/api 改动**、3-lens final holistic review=全 READY；D7 签字（authenticator=legacy-parity wrap each DDL call）。下一批 = B4 sys-tables E7 + MVCC E5（gated on B2+B3 全完，现满足）。B0/B1/B2 见阶段日志）
- **启动日期**：2026-06-09（recon+设计）
- **目标完成**：TBD（估时 ~5-6 周，含 D2-A 的 MTMV/MVCC 桥）
- **阻塞**：无（D1=A / D2=A 已签字）；分批实现按 B0→B9 启动
- **阻塞下游**：P5 是最后一个 lakehouse full-adopter 样板验证（E5/E6/E7/E10 首次落地）；其 SPI 新面（E7 sys-table hook、E10 MTMV 桥、E5 wiring）将被未来 iceberg/hudi 翻闸复用——设计错须二次迁移
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
- [ ] procedure：`CALL paimon.x` / `ALTER ... EXECUTE` 翻闸后仍报错（no-op 守护）；doc 钉死两假阳性。
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
| P5-T16 | **新 E7 SPI**：`ConnectorMetadata.listSupportedSysTables`(default emptySet) + `getSysTableHandle`(default empty)；保 MC/jdbc/es/trino 不受影响 | B4 | C | ⏳ | greenfield，签名须慎（被未来连接器复用）|
| P5-T17 | paimon 实现 E7：名取 `SystemTableLoader.SYSTEM_TABLES`；`getSysTableHandle` 走 4-arg `Identifier(db,tbl,"main",sysName)`；handle 带 sysName+forceJni；reload fallback | B4 | C | ⏳ | branch="main" 限制保留+文档 |
| P5-T18 | 通用 fe-core `PluginDrivenSysExternalTable extends PluginDrivenExternalTable`(报 PLUGIN_EXTERNAL_TABLE) + `NativeSysTable` factory；override `PluginDrivenExternalTable.getSupportedSysTables/findSysTable` 委托连接器 | B4 | C | ⏳ | 路由经 `PluginDrivenScanNode`，**勿报 PAIMON_EXTERNAL_TABLE** |
| P5-T19 | `PaimonScanPlanProvider` 加 forceJni 分支（binlog/audit_log + 非 DataTable sys 全走 JNI）+ 通用节点 fail-loud 拒 sys 表 scan-params/time-travel；核 BE sys-table `TTableDescriptor`(HIVE_TABLE?) | B4 | C | ⏳ | binlog/audit_log 走 native = 行错（静默）|
| P5-T20 | **首个 E5 消费者**：实现 `beginQuerySnapshot/getSnapshotAt/getSnapshotById`(返 `ConnectorMvccSnapshot(snapshotId)`，空表 -1)+声明 `SUPPORTS_MVCC_SNAPSHOT/TIME_TRAVEL`；sys 表不得透出 time-travel | B4 | C | ⏳ | |
| P5-T21 | **GAP-LISTPART-AT-SNAPSHOT**：listPartitions 加 at-snapshot 重载（按 pin 的 snapshotId 列分区）；连接器实现；默认保 latest 向后兼容 | B5 | C | ⏳ | 单-pin 不变式前提 |
| P5-T22 | fe-core `PaimonPluginDrivenExternalTable extends PluginDrivenExternalTable` implements MTMVRelatedTableIf+MTMVBaseTableIf+MvccTable；`loadSnapshot`（beginQuerySnapshot 定 snapshotId + at-snapshot 物化分区集**一次**）| B5 | C | ⏳ | **gated on D2** |
| P5-T23 | 子类 MTMV 方法：getTableSnapshot(→MTMVSnapshotIdSnapshot,-1)/getPartitionSnapshot(→MTMVTimestampSnapshot,缺抛 AnalysisException)/getAndCopyPartitionItems(读 pin 非重列)/getPartitionType/getPartitionColumnNames/isPartitionColumnAllowNull(true)/beforeMTMVRefresh(no-op)/getNewestUpdateVersionOrTime(**绕 pin**) | B5 | C | ⏳ | |
| P5-T24 | rehome fe-core `PaimonMvccSnapshot`（包 `ConnectorMvccSnapshot` + fe-core 物化 name→PartitionItem/lastModifiedMillis/listed-count）；downcast 留 fe-core 内 | B5 | C | ⏳ | |
| P5-T25 | isPartitionInvalid parity（捕 listPartitions count vs 成功构建 PartitionItem count，size 不匹配→UNPARTITIONED 全表刷）；MTMV 单-pin 不变式测 + UT | B5 | C+T | ⏳ | |
| P5-T26 | **procedure DOC no-op**：连接器档 E2 改「NOTHING TO PORT」（非「后续」）；钉死两假阳性（Spark migrate_table / iceberg expire_snapshots）；记未来 seam 位置（`ExecuteActionFactory:59` + 可选 `ConnectorProcedureProvider`）；可选负回归（CALL/EXECUTE 仍报错）| B6 | D | ⏳ | 零 code |
| P5-T27 | **翻闸**：paimon 入 `SPI_READY_TYPES:52` + 删 built-in case `:142` + `pluginCatalogTypeToEngine` 加 `paimon→ENGINE_PAIMON`（`:937-944`）+ 删 `PhysicalPlanTranslator` PAIMON 分支(`:781`)+import(`:71`)| B7 | C | ⏳ | gated on B2-B5 |
| P5-T28 | **翻闸 GSON 原子**：5 catalog 名 + db + table 全转 `registerCompatibleSubtype`→PluginDriven*（table→`PaimonPluginDrivenExternalTable` 非裸 base）；加 5 flavor tag replay 测 | B7 | C+T | ⏳ | 漏 db→ClassCastException |
| P5-T29 | **删 legacy**：`datasource/paimon/`(28) + `metacache/paimon/`(3) + 反向引用；确认零引用；验 paimon-core FE classpath 恰一份（R-004/R-007 NoClassDefFound 守）| B8 | C | ⏳ | gated on 翻闸 live 验 |
| P5-T30 | post-cutover 回归：SHOW PARTITIONS + partitions TVF（预接 FE 分发现返行）/DROP·CREATE DB·TABLE/no-ENGINE CREATE/edit-log replay/MTMV 增量刷/sys-table/session-TZ 谓词不丢行 | B9 | T | ⏳ | |

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

- 无硬阻塞（D1=A / D2=A / D4=A / D5=A / D6=A / D7=B 已签字；**B0 + B1 + B2 + B3 已完成**）。下一 session = **B4**（sys-tables E7 + MVCC E5；gated on B2+B3 全完，现满足）：T16 新 E7 SPI（`listSupportedSysTables`/`getSysTableHandle` default-empty）/ T17 paimon 实现 E7 / T18 通用 `PluginDrivenSysExternalTable` / T19 forceJni 分支（binlog/audit_log）/ T20 首个 E5 消费者（`beginQuerySnapshot`/`getSnapshotAt`/`getSnapshotById`）。**注意 T16/T18 是 greenfield SPI 新面（被未来连接器复用，签名须慎）**。**B6**（procedure doc no-op，独立）可随时穿插。B5（MTMV 桥）gated on B4。
- 翻闸（B7）仍 gated on B2+B3+B4+B5 全完 + live e2e（用户真实 paimon 各 flavor 环境）。**翻闸/live-e2e 硬门**（见阶段日志 B1 条 + 「风险/开放问题」）：hms/dlf metastore-client 跨 loader、jdbc driver_url 安全 allow-list、hive-site.xml 文件加载、live createCatalog；**B3 新增 live-e2e 门**：DDL 的 `executeAuthenticated`（D7=B）在 Kerberized HMS/HDFS createTable/dropDatabase 正确性（离线 no-op，须 live 验）+ `lastFileCreationTime` 等 B2 dormant 项——pre-cutover 不可离线测，翻闸前用户须 live 验。
- 复用资产：`PaimonCatalogFactory`（纯 options/conf 构建器 + `resolveFlavor`，B3 createDatabase HMS-gate 已复用）；`PaimonCatalogOps` seam（现含 5 read + 4 DDL，B4 sys-table 可能再扩）；`PaimonTypeMapping`（双向）；`PaimonSchemaBuilder`；`RecordingPaimonCatalogOps`/`RecordingConnectorContext` 测基建（B4 复用）；parity doc 是后续批次 gap 清单 + 翻闸门基准。
