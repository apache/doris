# P5 — paimon 迁移（full adopter + 翻闸；复用 P4 写/事务 + cutover 样板）

> 设计 doc。事实底座见 `research/p5-paimon-migration-recon.md`（14-agent code-grounded recon + cross-cut 对抗复审）。
> 本 doc 含：old→new 映射、批次计划、有序 TODO、**开放决策（待用户签字）**。维护规则见 [README §4](../README.md)。

---

## 元信息

- **状态**：⏸ 待启动（recon+设计完成；**D1/D2 已签字 2026-06-09**，可启动分批实现）
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
| P5-T01 | 建 `fe-connector-paimon` 测试模块 + 注入式 SDK seam（`PaimonCatalogOps` 接口包远端 Catalog 调用，MC `McStructureHelper` 范式，no-mockito recording fake）| B0 | C+T | ⏳ | 0 测试现状 |
| P5-T02 | parity baseline（vs 旧 `PaimonScanNode`：谓词/分区/native·JNI/deletion/SELECT*）+ FE→BE round-trip smoke + **pin paimon-core 版本三方对齐** | B0 | T | ⏳ | 翻闸前后跑 |
| P5-T03 | `PaimonConnector.createCatalog` flavor 装配（switch on `paimon.catalog.type`：warehouse/options/重建 Hadoop·HiveConf/**每-flavor ExecutionAuthenticator**；filesystem→hms→rest/jdbc/dlf 渐进）| B1 | C | ⏳ | **gated on D1**；authenticator 丢=Kerberos DDL 炸 |
| P5-T04 | 拷 HMS/REST/DLF/JDBC + credential/storage 属性键入 `PaimonConnectorProperties`（禁 import fe-core）| B1 | C | ⏳ | |
| P5-T05 | 扩 `PaimonConnectorProvider.validateProperties`（flavor 合法性 + 每-flavor 必需属性，`IllegalArgumentException` fail-fast）| B1 | C | ⏳ | legacy `PaimonExternalCatalogFactory:29-47` |
| P5-T06 | 修 `PaimonTableHandle` transient-Table **reload fallback**（transient null 时由 `catalog.getTable(Identifier)` 重建）；`PaimonScanPlanProvider:95` 调用 | B2 | C | ⏳ | **BLOCKER** |
| P5-T07 | `PaimonPredicateConverter` session-TZ 化（读 `getTimeZone()` 惰性解析+降级，替 `:284` 固定 UTC）；不可转降级空；`supportsCastPredicatePushdown()=false`；保 FLOAT/CHAR 不下推 | B2 | C | ⏳ | [[catalog-spi-connector-session-tz-gotcha]] |
| P5-T08 | 实现 `PaimonConnectorMetadata.listPartitionNames/listPartitions/listPartitionValues`（填 `ConnectorPartitionInfo` 含 lastModifiedMillis=`Partition.lastFileCreationTime()`，partitionName=最终 legacy-name 解析后显示名）+ `getProperties`（现 stub `:154`）| B2 | C | ⏳ | 喂 `getNameToPartitionItems:246` 裁剪 + MTMV |
| P5-T09 | override 6-arg `planScan(...requiredPartitions)` 让引擎分区裁剪生效（`PluginDrivenScanNode:474`），OR 文档化纯谓词裁剪 + 测 | B2 | C | ⏳ | 现只 override 4-arg |
| P5-T10 | 连接器内 cache 已解析 Table+schema（替 `PaimonExternalMetaCache`）；核 REFRESH CATALOG 经 `PluginDrivenExternalCatalog` 销毁 connector（`:530-534`）是否够，否则提 `invalidateTable` SPI；核 REFRESH TABLE seam | B2 | C | ⏳ | 见开放问题 |
| P5-T11 | `PaimonTypeMapping` 加 Doris→paimon 方向（吃 ConnectorType；保留 legacy gap：无 TINYINT/SMALLINT/LARGEINT/TIME、char→VarChar(MAX)、DATETIME→plain Timestamp）| B3 | C | ⏳ | `DorisToPaimonTypeVisitor:81-108` |
| P5-T12 | `PaimonSchemaBuilder`（ConnectorCreateTableRequest→paimon Schema：primary-key/comment/location→CoreOptions.PATH、partitionKeys from IDENTITY spec；bucket 经 options passthrough）| B3 | C | ⏳ | DISTRIBUTE BY 禁(`CreateTableInfo:793`) |
| P5-T13 | 实现 `createTable`/`dropTable`（远端 + per-flavor authenticator；保留 latent remote-vs-local 名 bug 不修）| B3 | C | ⏳ | `PluginDrivenExternalCatalog` 已 override FE 侧 |
| P5-T14 | 实现 `supportsCreateDatabase=true`+`createDatabase`（HMS-only-props gate 读 `session.getCatalogProperties()`）+`dropDatabase(force)` enumerate-loop | B3 | C | ⏳ | MC parity `:466/478` |
| P5-T15 | DDL 离线 UT（createDb gate / dropDb force 级联 / createTable schema / IF NOT EXISTS / type gap）| B3 | T | ⏳ | |
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
- **开放｜JDBC flavor DriverShim/URLClassLoader** 在 parent-first 连接器 loader 下的归属。

---

## 阶段日志（倒序）

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

---

## 当前阻塞项

- 无硬阻塞（D1=A / D2=A 已签字 2026-06-09）。下一 session 可启动 B0（测试基建 + parity baseline，无前置）、B1（flavor 装配，单 Catalog 模型）、B6（procedure doc no-op）。
- 翻闸（B7）仍 gated on B2+B3+B4+B5 全完 + live e2e（用户真实 paimon 各 flavor 环境）。
