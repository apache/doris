# P7 — hive (+HMS) 迁移（最复杂、最后一个连接器；先在 fe-connector 实现完整能力 → 分子阶段翻闸 → 删 legacy）

> 阶段拆分 spec（phase-level plan），镜像 `P6-iceberg-migration.md`。各子阶段的逐 task 拆解（P7.x-Tnn）在**进入该子阶段时**做 code-grounded recon 后追加到本文件末尾。
> 本 spec 的"关键事实/映射/翻闸机制"来自 2026-07-05 的 10-agent code-grounded recon（工作流 `wf-p7-hive-recon` + 补充 type-coupling recon），已对照 HEAD 真实代码校正 HANDOFF/master-plan/connectors 里的过时数字。

---

## 元信息

| 项 | 值 |
|---|---|
| catalog type 名 | `hms`（`CATALOG_TYPE_PROP=hms`）|
| 目标模块 | `fe/fe-connector/fe-connector-hive/`（= "hms" 网关连接器）+ `fe/fe-connector/fe-connector-hms/`（共享元存储库）|
| fe-core 旧路径 | `fe/fe-core/src/main/java/org/apache/doris/datasource/hive/`（**52** 个文件：29 顶层 + `event/` 21 + `source/` 2）|
| 工作分支 | `catalog-spi-11-hive`（off `branch-catalog-spi` @ `8b391c7459d`）|
| PR base / 合并方式 | `branch-catalog-spi`，**squash**（复用 P5-T29 #64653 / P6 #64688 范式）|
| tracking issue | apache/doris#65185（PR 须引用）|
| 估时 | 6 周（master plan §3.8）|
| 主 owner | TBD |

---

## 阶段目标（终态）

1. `hms` catalog 走 SPI 路径：`CatalogFactory` 不再 `new HMSExternalCatalog`；catalog/db/table 退化为 `PluginDrivenExternalCatalog` / `PluginDrivenExternalDatabase` / `PluginDrivenExternalTable`。
2. fe-core **零** source-specific 代码：删净 `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名字符串判别（**85 处 occurrence / 33 文件** + 补充 recon 揪出的 ~7 处 type-level 耦合）。
3. fe-core 不解析任何 hive 属性（file-format/SerDe/ACID/staging/broker.name 全部移到插件；no-property-parsing 铁律）。
4. hive/hudi/iceberg-on-HMS 三格式经 **per-table SPI provider**（D-020）分流；hudi live cutover + 删 fe-core `datasource/hudi/`（D-019）并入本阶段。
5. 删除 fe-core `datasource/hive/` 整目录 + P6 遗留的 23 个 HMS-iceberg 支撑类 + `datasource/hudi/`。
6. ACID 写路径重写为连接器 `ConnectorTransaction`（E4），有独立集成测试作 gate（R-002，项目最大风险）。

---

## 关键事实（2026-07-05 code-grounded recon）

**规模（校正过时数字）**：`HMSTransaction` **1895** 行（plan 写 1866）、`HMSExternalTable` **1332** 行（plan 写 1293）、`HiveMetadataOps` 425 行、`MetastoreEventsProcessor` 357 行、`HiveTransactionMgr` 55 行、`HMSExternalCatalog` 250 行。反向 `instanceof/cast` = **85 occurrence / 33 文件**（plan 写 31）。fe-core test 引用 HMS 类型 = **22 文件**（迁移须预算）。

**连接器现状**：`fe-connector-hive`（12 主类）= **只读 scan 已立**（`HiveScanPlanProvider`/`HiveScanRange`/`HiveFileFormat`/`HiveTableFormatDetector`/`HiveTableType`/`HiveTextProperties`/handles）；`HiveConnectorMetadata` **override 了 0 个** DDL/txn/stats/partition SPI（全继承默认 throw）。`fe-connector-hms`（9 主类）= 共享读元存储客户端（`HmsClient`/`ThriftHmsClient`/`HmsClientConfig`/`HmsConfHelper`/`Hms*Info`/`HmsTypeMapping`）+ vendored `HiveVersionUtil` + `HiveMetaStoreClient` shim；**无写/txn/lock/col-stats 方法**。P3/P5/P6 已在用 `fe-connector-hms`（稳定）。

**HMS 是异构 catalog**：一个 `hms` catalog 下同时有 plain-hive（非 MVCC，时间戳新鲜度）+ iceberg-on-HMS / hudi-on-HMS（MVCC，snapshot 新鲜度）。今天靠 `HMSExternalTable` **单类 + 惰性探测 `dlaType`** + 处处 `switch(dlaType)`（recon 数出 ~19 个分支点）承载三者；`HMSDlaTable` 策略只抽出了 MTMV/partition 面，其余（schema/stats/rowcount/file-format/sys-table/mvcc/toThrift）仍是 `HMSExternalTable` 内联分支，**且 ICEBERG/HUDI 分支直接调 fe-core 的 `IcebergUtils`/`HudiUtils` 子系统 → 它们是那些 P6 遗留子系统的最后 live 消费者**（删除排序的核心约束，见下）。

---

## 已定架构（**勿重议**，实现即可 — 引 decisions-log）

| 决策 | 结论 | 对 P7 的含义 |
|---|---|---|
| **D-004** | HMS event pipeline 放 `fe-connector-hms`，经 `ConnectorMetaInvalidator` 回调 | P7.2 把 21 event 类 + processor 搬入 hms 库 |
| **D-005** | hive/hudi/iceberg-on-HMS 用 `ConnectorTableSchema.tableFormatType` 区分（值 `"HIVE"`/`"HUDI"`/`"ICEBERG"`，连接器探测后填充）| P7.4 DLA 退化；tableFormatType 作 opaque 串逐字上报、**fe-core 热路径不读**（不得 `if(format==...)`）|
| **D-020** | 单 `hms` catalog 多格式 scan 路由 = **方案 B（per-table SPI provider）**：`ConnectorMetadata` 新增向后兼容 default `getScanPlanProvider(handle)`（默认 null→回落 per-catalog）；注册 `"hms"` 的连接器（=`HiveConnectorProvider`/fe-connector-hive）override 之，按 `handle.getTableType()` **委派** Hudi/Iceberg provider | fe-connector-hive = "hms" 网关，**依赖 `-hudi`/`-iceberg` 模块**做委派；**否决**了"fe-core 发现期分派"和"hive 内嵌 iceberg/hudi SDK" |
| **D-019** | P3 hudi hybrid 把 live cutover（fe-core 消费 tableFormatType per-table 分流 + gate flip `SPI_READY_TYPES` 加 hms/hudi + 删 legacy `datasource/hudi/` + 完整增量/time-travel）**推入 P7** | hudi 批 E 并入本阶段（P7.4/P7.5）|
| **D-003** | 旧 `*ExternalCatalog` 子类全部删除，不留中间形态 | `HMSExternalCatalog/Database/Table` 删除 |
| 事务模型（D-022/24/25 + "A 桥接"）| 连接器 `ConnectorTransaction` 为单一事实源；fe-core 通用写编排经 `PluginDrivenTransaction` 桥接 | P7.3 HMSTransaction 重表达于 E4，**不新增** ConnectorMetadata 写 SPI（写/stats/partition 方法留在 HmsClient，由 hive 的 ConnectorTransaction.commit 驱动）|

---

## 阶段拆分（P7.1 – P7.5，master plan §3.8 + recon 细化）

> 节奏：串行为主（P7.1 是地基，P7.4 依赖 P7.1/P7.3 的连接器能力齐备，P7.5 依赖全部）。每子阶段 = 独立 commit + build + test + checkstyle 0 + import-gate 净；**每轮完成即更新 HANDOFF + commit**。

### P7.1 — HiveMetadataOps 全功能搬迁（DDL / partition / statistics 写端）— 2 周
把 `HiveMetadataOps`(425) 的 create/drop db+table、rename、truncate、add/drop partition、column-stats 写回搬进 `HiveConnectorMetadata` + `HmsClient`（加写方法，或 hive-only 写子接口避免撑大 hudi 共享面）。E1 CreateTable（identity partition + bucket）、E8 col-stats 写回、E10 listPartitions。**no-property-parsing**：file_format/owner/bucket/DLF-guard/LIST-partition 校验全部移入插件（须确认 `ConnectorCreateTableRequest` 富到能重建 `HiveTableMetadata`，否则扩之）。

### P7.2 — event pipeline 搬入 fe-connector-hms（D-004）— 1.5 周
21 event 类 + `MetastoreEventsProcessor` 搬入 hms 库，经 `ConnectorMetaInvalidator` 交付失效。**核心 fork（见开放决策 OQ-EVT）**：保留事件驱动的**结构化 register/unregister**（需新 SPI seam 回 fe-core）还是**降级为纯 invalidate + 惰性 re-list**（只复用现有 invalidator，但改 master/slave 语义 + 丢预填缓存）。连带：poller loop 位置（全搬 vs fe-core 留薄 driver 管 master/slave + edit-log）、MetaId/`ExternalMetaIdMgr` 是否 HMS 还需要（`genIdByName` 确定性可能使其冗余）、partition-name 粒度失效（现 SPI 只带 values→降级为 table 级）、R-010 线程泄漏 + TCCL pin。

### P7.3 — HMSTransaction + 写路径重写（ACID，最难，R-002）— 2 周
`HMSTransaction`(1895) + `HiveTransactionMgr`(55) 重表达于 `ConnectorTransaction`（E4）+ hms 库的 txn/writeId/lock/commit 方法。**写路径是 6 文件耦合链**（须一起 retype `HMSExternalTable`→generic）：`BindSink.bindHiveTableSink` → `LogicalHiveTableSink` → `PhysicalHiveTableSink` → `PhysicalPlanTranslator:569 (new HiveTableSink)` → legacy `planner/HiveTableSink` → `HiveInsertExecutor`。读侧 ACID（delete-delta，`AcidUtil.getAcidState` + valid-write-ids）须移入插件（否则 txn 表静默错读——`HiveScanRange.populateTransactionalHiveParams` 现只填 dir 不填 fileNames，是 stub）。**reader-txn 生命周期缺 SPI seam**（现由 `QeProcessorImpl:210` 硬调 `Env.getHiveTransactionMgr` 的 query-finish 回调 + 共享锁无 heartbeat）——须加中立 per-query finish 回调或收进 scan-provider teardown（见 OQ-RTX）。**gate = 独立 ACID 集成测试套件**。

### P7.4 — DLA 分流改造 + iceberg/hudi-on-HMS 委派 + hudi live cutover（D-005/D-019/D-020）— 0.5 周（实际含 hudi 会更重）
`HMSExternalTable`→`PluginDrivenExternalTable`（plain，非 MVCC）/ iceberg-hudi-on-HMS 表经 per-table `getScanPlanProvider(handle)` 委派给 `-iceberg`/`-hudi` 连接器。异构 catalog 的**表类抉择**（见 OQ-HET）+ tableFormatType 须 thread 进 `PluginDrivenSchemaCacheValue`（现被 `toSchemaCacheValue` 丢弃）。31→85 处 instanceof/switch 的 planner/nereids/stats/tvf 侧改为中立 capability / per-table trait。plain-hive 的**时间戳新鲜度** MTMV（非 snapshot-id）须 `PluginDrivenMvccExternalTable` 支持（现 `getTableSnapshot` 只返 snapshot-id）。

### P7.5 — 删 fe-core datasource/hive + hudi + 23 HMS-iceberg 类 + 翻闸收口 — 0.5 周（实际更重）
gate flip + 删目录 + 删所有 instanceof + 常量搬迁 + GsonUtils 兼容（见翻闸机制）。**受删除排序约束**（见下）。

---

## 跨连接器删除排序（**本阶段最硬约束**）

`datasource/hive/` **不能删**，直到以下非-hive 消费者全部 retype 到 generic table（否则编译断）：

| fe-core 消费者 | 依赖的 hive 类 | 何时解绑 |
|---|---|---|
| `datasource/hudi/HudiUtils`（5 方法带 `HMSExternalTable` 参，:259–423，用 `getHudiClient`/`useHiveSyncPartition`）| HMSExternalTable | hudi 迁入插件（P7.4）|
| `datasource/hudi/HudiExternalMetaCache`（`findHudiTable` cast HMSExternalCatalog.getClient）| HMSExternalCatalog/Table | hudi 迁入插件（P7.4）|
| `datasource/hudi/HudiScanNode extends HiveScanNode`；`HudiSchemaCacheValue extends HMSSchemaCacheValue` | HiveScanNode/HMSSchemaCacheValue | hudi 迁入插件 → 决定共享基类是搬 hms 库还是随 hudi（OQ-SHARE）|
| `datasource/iceberg/source/IcebergHMSSource`（field+ctor `HMSExternalTable`，:30/:34）| HMSExternalTable | iceberg-on-HMS 走 per-table provider（P7.4）|
| `statistics/HMSAnalysisTask`（field + `setTable(HMSExternalTable)`）| HMSExternalTable | col-stats E8 中立化（P7.4）|
| `statistics/util/StatisticsUtil.getIcebergColumnStats(org.apache.iceberg.Table)` | iceberg SDK in fe-core | iceberg-on-HMS 走 iceberg 连接器（P7.4）|

**含义**：P7.4 必须把 hudi + iceberg-on-HMS + hudi-on-HMS 全部切到插件路径，P7.5 才能删 `datasource/hive/`。`fe-connector-hive` 依赖 `-iceberg`/`-hudi`（D-020）是委派前提。

---

## 翻闸机制（cutover mechanics，实测行号）

1. **CatalogFactory**：`SPI_READY_TYPES`（:50）加 `"hms"`；删 `case "hms"`（:133–134 `new HMSExternalCatalog`）+ import。iceberg/paimon 已是此形态（其 case 已删）。
2. **GsonUtils 兼容（元数据 image/edit-log 回放 HAZARD，3 factory）**：把 `registerSubtype` → `registerCompatibleSubtype`：
   - :366 `HMSExternalCatalog` → `PluginDrivenExternalCatalog`
   - :447 `HMSExternalDatabase` → `PluginDrivenExternalDatabase`
   - :471 `HMSExternalTable` → **plain** `PluginDrivenExternalTable`（hive **非 MVCC**，区别于 paimon/iceberg 的 `PluginDrivenMvccExternalTable`，:494）
   - **一条 `"HMSExternalTable"` 兼容行覆盖 hive+hudi-on-HMS+iceberg-on-HMS**（三者历史都持久化为同一 tag；格式判别 load 后由 tableFormatType 承接）。precedent：:388 `PaimonHMSExternalCatalog` / :399 `IcebergHMSExternalCatalog`。须加 hive gson-compat replay 单测（仿 `IcebergGsonCompatReplayTest`/`PaimonGsonCompatReplayTest`）。
3. **常量搬迁（删类前）**：`HMSExternalCatalog.BIND_BROKER_NAME="broker.name"`（`ExternalCatalog:1320` 基类读它，generic 路径，`DefaultConnectorContext:304` 也调）、`HIVE_STAGING_DIR`/`DEFAULT_STAGING_BASE_DIR`（`HiveTableSink:173`）→ 移插件/属性侧，基类 `bindBrokerName` 改读 fe-core-local 常量或经 properties thread。
4. **写路径 6 文件 retype**（P7.3）：见上。

---

## SPI 缺口（consolidated，按子阶段）

**P7.1**：ConnectorTableOps 加 `truncateTable`（default throw，hive override→HMS native truncate）；`ConnectorCreateTableRequest` 富化（bucket cols/count、LIST partition col names、per-col default、DLF flag）；force `dropDatabase` cascade 语义下沉连接器；写/stats/partition 方法**留 HmsClient**（不上 ConnectorMetadata），由 P7.3 的 txn 驱动。

**P7.2**：`ConnectorMetaInvalidator` 三缺口——(a) 结构化 register/unregister seam（或降级 invalidate，OQ-EVT）；(b) partition-**name** 粒度失效（现只带 values→降级 table 级）+ 批量（modified+added）；(c) master/slave 角色 + master 转发 + edit-log/MetaId（须 fe-core 留 driver 或加 ConnectorContext seam）。R-010 线程生命周期 + TCCL pin。

**P7.3**：reader-txn 生命周期回调（query-finish，中立、所有连接器可用，替 `QeProcessorImpl`→`Env` 硬耦合）；write-begin context（isOverwrite/fileType/writePath 进 `ConnectorWriteHandle.getWriteContext`，仿 iceberg `IcebergWriteContext`，finishInsert 折进 commit）；post-commit **选择性** partition 失效 + follower edit-log（确认 `invalidatePartition` 是否 fan-out edit-log，否则退化 full-table）；连接器共享 Executor（异步 rename）；ACID 读 delete-delta（dir+fileNames）；共享锁 heartbeat（OQ-LOCK，默认保持现状 no-heartbeat）。

**P7.4**：`tableFormatType` thread 进 `PluginDrivenSchemaCacheValue` + getter（**只用于 split 路由/capability 派生，不用于 planner 分支**）；异构 catalog per-table MVCC-vs-plain 表类（OQ-HET）；plain-hive 时间戳新鲜度（`Freshness.TIMESTAMP` + `MTMVMaxTimestampSnapshot` 语义）；per-table file-scan trait（top-N lazy-mat / nested-column-prune，hive 按 format、iceberg 无条件）；`SUPPORTS_SQL_CACHE` capability（否则 hive 翻闸后静默丢 SQL cache）；`SUPPORTS_SAMPLE_ANALYZE` capability；partition_values TVF 中立化（`Map<name,List<value>>` + capability gate）；ACID kind 作 `HiveTableHandle` field/capability（连接器持 `AcidUtils`）。

**P7.5**：`ExternalMetaCacheRouteResolver` 硬编 `instanceof HMSExternalCatalog→{hive,hudi,iceberg}`——加 capability 返回 cache-engine id 集（否则退化 default，静默断跨格式失效）。

---

## 验收门（per 子阶段，逐项细化在各子阶段 recon 时定）

- 编译 `BUILD SUCCESS`（fe-core 只依赖 fe-connector-api；连接器注意 optional-shade `HiveConf`/hadoop-common）。
- checkstyle 0（`mvn -pl :<art> checkstyle:check` **不带 -am**）。
- import-gate 净（`tools/check-connector-imports.sh`；HMS `HiveVersionUtil` 命中=误报，见 memory）。
- 连接器单测（无 Mockito，真 fake/recording）+ fe-core 单测（Mockito）。
- **P7.3 硬门**：独立 ACID 集成测试套件（INSERT INTO / INSERT OVERWRITE / 分区写 / delete-delta 读 / rollback / 多 FE 失效），R-002 缓解。
- 翻闸门：gson-compat replay 单测（老 image tag 反序列化）+ image 兼容回归。
- 端到端：docker 重部署类加载冒烟（TCCL split-brain，见 memory）。

---

## 开放决策（待各子阶段 recon 后**用户签字**；此处附推荐，勿在本 session 拍板）

- **OQ-HET（P7.4，异构 catalog 表类）**：一个 hms catalog 混装 plain-hive（非 MVCC）+ iceberg/hudi-on-HMS（MVCC），而表的 Java 类今天在**注册进 cache 时**（读表前，为 SHOW TABLES 快）就定。方案 (a) 一律建 `PluginDrivenMvccExternalTable`、plain-hive 退化为 empty/timestamp 新鲜度；(b) 惰性到首读再定类/行为。**推荐 (a)**（避免 eager 全 catalog load；MVCC 类对 plain-hive 做 trivial no-snapshot）。
- **OQ-EVT（P7.2，事件模型）**：结构化 register/unregister（新 SPI seam）vs 纯 invalidate + 惰性 re-list（只复用现 invalidator，改 master/slave 语义 + 丢预填缓存）。**推荐**：pipeline 类搬插件，但 role-aware polling driver + edit-log 留 fe-core 薄壳（cleaner，少新 SPI）；结构事件优先降级 invalidate（惰性 reload），仅 partition-name 粒度确需扩 SPI。
- **OQ-RTX（P7.3，reader-txn 生命周期）**：加通用 per-query finish 回调（fe-core 为所有连接器调）vs 收进 scan-provider teardown。**推荐**：通用 query-lifecycle 回调（替 `QeProcessorImpl`→`Env` 硬耦合，其他连接器也可用）。
- **OQ-ACID-WRITE（P7.3，写 ACID 范围）**：今天 full-ACID **表**的 INSERT 是硬拒（`InsertIntoTableCommand:553`），非-ACID INSERT INTO/OVERWRITE 走 HMSTransaction，ACID **读**（delete-delta）支持。**推荐：迁移行为保持**——非-ACID 写 + ACID 读迁移到位，full-ACID 写**继续拒**（不在本阶段引入净新 ACID 写能力，控 R-002）。
- **OQ-SHOWCREATE（P7.4/P7.5，可见行为）**：hive `SHOW CREATE TABLE` 今天吐原生 Hive DDL（`HiveMetaStoreClientHelper.showCreateTable`）。加"render full DDL" SPI 逐字保 vs 接受 generic `Env.getDdlStmt`（可见输出变化）。**需产品签字**；推荐加 SPI 保持向后兼容。
- **OQ-SHARE（P7.4/P7.5，共享基类去向）**：`HiveScanNode`/`HiveSplit`/`HivePartition`/`HMSSchemaCacheValue` 被 hudi 继承。搬 `fe-connector-hms` 共享库并 re-point hudi，还是随各连接器复制。**推荐**搬 hms 共享库（单副本，对齐 D-004）。
- **OQ-LOCK（P7.3）**：读侧共享 HMS 锁无 heartbeat。**推荐**：迁移保持现状（不静默改），若加 heartbeat 单列。
- **OQ-COLSTATS（P7.1/P7.4，E8）**：hive col-stats 保 HMS-metadata 快路径（读 spark col-stats + NUM_ROWS，不扫描；需扩 ConnectorStatisticsOps 加 per-column 读）vs 降级为 generic SQL-based analyze（同 iceberg/paimon，简单但丢快路径）。**推荐**：扩 SPI 保快路径（hive 大表 analyze 性能敏感）。

---

## 阶段依赖 + 节奏

```
P7.1 (metadata 地基) ──→ P7.3 (写/txn，依赖 P7.1 的 HmsClient 写方法)
       │                      │
       └──→ P7.2 (event，可与 P7.1 并行) 
                              ↓
              P7.4 (DLA 退化 + hudi/iceberg-on-HMS 委派，依赖 P7.1/P7.3 连接器能力齐 + hudi 迁移)
                              ↓
              P7.5 (删 legacy + 翻闸收口，依赖全部 + 删除排序解绑)
```
每子阶段单独 PR？否——沿 P6 范式**整阶段一次 squash 合入**（未 push 铁律已随 #64688 解除，P7 走常规流程）。子阶段间在工作分支上串行 commit。

---

## 给下一个 agent 的 meta

- **起步 P7.1**：读 `HiveMetadataOps.java`(425) 全 public 面 + `HiveConnectorMetadata.java`（现 override 0）+ `HmsClient`/`ThriftHmsClient`；对照 gap（recon R4/R7 已列，但**信 HEAD 代码不信本 spec 行号**）。建 `P7.1-Tnn` 逐 task 拆解追加本文件。
- **recon 存档**：完整 10-agent recon 结构化结果 + 补充 type-coupling recon 在本 session 的 job tmp（`p7-recon-raw.json` / `p7-recon-digest.md`，**ephemeral**）；关键结论已蒸馏进本 spec + `connectors/hive.md`。若需重跑：`.claude/wf-p7-hive-recon.js`。
- **铁律复读**：fe-core 不得新增 `if(hive)`/`instanceof HMSExternal*`/引擎名判别（memory `catalog-spi-plugindriven-no-source-specific-code`）；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；跨边界 pin TCCL（memory `catalog-spi-plugin-tccl-classloader-gotcha`）；history_schema_info nested 名 lowercase（memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- **决策纪律**：D-004/005/019/020 + 事务桥接**已定勿重议**；OQ-* 到各子阶段 recon 后再用户签字（先中文讲背景+示例+推荐，不引任务代号）。

---

# P7.1 逐 task 拆解（code-grounded recon 完成 2026-07-06；3-agent + 直读 HEAD）

> **recon 结论（已对照 HEAD 校正 spec）**：
> - **通用桥 = `PluginDrivenExternalCatalog`**（非独立 MetadataOps；`metadataOps==null`）。它逐条 override DDL 命令，`connector.getMetadata(session)` 拿 `ConnectorMetadata`，并**内联** cache/editlog 记账（替代旧 `ExternalMetadataOps.afterXxx()`）。→ **fe-core 侧 cache/editlog 钩子留在桥里，连接器只实现纯 SPI 方法。**
> - DDL 流向（桥 → SPI）：CREATE TABLE→`createTable(session, request)`（db=remote 名、table=SQL 名）；CREATE DB→`createDatabase(session, dbName, props)`；DROP DB→`dropDatabase(session, remoteName, ifExists, force)`；DROP TABLE→`dropTable(session, handle)`（handle 预解析，viewExists 时走 dropView）；RENAME→`renameTable(session, handle, newName)`。**IF [NOT] EXISTS 桥侧（fe-core）判定**，SPI 方法不带该 flag。
> - **TRUNCATE = 硬缺口**：桥**未** override `truncateTable`，落到 base `ExternalCatalog.truncateTable`(:1353) → `metadataOps==null` 抛 "Truncate table is not supported"。SPI 上**根本无** `truncateTable` 方法。→ 须**加 SPI seam + 桥 override**。editlog `OP_TRUNCATE_TABLE`/`logTruncateTable(TruncateTableInfo)`/`replayTruncateTable` 已存在（复用；external 失效等价旧 `afterTruncateTable`=`refreshTableInternal(updateTime)`）。
> - **converter = `CreateTableInfoToConnectorRequestConverter.convert(info, dbName)`**：properties **逐字**拷贝（不 parse ✅）；columns/partitionSpec(style+fields)/bucketSpec(cols+count+algorithm)/comment/ifNotExists/external 均已带。**但丢两项**：(1) **每列默认值**恒传 `null`（注释："not exposed via public getter … until SPI gains a typed carrier"）——**破坏 hive `createTableWithConstraints` 列默认值路径 + DLF guard**；(2) **LIST/RANGE partition value 定义**恒空（`initialValues=[]`）——只留 partition 列名。bucket `algorithm` 桥给 `doris_default`/`doris_random`（非 `hive_hash`；插件按此判 hash-only 即可）。
> - **模板 = `IcebergConnectorMetadata`**（最接近：HMS-backed、DLF guard、location cleanup、插件侧 `IcebergSchemaBuilder.buildTableProperties(requestProps, catalogProps)` 装配、`context.executeAuthenticated` 包裹）。paimon 亦可参。**no-property-parsing 实践**：fe-core 从不看 property map，全插件侧解释。
> - **调用方分布（agent C）**：`updateTableStatistics`/`updatePartitionStatistics`/`addPartitions`/`dropPartition` = **仅 `HMSTransaction` 消费**（stats/partition 写是 INSERT-commit 后回写 HMS 行数/分区，非 DDL、非 ANALYZE）。→ **本阶段不搬这 4 个 + 不动 HMSTransaction**；它们的 HmsClient 写原语**推到 P7.3** 随 HMSTransaction 落地（否则 P7.1 加进来即死代码，违背 surgical/minimal）。ANALYZE 列统计走 `HMSAnalysisTask`（读 `getTableColumnStatistics` + Doris 内部统计表）= P7.4。
> - **rename 现状**：`HMSCachedClient` 无 rename/alterTable、`HiveMetadataOps` 无 renameImpl → **hive 今天不支持 ALTER TABLE RENAME**。→ 本阶段 `HiveConnectorMetadata.renameTable` **保持 SPI default throw**（等价现状），**不实现**。（翻闸后行为与今一致。实现前二次确认 `ExternalMetadataOps` rename 默认行为。）
> - **config 缺口**：DLF guard 插件可自足（`HmsClientConfig.METASTORE_TYPE_KEY="hive.metastore.type"` + `METASTORE_TYPE_DLF="dlf"` 已在，连接器已收到 catalog props）。但 `Config.hive_default_file_format`(="orc") + `Config.enable_create_hive_bucket_table`(=false) 是 **FE 全局 master-mutable Config、非 catalog 属性**，插件读不到、无等价物 → **见开放决策 OQ-HIVE-CFG（待用户签字）**。
> - **SPI 面已足**：`ConnectorSession.getUser()`（owner 默认）、`getCatalogProperties()`（DLF 判别）、`ConnectorColumn` 已带 `defaultValue/nullable/comment`、`ConnectorPartitionSpec.Style{IDENTITY,TRANSFORM,LIST,RANGE}`+fields、`ConnectorBucketSpec`(cols+count+algo)、`ConnectorSchemaOps` 已有 `supportsCreateDatabase()`/`createDatabase`/`dropDatabase(force)` seam。→ **除 `truncateTable` + 列默认值 carrier 外，SPI 无须扩。**

## 范围（本阶段 = **DDL 写路径**，不含 txn/stats 写原语；不翻闸 = 非 live，验收靠连接器单测）

| task | 模块 | 内容 |
|---|---|---|
| **P7.1-T01** | fe-connector-api + fe-core | **truncateTable SPI seam**：`ConnectorTableOps.truncateTable(session, handle, List<String> partitions)` default throw；`PluginDrivenExternalCatalog.truncateTable(...)` override（解析 handle→`metadata.truncateTable`→cache 失效 `refreshTableInternal(updateTime)` + editlog，复用 `OP_TRUNCATE_TABLE`；对齐 base `ExternalCatalog.truncateTable` 签名 `(db, tbl, PartitionNamesInfo, forceDrop, updateTime)`）。实现前二次确认 external truncate 的 editlog/binlog 路径细节。 |
| **P7.1-T02** | fe-connector-hms | **写 DTO（SPI-clean，仿 `HmsTableInfo`/`HmsDatabaseInfo` 风格，无 fe-core `Column`/`NameMapping`）**：table-create spec（db、table、location?、columns=`ConnectorColumn`、partitionKeys、bucketCols、numBuckets、fileFormat、comment、properties + 列默认值）；db-create spec（db、locationUri、comment、properties）。（partition/stats DTO 推 P7.3。） |
| **P7.1-T03** | fe-connector-hms | **converter（HiveUtil 等价，插件侧、fe-core-free）**：`toHiveTable`（storage-descriptor / serde / input-output-format per orc/parquet/text、compress、`tableType=MANAGED_TABLE`、`DORIS_VERSION` 常量本地化、owner/comment props、列默认值→`SQLDefaultConstraint`）、`toHiveDatabase`。**Doris→Hive 类型映射**：复用/反转 `HmsTypeMapping` 或加 `dorisTypeToHiveType` 等价（断 `HiveMetaStoreClientHelper.dorisTypeToHiveType`）。断耦：`ExternalCatalog.DORIS_VERSION`、`HiveProperties`、text 压缩 **session var**（`ConnectorSession.getProperty` 线程进）、`AnalysisException`。 |
| **P7.1-T04** | fe-connector-hms | **HmsClient 写方法（DDL only）**：`HmsClient` 接口 + `ThriftHmsClient`（走现成 `execute(...)` + auth）加 `createDatabase`、`dropDatabase`、`createTable`(含 default-constraint 分支)、`dropTable`、`truncateTable(db,tbl,List<String>)`。（addPartitions/dropPartition/updateTableStatistics/updatePartitionStatistics 推 P7.3。） |
| **P7.1-T05** | fe-connector-hive | **HiveConnectorMetadata DB DDL**：override `supportsCreateDatabase()=true`、`createDatabase`（插件侧解析 `location` prop）、`dropDatabase(force)`（force 时枚举 drop 表，仿 iceberg/paimon cascade）。 |
| **P7.1-T06** | fe-connector-hive | **HiveConnectorMetadata createTable**（模板 iceberg）：由 request + catalog props 装 table-create spec；**插件侧 parse**：file_format 默认（OQ-HIVE-CFG）、owner 默认（`session.getUser()`）、transactional 建表拒绝、DLF 列默认值 guard（catalog prop + 列默认值）、bucket gate（OQ-HIVE-CFG）+ hash-only（algorithm≠random）、LIST-only（style==RANGE 拒）、partition-value 表达式拒绝（converter gap，见 T08）。→ `hmsClient.createTable`。 |
| **P7.1-T07** | fe-connector-hive | **HiveConnectorMetadata dropTable + truncateTable**：`dropTable(handle)`（先 `getTable` 判 `AcidUtils.isTransactionalTable` 拒事务表）→`hmsClient.dropTable`；`truncateTable(handle, partitions)`→`hmsClient.truncateTable`。**renameTable 保持 default throw（不实现，parity）。** |
| **P7.1-T08** | fe-core（shared converter） | **converter 带上每列默认值**：给 `CreateTableInfo`/列定义加 public getter，`CreateTableInfoToConnectorRequestConverter.convertColumns` 把默认值填进 `ConnectorColumn.defaultValue`（现恒 null）。additive、对 iceberg/paimon 安全（它们忽略）。（partition-value 表达式拒绝：评估是否顺带 thread `initialValues` 或退化为静默忽略——实现时定，倾向最小改动＝插件按 style 判别 + 记录降级。） |
| **P7.1-T09** | 依 OQ-HIVE-CFG | **config threading**：按用户裁定落地 `hive_default_file_format` + `enable_create_hive_bucket_table` 进插件。 |
| **P7.1-T10** | fe-connector-hms/hive 单测（无 Mockito，recording fake IMetaStoreClient） | HmsClient 写方法（create/drop db+table、truncate）；HiveConnectorMetadata DDL（createTable 装配正确性：file_format 默认/owner/bucket gate/DLF guard/partition；dropTable 事务表拒绝；createDatabase location；dropDatabase cascade）；converter 列默认值。 |
| **P7.1-T11** | 守门 + 交接 | build SUCCESS（fe-core 仅依赖 fe-connector-api；连接器注意 optional-shade HiveConf/hadoop-common）+ checkstyle 0 + import-gate 净 + 单测过；更新 HANDOFF + commit。**非 live**（翻闸 P7.5），故本阶段无 e2e DDL。 |

## P7.1 开放决策（待用户签字）

## P7.1 实现进度 + T03 移植指针（recon 存档，勿再 recon）

- **T01 ✅ 已提交 `c0222977ebd`**：`ConnectorTableOps.truncateTable(session, handle, partitions)` default-throw + `PluginDrivenExternalCatalog.truncateTable`/`replayTruncateTable`（editlog `TruncateTableInfo` + `refreshTableInternal`；base 签名 = `truncateTable(String db, String tbl, PartitionNamesInfo, boolean forceDrop, String rawTruncateSql)`，`forceDrop`/`rawTruncateSql` 无 external 语义故忽略）。全绿。
- **T02+T03+T04 ✅ 已提交 `fdf577e7946`（插件写客户端，一个自洽单元）**，全在 `fe-connector-hms`：
  - **T02**：`HmsCreateTableRequest`（builder；columns=全列[数据+分区]、partitionKeys=名、bucketCols/numBuckets、fileFormat、comment、properties、`defaultTextCompression`、`dorisVersion`；列默认值走 `ConnectorColumn.getDefaultValue()`）+ `HmsCreateDatabaseRequest`（dbName/locationUri/comment/properties）。仿读侧 `HmsTableInfo`/`HmsDatabaseInfo` 风格，SPI-clean。
  - **T03**：`HmsTypeMapping.toHiveTypeString(ConnectorType)`（反向映射，等价 `HiveMetaStoreClientHelper.dorisTypeToHiveType`；switch 于 `PrimitiveType.toString()` 名——`ConnectorColumnConverter.toConnectorType` 之输出；VARCHAR→string、datetime 丢 scale、decimal p==0→9、不支持类型 throw）+ `HmsWriteConverter.toHiveTable/toHiveDatabase`（忠实移植 `HiveUtil` + `HiveProperties.setTableProperties` 的 serde/table 属性切分；orc/parquet/text 的 in/out/serde+压缩默认逐字照搬；`createTime=(int)millis*1000` 溢出照搬；MANAGED_TABLE、`doris external hive table` tag）。**断耦**：DORIS_VERSION_VALUE 经 request 线程进（插件不能 import fe-common `Version`）、text 压缩默认经 request（T06 从 session 取）、`AnalysisException`→`IllegalArgumentException`、JDK-only（无 guava/commons）。
  - **T04**：`HmsClient` 加 5 个写方法（createDatabase/dropDatabase/createTable/dropTable/truncateTable）为 **default-throw seam**（读侧 fake 不受影响）；`ThriftHmsClient` 用现成 `execute(...)`（auth+pool）override 全部，`createTable` 从列默认值建 `SQLDefaultConstraint`→`createTableWithConstraints`（否则 `createTable`），等价 legacy。
  - **验收**：fe-connector-hms compile SUCCESS + checkstyle 0（main+test）+ import-gate 净 + 28 单测绿（反向类型映射 + 转换器；client-dispatch/HiveConnectorMetadata 单测留 T10 recording-fake）。**无连接器 override，行为不变。**
- **下一单元 = T05→T07（`HiveConnectorMetadata` DDL override）+ T08（shared converter 带列默认值）+ T09（config 注入=方案A）+ T10（recording-fake 单测）+ T11（守门+交接）。** T06 需从 `ConnectorSession` 取 text 压缩默认填 request、并把 `doris.version`（值）与两个全局 Config 一起在注入 locus（T09）落地填 request；模板 = `IcebergConnectorMetadata`。**信 HEAD 控制流不信本 spec 行号。**
- **T03（converter，最精细）移植指针（HEAD 行号，信控制流）**：源在 `datasource/hive/HiveUtil.java` —— `toHiveTable`(:227)、`setCompressType`(:259)、`toHiveStorageDesc`(:286)、`setFileFormat`(:301)、`toHiveSchema`(:327)、`toHiveDatabase`(:346)。**必须断的 fe-core 耦合**：
  - `HiveMetaStoreClientHelper.dorisTypeToHiveType(Type)`（`HiveMetaStoreClientHelper.java:529`，walk scalar/array/map/struct → hive 类型串；char(len)/decimal(p,s)；date←DATE/DATEV2、timestamp←DATETIME/DATETIMEV2；VARCHAR/STRING→string）→ **插件侧要写 `ConnectorType` 版逆映射**（`HmsTypeMapping` 是正向 hive-str→ConnectorType，需补逆向；~80 行对称）。
  - `ExternalCatalog.DORIS_VERSION` / `DORIS_VERSION_VALUE` → 插件本地常量。
  - `HiveProperties.setTableProperties(table, props)` → 核实其语义（大概率 `table.setParameters`）后插件内联。
  - text 压缩默认 `ConnectContext.get().getSessionVariable().hiveTextCompression()` → 经 `ConnectorSession`（`getProperty`/session 属性）线程进，别在插件 import fe-core。
  - `AnalysisException` → `DorisConnectorException`/`IllegalArgumentException`。
  - format 表（orc/parquet/text 的 input/output format + serde，见 :301-325）+ 压缩默认（parquet→snappy、orc→zlib、text→session var）逐字照搬。
  - `toHiveTable` 里：`tableType="MANAGED_TABLE"`、`props.put(DORIS_VERSION,…)`、`props.put("comment", …)`、`owner`→`table.setOwner`、createTime=`(int)(System.currentTimeMillis()*1000)`（注意原代码此处有整型溢出但**照搬保持一致**）、sd.parameters 里 `tag="doris external hive table"`。
  - columns = **全列**（数据+分区），`toHiveSchema` 用 partitionKeys 名集切分数据列/分区列。

- **OQ-HIVE-CFG（config threading）—— ✅ 用户已裁定 2026-07-06 = 方案 A**：`hive_default_file_format`(默认 orc) + `enable_create_hive_bucket_table`(默认 false) 是 FE 全局 master-mutable Config、非 catalog 属性，插件读不到。**结论：方案 A** —— fe-core **建连接器时**把这两个全局 Config 的当前值**注入为连接器属性默认**（仅当用户未在 catalog 显式设置时；键名如 `hive.default.file.format`/`hive.enable.create.bucket.table`），插件只读属性。保留全局配置生效 + 允许 per-catalog 覆盖 + 插件不碰 `org.apache.doris.common.Config`，行为零回归。（备选 B＝纯 per-catalog 属性对齐 Trino、C＝硬编码，均**否决**：B 改用户可见行为、C 丢可调性。）→ 见 T09；注入点 = fe-core 组装 `hms` 连接器属性处（须 recon 具体 locus，勿在插件侧读 Config）。
