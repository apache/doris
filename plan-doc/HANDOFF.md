# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.5-T08 收口 ⇒ P6.5 DONE**

**T07（审计 + 2 现修 + 9 gap-fill）+ T07-续（残留 8 gap-fill）= ✅ DONE 且全验**。T08 = **收口汇总设计 + faithfulness 对抗验证 wf + gate 重跑 = P6.5 DONE**，然后 → P6.6 翻闸（全有或全无，须先 holistic 修 DV-038/041/045，见下）。

## T08 起步指引
- **汇总设计 doc**：仿 `designs/P6.3-T09-iceberg-write-summary-design.md` / `designs/P6.4-T09-procedure-summary-design.md` 模板 → 新 `designs/P6.5-T08-systable-summary-design.md`（sys-table SPI 全貌：`listSupportedSysTables`/`getSysTableHandle`〔LAZY，[D-063]〕/`loadSysTable`/`getTableSchema`·`getColumnHandles` sys 分支〔[D-064]〕/`planSystemTableScan`+`IcebergScanRange` serialized-split〔[D-065]〕/guard capability `supportsSystemTableTimeTravel`〔[D-067]〕/`buildTableDescriptor` hms↔iceberg fork + engine-name/SHOW-CREATE parity〔[D-066/067]〕；**dormant-至-翻闸激活集** + DV-048/049）。
- **faithfulness 对抗验证 wf**：verifier(refute-by-default) 核每条 doc claim 的行号/commit/UT 计数 + completeness critic。**UT 计数必重跑 surefire 实证**（Rule 12）：连接器全量 **543/0/1**、fe-core `PluginDrivenSysTableTest` **10/0**、`PluginDrivenScanNodeSysTableGuardTest` **8/0**、`IcebergScanPlanProviderTest` 67/0、`IcebergConnectorMetadataSysTableTest` 22/0。
- T08 **纯文档/审计，0 产品码预期**。完成即 **P6.5 DONE** → 更新 board（PROGRESS §一/§二 + tasks 表 + connectors/iceberg.md 完成度）P6.5 🔵→✅。**决策 A/B/[D-063..067] 已定，无需再问**。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完。翻闸前必修下述阻塞：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge`。**P6.5 元数据列挂靠** + 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。**P6.5 本族一项**：**sys-table 时间旅行 query→连接器 handle pin 的接线**——guard 已修不再 BLOCK〔[D-067]〕、连接器 T02/T05 已保留+honor pin，但 query `getQueryTableSnapshot()`/`getScanParams()`→`IcebergTableHandle` 的 snapshot/ref pin **的通用-路 threading 仍休眠**〔现仅 MVCC 路 `applyMvccSnapshotPin` 走 `metadata.applySnapshot`，非 iceberg `FOR TIME AS OF`〕→ 翻闸后 sys 时间旅行**完整 e2e** 须接此线 + P6.8 docker 验。

**[DV-045]**（写路径）= `rewrite_data_files` **执行半翻闸接线**（R-B 专门写路径 RFC，用户裁 Option 1 推后）。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager`。

**⚠️ P6.5 翻闸激活点（pre-flip 不激活，flip 后才生效）**：T06-F1 `PluginDrivenExternalTable.getEngine/getEngineTableTypeName` `case "iceberg"`〔续 8 gap-fill 已为 sys 路单独钉 engine/mysqlType〕 + T06-F2 `ShowCreateTableCommand` 解包〔F2 对 **live paimon** 立即生效=DV-048① priv 修〕 + **T07 guard `supportsSystemTableTimeTravel()`**〔iceberg→true 仅 flip 后命中；paimon→默认 false 不变〕 + T07 hms `equalsIgnoreCase`〔描述符，flip 后 EXPLAIN 激活〕。

**[pre-flip 行为偏差中央登记]**：P6.4 = DV-046/047。**P6.5 = DV-048〔correctness-bearing：F2 paimon priv loosening〔live〕+ serialized FileScanTask 字节潜伏（T05）〕/ DV-049〔perf-cosmetic：sys split-weight 丢〔镜像 DV-033〕+ 内部 TableType.PLUGIN_EXTERNAL_TABLE〔user-visible 已 parity〕+ position_deletes 文案〕**。**注**：T07 揭出的 sys 时间旅行 guard 拒绝 + hms 大小写**用户裁现修**〔[D-067]〕→ **非 DV**（消除偏差）。**T07-续 test-6 揭出的「sys 元数据列谓词非 FE 行裁」是 BE-applied residual = legacy parity，非 DV**（仅纠正 audit 的 test spec）。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}；翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**P6.4 T07/T08/T09 + P6.5 T01–T07 + T07-续 已 commit 未 push（11 commit；T07-续 = `git log` HEAD，本 session 提交）**。**用户未要求 push**——留用户裁量。工作树 tracked 干净（仅 4 测文件本 session 改、已 commit）。
- **P6.1–P6.4 = ✅ DONE**。**P6.5 = 🔵 进行中**（T01–T07 ✅〔含 T07-续〕；**T08 收口待** ⇒ 即 P6.5 DONE）。
- iceberg **不在** `SPI_READY_TYPES`（仍走 switch-case `:137 case "iceberg"`）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 = P6.5-T07-续（残留 8 gap-fill UT + mutation-check，待 commit）

- **8 gap-fill 全落 + 全验**：fe-core `PluginDrivenSysTableTest` +4〔sys `getMysqlType`="BASE TABLE"〔同测钉 new==legacy `ICEBERG_EXTERNAL_TABLE.toMysqlType`〕/ `getEngine`="iceberg"+`getEngineTableTypeName`="ICEBERG_EXTERNAL_TABLE"〔**assertAll** 两 pin 独立 mutation〕/ position_deletes 通用 not-found〔含 snapshots 正控〕/ 非注册不变式〔sys 类无 `@SerializedName` + discovery key 无 `$`〕〕 + guard +1〔guard-message 顺序：scan-params 先于 time-travel〕 + 连接器 `IcebergScanPlanProviderTest` +2〔sys predicate residual / sys split `/dummyPath`〕 + `IcebergConnectorMetadataSysTableTest` WHY-comment 修。
- **⚠️ test-6 实证纠 audit spec**：原 spec 设 sys 元数据列谓词「裁到 1 行」**实证为假**〔record_count=10 后 FE 行数 2 vs 2 不变〕——iceberg 元数据表**列**谓词是 **BE-applied residual**（`IcebergSysTableJniScanner` 读时应用），非 FE plan-time 行裁〔plan-time 裁是 snapshot pin，已另测〕。test-6 改钉 FE 可达观测 = 反序列化 `task.residual()` 携 `record_count`（无谓词 residual=`true`）。**非 legacy 偏差**（legacy 同 serialize FileScanTask 带 residual 给 BE）。**playbook 教训重申**：HANDOFF/audit 的 test spec 也可能基于错误前提，实证 > 文档。
- **WHY-comment 修（test 8）**：`getSysTableHandleNormalizesNameToLowercase` 原注释将大小写不敏感误归 resolution gate。实证：legacy resolution 是 case-SENSITIVE `TableIf.findSysTable:413` `Map.get` over `IcebergSysTable` 小写键〔`getTableNameWithSysTableName` 不 lower-case suffix〕→连接器 `equalsIgnoreCase` 是 production 永不达超集；`MetadataTableType.from` 大小写不敏感作用在 metadata-table BUILD 时〔`resolveSysTable:1132`〕非 resolution gate。
- **测改进**：test-2 由两 `assertEquals` 改 `assertAll`，使 getEngine/getEngineTableTypeName 两 pin 各被自身 mutation 捕获（否则 short-circuit 掩盖）。
- **mutation-check（Rule 9/12，全绿→红→`git checkout` 复原 IDENTICAL）**：fe-core 5 变异一次 build〔`TableIf` PLUGIN `toMysqlType`→null / 删 `getEngine`+`getEngineTableTypeName` iceberg case〔assertAll 双红〕/ 注入 position_deletes / `@SerializedName` on `sysTableName` / swap guard 两块→time-travel 先〕→ test1–5 全红 + 1 已知 collateral〔`testGetSupportedSysTablesDelegatesToConnector` size 2→3〕；连接器 2 变异〔sys 丢 filter→residual=`true` / 改 `SYS_TABLE_DUMMY_PATH` 常量〕→ test6/7 红。**生产文件全程 `git checkout` 复原**（本 session 仅改 4 测文件，生产 0 改）。
- **验证（重跑 surefire 实证，Rule 12）**：fe-core `PluginDrivenSysTableTest` **10/0/0** + guard **8/0/0**；连接器全量 `package -Dassembly.skipAssembly=true` **543/0/1**〔=541+2，0 回归〕；checkstyle 0〔两模块〕；import-gate exit 0；`CatalogFactory:51` 未改。**0 新 D/DV**。**live-e2e 未跑**（dormant，P6.8 兜底）。

---

# 🗺️ 代码脚手架（iceberg sys-table）

- **连接器**（dormant）：`IcebergTableHandle`〔sys 变体 + 保留 pin〕/`IcebergConnectorMetadata`〔`listSupportedSysTables`/`getSysTableHandle`〔LAZY〕/`getTableSchema`·`getColumnHandles` sys 分支/`loadSysTable`/`buildTableDescriptor` hms↔iceberg fork〔T07 `equalsIgnoreCase`〕〕/`IcebergScanPlanProvider`〔`planSystemTableScan`〔`buildScan` 应用 filter→FileScanTask `residual`〕/`resolveSysTable`/`getScanNodeProperties` sys guard/**T07 `supportsSystemTableTimeTravel()`=true**/`SYS_TABLE_DUMMY_PATH="/dummyPath"`:127〕/`IcebergScanRange`〔`serializedSplit` 早返〕。
- **fe-core**：`PluginDrivenScanNode.checkSysTableScanConstraints`〔**T07 capability-aware**：`sysTableSupportsTimeTravel()` 委派 `connector.getScanPlanProvider().supportsSystemTableTimeTravel()`；scan-params 块**先于** snapshot 块〕 + T06 `PluginDrivenExternalTable.getEngine:474/getEngineTableTypeName:505` iceberg case + `getSupportedSysTables:419`〔SPI list→bare-key PluginDrivenSysTable〕 + `TableIf.TableType.toMysqlType:324` PLUGIN→"BASE TABLE" + `ShowCreateTableCommand.validate` 解包。其余〔`PluginDrivenSysExternalTable`〔无 `@SerializedName` 字段〕/`PluginDrivenSysTable`/`SysTableResolver`/`Env.getDdlStmt:4915`/`UserAuthentication:60`〕零改动复用。
- **新 SPI**：`ConnectorScanPlanProvider.supportsSystemTableTimeTravel()` default false——paimon/mc/jdbc/es 继承默认，仅 iceberg override。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：`datasource/iceberg/IcebergSysExternalTable`〔`toThrift:116-131`〕 + `datasource/systable/IcebergSysTable`〔小写键 `SUPPORTED_SYS_TABLES` + `UNSUPPORTED_POSITION_DELETES_TABLE`〕 + `IcebergScanNode`〔`createTableScan:569` sys time-travel honor / `doGetSystemTableSplits:974`〕。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相）；单类/多类 `test -Dtest=A,B` 即可。**实测**：连接器单类 warm ~13s、全量 ~48s；**fe-core -am 单类/多类 ~2.5min**。后台 task 通知的 exit code 是 echo 的非 maven 的，读 surefire XML / `*_EXIT` 行。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（`org.apache.doris.thrift.*` + SDK `org.apache.iceberg.*` 允许；新 SPI 在 `connector.api.scan` 合法）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔guard 用 `CALLS_REAL_METHODS` + `guardOnlyNode()`〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow/guard 测易 trivially-pass → 必变异验真。**单线产品变异**比 cp-整文件更省（python in-place replace）；**生产 0 改的 session 用 `git checkout -- <prod>` 复原最干净**（diff 必空）。多 assert-pin 一测用 `assertAll` 让各 pin 被自身 mutation 捕获（否则 `assertEquals` short-circuit 掩盖后续 pin）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕）。本 session 动 **0 产品 + 4 测 + 4 文档**：
  - 测：`fe/fe-core/src/test/java/org/apache/doris/datasource/{PluginDrivenSysTableTest,PluginDrivenScanNodeSysTableGuardTest}.java` · `fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/{IcebergScanPlanProviderTest,IcebergConnectorMetadataSysTableTest}.java`
  - 文档：`plan-doc/{tasks/P6-iceberg-migration.md,PROGRESS.md,connectors/iceberg.md,HANDOFF.md}`（decisions-log/deviations-log 无新条目）
- message `[refactor](catalog) P6.5 iceberg: T07-续 — <subj>` + 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。**注意 P6.4-T07..T09 + P6.5-T01..T07 已 commit 未 push**（rebase/force 须谨慎）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/audit-spec 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep + 实证）再信文档。**T07-续 实证纠 1 处**：audit test-6 spec 设「sys 元数据列谓词 FE 行裁」**实证为假**（是 BE-applied residual），改钉 `task.residual()`。**教训**：连 audit wf 写的 test spec 也须实证；写完即 run，红则 root-cause（systematic-debugging）不硬凑。
- **大文件用 subagent/workflow 总结**；PROGRESS.md 巨行（§一 6191 字符 + §二 board + §四 dynamics）编辑前先 grep 定位精确子串再 surgical Edit（Edit 前必先 Read 该文件一次，否则 tool 报 "not read yet"）。
- **文档同步五步**：tasks 表 + 实现记录 / PROGRESS〔§一 + §二 board + §四 dynamics〕 / connectors/iceberg.md〔完成度 + 进度日志〕 / decisions-log / deviations-log；HANDOFF 覆盖式。本 session：tasks/PROGRESS/connectors/HANDOFF 已做，decisions/deviations 无新条目。
- **T08 起步**：照上「T08 起步指引」——汇总设计 doc + faithfulness 对抗验证 wf〔引行号/commit/UT 计数必跑 + **UT 计数必重跑 surefire 实证**〕⇒ P6.5 DONE → board P6.5 🔵→✅。**决策已全定，无需再问**。
