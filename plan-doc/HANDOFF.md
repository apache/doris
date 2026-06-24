# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.5-T07-续（残留 gap-fill UT，8 项）→ 然后 T08 收口**

**P6.5-T07 = 🟡 进行中**（对抗审计 + 2 现修〔guard capability + hms 大小写〕+ 9 gap-fill + DV-048/049 + [D-067] **DONE 且已验**；**残留 8 项 gap-fill UT 延本续做**）。T07 节奏 = T06 → **T07 parity 审计 + DV 登记 + 对抗 wf** → T08 收口/汇总设计 + faithfulness 对抗验证 wf + gate 重跑 = **P6.5 DONE**。

## 残留 gap-fill UT（audit confirmed，**精确 spec 见下，无需重跑审计**）

> 全部 dormant-路 / 加法 UT；连接器无 Mockito〔fail-loud fake + InMemoryCatalog〕，fe-core Mockito〔`TestablePluginCatalog(type)`〕。**assertFalse/doesNotThrow/guard 类必 mutation-check**（HANDOFF 坑①）。

**fe-core（`fe/fe-core/src/test/.../datasource/PluginDrivenSysTableTest.java`，Mockito）**：
1. **sys `getMysqlType`="BASE TABLE"**：build `PluginDrivenSysExternalTable` over `TestablePluginCatalog("iceberg")`；`assertEquals("BASE TABLE", sys.getMysqlType())` **AND** `assertEquals("BASE TABLE", TableType.ICEBERG_EXTERNAL_TABLE.toMysqlType())`〔同测钉 new==legacy，无 Env〕。Mutation：删 `TableIf:324` PLUGIN_EXTERNAL_TABLE case → sys TABLE_TYPE null → red。〔= DV-049② internal-tabletype 的 user-visible parity 钉〕
2. **sys `getEngine`="iceberg" + `getEngineTableTypeName`="ICEBERG_EXTERNAL_TABLE"**：同 over iceberg catalog 的 sys 实例（现仅 base 表测了 engine）；assert 两者。Mutation：删 `PluginDrivenExternalTable:474/505` iceberg case → "Plugin"/PLUGIN → red。〔sys 表继承 `getEngine`，T06-F1 对 sys 路未单独钉〕
3. **position_deletes fe-core seam**：stub metadata `listSupportedSysTables` 返 iceberg-style 列（去 position_deletes）；assert `table.getSupportedSysTables()` **不** containsKey("position_deletes") **AND** `table.findSysTable("t$position_deletes").isEmpty()`〔证通用 not-found 路，别于 legacy `UNSUPPORTED_POSITION_DELETES_TABLE`〕。〔= DV-049③ 文案残留的连接器/fe-core 交叉钉〕
4. **non-registration 不变式**：resolve 一个 sys handle 后 assert catalog 的 table map **不** contains "$"-后缀名 + 类无 `@SerializedName`〔防 sys 表泄进 SHOW TABLES / edit-log〕。
5. **guard-message ordering**（用我新 guard）：sys 表 + capability=**false**（paimon-like）+ **同时** stub `getScanParams()`〔非 incr，如 branch mock〕**和** `getQueryTableSnapshot()` 非 null；assert **"scan params"** 文案先于 "time travel"〔我的 guard `if(getScanParams()!=null)` 块在 snapshot 块前〕。Mutation：交换两块 → "time travel" 先 → red。

**连接器（`IcebergScanPlanProviderTest`，无 Mockito）**：
6. **sys predicate-pushdown**：$files 表 2 数据文件 + 对元数据列〔如 `record_count`/`file_size`〕的可下推谓词裁到 1 行；用既有 `countSerializedSplitRows` helper〔`IcebergScanPlanProviderTest:~1814`〕deserialize 序列化 split；assert 过滤后行数 < 无过滤〔证 filter 真达 metadata `scan.filter`，`planSystemTableScan:292` `buildScan(...,filter,...)`〕。Mutation：sys 路丢 `filter` arg → 不减行 → red。
7. **sys `/dummyPath` provider 级**：plan $snapshots 后 assert 每 sys range `getPath().get().equals("/dummyPath")`〔`IcebergScanPlanProvider:297` `.path(SYS_TABLE_DUMMY_PATH)`；现仅 carrier 测自供 path〕。Mutation：`.path(task.file().path())` → red。

**WHY-comment 修（`IcebergConnectorMetadataSysTableTest`，无新测）**：
8. 改 `getSysTableHandleNormalizesNameToLowercase` 的 WHY-comment〔`:187-188`〕——legacy resolution 是 **case-SENSITIVE `Map.get`**〔`TableIf.findSysTable:413` over 小写键 `IcebergSysTable:52`〕，**非** equalsIgnoreCase；连接器的 case-insensitive accept 是 production **永不达**的无害超集〔only 小写 map 键被喂入，`PluginDrivenSysExternalTable:85`〕。现注释把 SDK `MetadataTableType.from` 的大小写不敏感误归给 resolution gate。

**audit 完整产出**（22 finding/19 confirmed/3 refuted）= 本 session 审计 wf `wf_d530d760-ccf` 结果，原始 JSON 在 `/tmp/.../tasks/wc77vamfp.output`（scratch 可能已清——上述 8 项已抄全 spec）。

**TDD + 基建**：复用连接器 `RecordingIcebergCatalogOps`〔`.table` 注 InMemoryCatalog BaseTable，`createMetadataTableInstance` 需真 `BaseTable`〕/`RecordingConnectorContext`〔`authCount`/`failAuth`/`storageProperties`〕；fe-core `TestablePluginCatalog`。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完。翻闸前必修下述阻塞：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge`。**P6.5 元数据列挂靠** + 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。**⚠️ P6.5-T07 新增本族一项**：**sys-table 时间旅行 query→连接器 handle pin 的接线**——guard 已修不再 BLOCK〔[D-067]〕、连接器 T02/T05 已保留+honor pin，但 query `getQueryTableSnapshot()`/`getScanParams()`→`IcebergTableHandle` 的 snapshot/ref pin **的通用-路 threading 仍休眠**〔现仅 MVCC 路 `applyMvccSnapshotPin` 走 `metadata.applySnapshot`，非 iceberg `FOR TIME AS OF`〕→ 翻闸后 sys 时间旅行**完整 e2e** 须接此线 + P6.8 docker 验。

**[DV-045]**（写路径）= `rewrite_data_files` **执行半翻闸接线**（R-B 专门写路径 RFC，用户裁 Option 1 推后）。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager`。

**⚠️ P6.5 翻闸激活点（pre-flip 不激活，flip 后才生效）**：T06-F1 `PluginDrivenExternalTable.getEngine/getEngineTableTypeName` `case "iceberg"` + T06-F2 `ShowCreateTableCommand` 解包〔F2 对 **live paimon** 立即生效=DV-048① priv 修〕 + **T07 guard `supportsSystemTableTimeTravel()`**〔iceberg→true 仅 flip 后命中；paimon→默认 false 不变〕 + T07 hms `equalsIgnoreCase`〔描述符，flip 后 EXPLAIN 激活〕。

**[pre-flip 行为偏差中央登记]**：P6.4 = DV-046/047。**P6.5 = DV-048〔correctness-bearing：F2 paimon priv loosening〔live〕+ serialized FileScanTask 字节潜伏（T05）〕/ DV-049〔perf-cosmetic：sys split-weight 丢〔镜像 DV-033〕+ 内部 TableType.PLUGIN_EXTERNAL_TABLE〔user-visible 已 parity〕+ position_deletes 文案〕**。**注**：T07 揭出的 sys 时间旅行 guard 拒绝 + hms 大小写**用户裁现修**〔[D-067]〕→ **非 DV**（消除偏差）。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}；翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**P6.4 T07/T08/T09 + P6.5 T01–T07 已 commit 未 push（10 commit；T07 = `git log` HEAD，本 session 提交）**。**用户未要求 push**——留用户裁量。残留 8 gap-fill UT 未实现（见上「下一个 session」spec），工作树 tracked 干净。
- **P6.1–P6.4 = ✅ DONE**。**P6.5 = 🔵 进行中**（T01–T06 ✅；**T07 🟡**〔审计+2 现修+9 gap-fill+DV-048/049 done；残留 8 gap-fill 延续〕；T08 待）。
- iceberg **不在** `SPI_READY_TYPES`（仍走 switch-case `:137 case "iceberg"`）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 = P6.5-T07（审计 + 2 现修 + 9 gap-fill + DV，**残留 8 gap-fill 延续**，待 commit）

- **对抗 byte-parity 审计 wf** `wf_d530d760-ccf`（8 area finder + refute-by-default skeptic〔effort=high〕+ completeness critic；31 agent/1.6M token）= **22/19 confirmed**。**主 session 独立读码交叉核**揭出 2 项主动偏差〔playbook：recon 否定/矛盾断言须主核——本次正是 critic+finder 双揭、主核实证〕。
- **用户 AskUserQuestion 双裁「现修」（[D-067]）**：**A** = sys 时间旅行 guard〔共享 fe-core `checkSysTableScanConstraints`，P5 paimon `38e7140ce56`〕connector-capability-aware；**B** = hms 分叉 `equalsIgnoreCase`。
- **现修 A（4 文件其 3 + guard 测）**：① 新 SPI `ConnectorScanPlanProvider.supportsSystemTableTimeTravel()` 默认 false；② `IcebergScanPlanProvider` override true；③ fe-core `PluginDrivenScanNode` guard〔顶 `boolean timeTravelSupported = sysTableSupportsTimeTravel()`〔新包私方法委派 `connector.getScanPlanProvider()`，null-safe〕；capability=true 放行 time-travel + branch/tag，**@incr 对所有连接器仍拒**〕；`PluginDrivenScanNodeSysTableGuardTest` +3〔guardOnlyNode 默认 stub capability=false〕。
- **现修 B（1 产品行 + UT）**：`IcebergConnectorMetadata.buildTableDescriptor` `TYPE_HMS.equals`→`equalsIgnoreCase` + `IcebergBuildTableDescriptorTest.forkIsCaseInsensitiveOnHmsType`。
- **+9 连接器 gap-fill**：`IcebergTableHandleTest`〔coords-in-identity / pinned-toString〕·`IcebergConnectorMetadataSysTableTest`〔colhandles auth-scope×2 / keyset #969249 / empty-sysname〕·`IcebergScanPlanProviderTest`〔capability / sys location-creds〕·`IcebergBuildTableDescriptorTest`〔hms 大写〕。
- **mutation-check**：guard **Mut-A**〔`timeTravelSupported=sysTableSupportsTimeTravel()`→`=false`〕→AllowsTimeTravel+AllowsBranchTag 双红；guard **Mut-B**〔去 `|| getScanParams().incrementalRead()`〕→StillRejectsIncr 红；hms **Mut**〔`equalsIgnoreCase`→`equals`〕→大写 UT 红。每次 `cp`/python 复原 diff IDENTICAL（GOOD 备份在 scratchpad）。
- **验证（重跑 surefire 实证，Rule 12）**：连接器全量 `package -Dassembly.skipAssembly=true` **541/0/1**〔=532+9，0 回归〕；fe-core guard **7/0/0**；checkstyle 0；import-gate exit 0；`CatalogFactory:51` 未改。**新 1 D（[D-067]）；新 2 DV（DV-048/049）**。**live-e2e 未跑**（dormant，P6.8 兜底）。
- **⚠️ 未做**：残留 8 gap-fill UT（上「下一个 session」spec）；guard 修的完整 sys 时间旅行 e2e 须 query→handle pin 接线〔DV-041 族，P6.6〕。

---

# 🗺️ 代码脚手架（iceberg sys-table）

- **连接器**（dormant）：`IcebergTableHandle`〔sys 变体 + 保留 pin〕/`IcebergConnectorMetadata`〔`listSupportedSysTables`/`getSysTableHandle`〔LAZY〕/`getTableSchema`·`getColumnHandles` sys 分支/`loadSysTable`/`buildTableDescriptor` hms↔iceberg fork〔T07 `equalsIgnoreCase`〕〕/`IcebergScanPlanProvider`〔`planSystemTableScan`/`resolveSysTable`/`getScanNodeProperties` sys guard/**T07 `supportsSystemTableTimeTravel()`=true**〕/`IcebergScanRange`〔`serializedSplit` 早返〕。
- **fe-core**：`PluginDrivenScanNode.checkSysTableScanConstraints`〔**T07 capability-aware**：`sysTableSupportsTimeTravel()` 委派 `connector.getScanPlanProvider().supportsSystemTableTimeTravel()`〕 + T06 `PluginDrivenExternalTable.getEngine/getEngineTableTypeName` iceberg case + `ShowCreateTableCommand.validate` 解包。其余〔`PluginDrivenSysExternalTable`/`PluginDrivenSysTable`/`SysTableResolver`/`Env.getDdlStmt:4915`/`UserAuthentication:60`〕零改动复用。
- **新 SPI**：`ConnectorScanPlanProvider.supportsSystemTableTimeTravel()` default false〔与 `ignorePartitionPruneShortCircuit`/`supportsBatchScan` 同族 capability 默认〕——paimon/mc/jdbc/es 继承默认，仅 iceberg override。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：`datasource/iceberg/IcebergSysExternalTable`〔`toThrift:116-131` + 类型 `:57` ICEBERG_EXTERNAL_TABLE〕 + `datasource/systable/IcebergSysTable`〔`:74` "not supported yet"〕 + `IcebergScanNode`〔`createTableScan:569` sys time-travel honor〔无 isSystemTable gate〕/`doGetSystemTableSplits:974`/`setIcebergParams`/`getFileFormatType`〕。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相）；单类/多类 `test -Dtest=A,B` 即可。**实测**：连接器单类 warm ~13s、全量 ~48s；**fe-core -am 单类 ~2.5min**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（**`org.apache.doris.thrift.*` + SDK `org.apache.iceberg.*` 允许**；T07 新 SPI 在 `connector.api.scan` 合法）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow guard 测易 trivially-pass → 必变异验真。**单线产品变异**比 cp-整文件更省（T07 用 python in-place replace + 复原）；**`cp` 复原务必绝对路径 + 复原后 Edit 前重 Read**（坑）。fe-core 变异每次 ~2.5min build，连接器 ~13s。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕）。本 session 动 **4 产品 + 5 测 + 6 文档**：
  - 产品：`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanPlanProvider.java` · `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/{IcebergScanPlanProvider,IcebergConnectorMetadata}.java` · `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java`
  - 测：`fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodeSysTableGuardTest.java` · `fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/{IcebergScanPlanProviderTest,IcebergConnectorMetadataSysTableTest,IcebergTableHandleTest,IcebergBuildTableDescriptorTest}.java`
  - 文档：`plan-doc/{tasks/P6-iceberg-migration.md,PROGRESS.md,connectors/iceberg.md,decisions-log.md,deviations-log.md,HANDOFF.md}`
- message `[refactor](catalog) P6.5 iceberg: T07 — <subj>` + 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。**注意 P6.4-T07..T09 + P6.5-T01..T06 已 commit 未 push**（rebase/force 须谨慎）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动码前先 recon（grep + 实证）再信文档。**T07 实证纠 HANDOFF 框定 1 处**：偏差①「sys handle 保留 pin = parity-保留、非 DV」**分类错**——共享 fe-core guard〔P5 paimon〕翻闸后拒 sys 时间旅行，使保留的 pin dead-on-arrival=回归；本 session 经 [D-067] guard capability 修消解〔connector 声明能力〕。**教训**：审计 finder + critic 双独立揭出的「跨层不变式互相抵消」类 finding 价值最高，主 session 务必读码交叉核（本次 finder 称偏差① unreachable、critic 标 load-bearing contradiction，主核 git+读码全证实）。
- **大文件用 subagent/workflow 总结**；PROGRESS.md 巨行（§一 + §二 board〔UT 计数双处〕）编辑前先 Read 精确行再 surgical Edit（Edit 前必 Read 该文件；PROGRESS/iceberg.md 用 grep 定位行号后 `Read offset=N limit=1` 再 Edit）。
- **文档同步五步**：tasks 表 + 实现记录 / PROGRESS〔§一 + §二 board + §四 dynamics〕 / connectors/iceberg.md / decisions-log / deviations-log；HANDOFF 覆盖式。本 session 五步 + HANDOFF 全做。
- **T07-续 起步**：直接照上「残留 gap-fill spec」逐条 TDD+mutation〔无需重跑审计〕；fe-core gap-fill〔1-5〕一次 build 跑，连接器〔6-7〕一次 build 跑，WHY-comment〔8〕纯注释。**决策 A/B/[D-063..067] 已定，无需再问**。完成后即 T08 收口〔faithfulness 对抗 wf 若引行号/commit/UT 计数必跑 + **UT 计数必重跑 surefire 实证**，Rule 12〕⇒ P6.5 DONE。
