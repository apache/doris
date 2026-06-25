# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 翻闸（全有或全无）= 先 holistic 修 4 阻塞 → 再加 iceberg 进 `SPI_READY_TYPES`**

**P6.1–P6.5 = ✅ 全 DONE**（实现完）。**P6.5 sys-table 本 session 收口 DONE**（T07-续 8 gap-fill + T08 汇总设计 + faithfulness wf 18/18 confirmed）。下一 = **P6.6 唯一翻闸**——但翻闸是**全有或全无**（`CatalogFactory:104-113`），按下即所有 iceberg 查询走 SPI 路；故**必须先 holistic 修下述 4 个翻闸阻塞**（同需写/读/handle 共享 fe-core seam），再动 `SPI_READY_TYPES`。

## P6.6 起步指引（先做这个）
1. **读** `tasks/designs/P6.5-T08-systable-summary-design.md` §5 + 本文「🔴🔴 开放问题」=翻闸阻塞全集。P6.6 是大阶段（非单 session），**先起一个翻闸-holistic 设计/RFC**（仿 P6.3 写路径 RFC `designs/...write-framework`），把 4 阻塞统一编排。
2. **4 阻塞（共享 fe-core seam，须一并修）**：[DV-038]（读路径 field-id BE DCHECK）+ [DV-041]（写路径 `visitPhysicalConnectorTableSink` 合成列物化 + pin threading）+ [DV-045]（`rewrite_data_files` 执行半 R-B 写路径接线）+ **sys 时间旅行 query→`IcebergTableHandle` pin threading**（P6.5 揭，DV-041 同族）。
3. **翻闸本体**（阻塞修完后）：加 iceberg 进 `SPI_READY_TYPES`〔`CatalogFactory:51`〕+ 删 built-in `:137 case "iceberg"` + `pluginCatalogTypeToEngine` 加 `iceberg→ENGINE_ICEBERG` + `PhysicalPlanTranslator` 分支收口 + **GSON compat**（7 catalog flavor + db + table 全 `registerCompatibleSubtype`→PluginDriven*）+ restore SHOW PARTITIONS/CREATE TABLE parity。
4. **翻闸后**：P6.7 删 legacy（`datasource/iceberg/` + `datasource/systable/IcebergSysTable` + `IcebergScanNode` sys 路 + ~49 反向 `instanceof IcebergExternal*` + 写路径 `IcebergTransaction`/legacy sink + `action/`+`rewrite/`）→ P6.8 docker 回归（首验全部 UT 不可见 deviation：sys-table + DV-038..049 + 7-flavor 读/JNI/time-travel/Kerberos/vended）。
5. **⚠️ 翻闸前切忌动 `SPI_READY_TYPES`**（现 = {jdbc,es,trino-connector,max_compute,paimon}；提前翻 = procedure/sys-table/写路径全断）。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 一并修）

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge`。**P6.5 元数据列挂靠** + 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-045]**（写路径）= `rewrite_data_files` **执行半翻闸接线**（R-B 专门写路径 RFC，用户裁 Option 1 推后）。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager`。

**[P6.5 揭] sys 时间旅行 query→handle pin threading**（DV-041 同族）：guard 已修不再 BLOCK〔[D-067]〕、连接器 T02/T05 已**保留 + honor** pin，但 query `getQueryTableSnapshot()`/`getScanParams()` → `IcebergTableHandle` snapshot/ref pin 的**通用-路 threading 仍休眠**（现仅 MVCC 路 `applyMvccSnapshotPin` 走 `metadata.applySnapshot`，非 iceberg `FOR TIME AS OF`/`@branch`/`@tag`）→ 翻闸后 sys 时间旅行**完整 e2e** 须接此线 + P6.8 docker 验。

**[pre-flip 行为偏差中央登记]**：P6.4 = DV-045/046/047；**P6.5 = DV-048〔correctness-bearing：F2 paimon priv loosening〔live〕+ serialized FileScanTask 字节潜伏（T05）〕/ DV-049〔perf-cosmetic：sys split-weight 丢 + 内部 TableType.PLUGIN_EXTERNAL_TABLE〔user-visible 已 parity〕+ position_deletes 文案〕**。**注**：T07 sys 时间旅行 guard 拒 + hms 大小写 = 用户裁现修〔[D-067]〕非 DV；T07-续 test-6「sys 元数据列谓词非 FE 行裁」= BE-applied residual = legacy parity，非 DV（仅纠 audit test spec，T08 faithfulness wf 经 iceberg 1.10.1 bytecode 复证）。

**⚠️ P6.6 才动 `SPI_READY_TYPES`**（`CatalogFactory:51`）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**P6.4 T07/T08/T09 + P6.5 T01–T07 + T07-续 + T08 已 commit 未 push（12 commit；T07-续 `6e96a20f68e` + T08 = 本 session 提交）**。**用户未要求 push**——留用户裁量。工作树 tracked 干净。
- **P6.1–P6.5 = ✅ DONE**（实现完）。**P6.6 翻闸 = 下一阶段（behind gate，须先 holistic 修 4 阻塞）**。
- iceberg **不在** `SPI_READY_TYPES`（仍走 switch-case `:137 case "iceberg"`，pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 = P6.5-T07-续 + T08 ⇒ **P6.5 DONE**（待 push）

- **T07-续（commit `6e96a20f68e`）= 残留 8 gap-fill UT + mutation（生产 0 改）**：fe-core `PluginDrivenSysTableTest` +4〔sys `getMysqlType`="BASE TABLE"〔同测钉 new==legacy〕/ `getEngine`+`getEngineTableTypeName`〔**assertAll** 两 pin 独立 mutation〕/ position_deletes 通用 not-found〔含正控〕/ 非注册不变式〔无 `@SerializedName` + 无 `$`-key〕〕 + guard +1〔message 顺序 scan-params 先于 time-travel〕 + 连接器 `IcebergScanPlanProviderTest` +2〔sys predicate residual / sys split `/dummyPath`〕 + `IcebergConnectorMetadataSysTableTest` WHY-comment 修。
- **⚠️ test-6 实证纠 audit spec**：原 spec 设 sys 元数据列谓词「裁到 1 行」**实证为假**（record_count=10 后 FE 行数 2 vs 2 不变）——iceberg 元数据表**列**谓词是 **BE-applied residual**（`IcebergSysTableJniScanner` 读时应用），非 FE plan-time 行裁〔plan-time 裁是 snapshot pin〕。test-6 改钉 `task.residual()` 携 `record_count`。**非 legacy 偏差**。**T08 faithfulness wf 经 iceberg 1.10.1 bytecode 独立复证**（`BaseFilesTable$ManifestReadTask` 携 filter 为 per-task residual、`rows()` 不 apply）。
- **测改进**：test-2 改 `assertAll`，getEngine/getEngineTableTypeName 两 pin 各被自身 mutation 捕获。
- **mutation-check（全绿→红→`git checkout` 复原 IDENTICAL）**：fe-core 5 变异一次 build→test1–5 全红 + 1 已知 collateral〔testDelegates 2→3〕；连接器 2 变异→test6/7 红。
- **T08（汇总设计 + faithfulness wf ⇒ P6.5 DONE）**：`tasks/designs/P6.5-T08-systable-summary-design.md`（7 节）+ **faithfulness 对抗验证 wf** `wf_27596236-5fe`（3 verifier refute-by-default + completeness critic；4 agent/256k token）= **18/18 confirmed、0 refuted、0 critic fix**（critic 经 bytecode 复证 test-6 + WHY-comment 链 C1–C5「factually accurate」）。
- **验证（重跑 surefire，Rule 12）**：fe-core `PluginDrivenSysTableTest` **10/0/0** + guard **8/0/0**；连接器全量 **543/0/1**〔=541+2，0 回归〕`IcebergScanPlanProviderTest` 67/0 + `IcebergConnectorMetadataSysTableTest` 22/0；checkstyle 0〔两模块〕；import-gate 0；`CatalogFactory:51` 未改。**0 新 D/DV**。**live-e2e 未跑**（dormant，P6.8 兜底）。

---

# 🗺️ 代码脚手架（iceberg sys-table，全 DONE/dormant）

- **连接器**：`IcebergTableHandle`〔sys 变体 + 保留 pin〕/`IcebergConnectorMetadata`〔`listSupportedSysTables`/`getSysTableHandle`〔LAZY〕/`getTableSchema`·`getColumnHandles` sys 分支/`loadSysTable`/`buildTableDescriptor` hms↔iceberg fork〔`equalsIgnoreCase`〕〕/`IcebergScanPlanProvider`〔`planSystemTableScan`〔`buildScan` 应用 filter→FileScanTask `residual`〕/`resolveSysTable:1132`/`getScanNodeProperties` sys guard/`supportsSystemTableTimeTravel()`=true:196/`SYS_TABLE_DUMMY_PATH="/dummyPath":127〕/`IcebergScanRange`〔`serializedSplit` 早返〕。
- **fe-core**：`PluginDrivenScanNode.checkSysTableScanConstraints`〔capability-aware：`sysTableSupportsTimeTravel()` 委派 `connector.getScanPlanProvider().supportsSystemTableTimeTravel()`；scan-params 块**先于** snapshot 块〕 + `PluginDrivenExternalTable.getEngine:474/getEngineTableTypeName:505` iceberg case + `getSupportedSysTables:419`〔SPI list→bare-key〕 + `TableIf.TableType.toMysqlType:324` PLUGIN→"BASE TABLE" + `findSysTable:413`〔case-SENSITIVE Map.get〕 + `ShowCreateTableCommand.validate` 解包。零改动复用：`PluginDrivenSysExternalTable`〔无 `@SerializedName` 字段；`resolveConnectorTableHandle:85` 透 sys handle〕/`PluginDrivenSysTable`/`SysTableResolver`/`SysTable.getTableNameWithSysTableName`〔不 lower-case suffix〕。
- **新 SPI**：`ConnectorScanPlanProvider.supportsSystemTableTimeTravel()` default false——仅 iceberg override。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：`datasource/iceberg/IcebergSysExternalTable`〔`toThrift:116-131`〕 + `datasource/systable/IcebergSysTable`〔小写键 `SUPPORTED_SYS_TABLES` + `UNSUPPORTED_POSITION_DELETES_TABLE`〕 + `IcebergScanNode`〔`createTableScan:569` sys time-travel honor / `doGetSystemTableSplits`〕。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相）；单类/多类 `test -Dtest=A,B` 即可。**实测**：连接器单类 warm ~13s、全量 ~48s；**fe-core -am 单类/多类 ~2.5min**。后台 task 通知的 exit code 是 echo 的非 maven 的，读 surefire XML / `*_EXIT` 行。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（`org.apache.doris.thrift.*` + SDK `org.apache.iceberg.*` 允许；SPI 在 `connector.api.scan` 合法）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔guard 用 `CALLS_REAL_METHODS` + `guardOnlyNode()`〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow/guard 测易 trivially-pass → 必变异验真。**生产 0 改的 session 用 `git checkout -- <prod>` 复原最干净**（diff 必空）。多 assert-pin 一测用 `assertAll`（否则 `assertEquals` short-circuit 掩盖后续 pin）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕）。本 session T08 动 **0 产品 + 0 测 + 5 文档**（含新 `designs/P6.5-T08-systable-summary-design.md`）：
  - 文档：`plan-doc/tasks/designs/P6.5-T08-systable-summary-design.md`（新）· `plan-doc/{tasks/P6-iceberg-migration.md,PROGRESS.md,connectors/iceberg.md,HANDOFF.md}`（decisions-log/deviations-log 无新条目）
- message `[refactor](catalog) P6.5 iceberg: T08 — <subj>` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。**注意 P6.4-T07..T09 + P6.5-T01..T08 已 commit 未 push（12 commit）**（rebase/force 须谨慎）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。P6.7 删 legacy 时尤其。
- **HANDOFF/设计/audit-spec 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep + 实证）再信文档。**P6.5 实证纠 audit test-6 spec 1 处**（「sys 元数据列谓词 FE 行裁」实为 BE-applied residual），写完即 run、红则 root-cause 不硬凑。
- **大文件用 subagent/workflow 总结**；PROGRESS.md 巨行（§一 6191 字符 + §二 board mega-row + §四 dynamics）编辑前先 grep 定位精确子串再 surgical Edit（Edit 前必先 Read 一次）。
- **文档同步五步**：tasks 表 + 实现记录 / PROGRESS〔§一 + §二 board + §四 dynamics〕 / connectors/iceberg.md〔完成度 + 进度日志〕 / decisions-log / deviations-log；HANDOFF 覆盖式。本 session：tasks/PROGRESS/connectors/HANDOFF + 新 design doc 已做，decisions/deviations 无新条目。
- **P6.6 起步**：照上「P6.6 起步指引」——先翻闸-holistic 设计/RFC 编排 4 阻塞（DV-038/041/045 + sys threading），再翻闸 + GSON compat。**这是大阶段，分多 session**。决策已全定（P6.5 范围），P6.6 设计须新 recon + 用户签字（翻闸是全有或全无的高风险动作）。
