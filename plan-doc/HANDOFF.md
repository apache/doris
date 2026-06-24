# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.5-T06 = thrift hms↔iceberg 分叉核 `buildTableDescriptor`(sys handle) + DESCRIBE/SHOW parity + `getScanNodeProperties` sys 收口**（TDD）

**P6.5-T05 = ✅ DONE**（`IcebergScanPlanProvider`/`IcebergScanRange` sys split 路，TDD，2 产品文件 + 同两测试类 +6 测，连接器 **527/0/1**，**未 push**）。recon workflow `wf_c219ede1-8b6`〔7-agent，逐行核 legacy 字节形状 + 埋钩 + thrift + BE 消费方 + paimon 模板 + 测试基建〕**纠设计 1 处**：legacy sys 表**确** honor time-travel（直读 `IcebergScanNode.createTableScan():569,579-583`，偏差①正确——recon agent 初判「无 time-travel」是误推）。新决策 **[D-065]**（无需用户签字——决策 A/B/[D-063]/[D-064] 已定、§6 well-specified）。

- **T06 内容**（设计 §10）= **thrift 描述符 hms↔iceberg 分叉核 `buildTableDescriptor` for sys handle（偏差⑥）+ DESCRIBE/SHOW CREATE/information_schema parity 核 + UT/gap-fill**，**并含 [D-065] 推迟项**：
  - **[D-065] T05 推迟项 = `getScanNodeProperties` sys handle 收口**：现 `IcebergScanPlanProvider.getScanNodeProperties`（`:585`）对 sys handle 仍调 `resolveTable`（**base 表**）→ 发 base 表的 `path_partition_keys`（`:594-597`）+ `schema_evolution` dict（`:631-639`，从 base schema 构）。对 sys handle 这**不正确**（meta 表非 base-spec 分区；BE sys JNI scanner 忽略 dict），但 **dormant**（pre-flip 不执行）。T06 须加 sys guard：resolve 元数据表（→ `path_partition_keys` 正确为空）+ **跳** schema_evolution dict（镜像 paimon 对 metadata 表跳 dict）。**⚠️ 验**：`IcebergSchemaUtils.encodeSchemaEvolutionProp(baseTable, baseSchema, requestedMetaCols)` 对 sys handle（请求列=meta 列，不在 base schema）是否抛——若抛=潜伏 crash（dormant 不触发，但 T06 须修）。
  - **thrift 分叉核（偏差⑥）**：legacy `IcebergSysExternalTable.toThrift():116-131` 对 **hms** catalog 返 `THiveTable` 否则 `TIcebergTable`（hms↔iceberg 分叉）。连接器 `buildTableDescriptor`（找连接器对应方法）对 sys handle 须复现该分叉，否则登记 DV。**先 recon**：grep 连接器现 `buildTableDescriptor`/`toThrift` 路 + 是否已对 base 表做 hms 分叉（P6.1/P6.2 可能已建）。
  - **类型变更（偏差⑥，pre-flip DV）**：legacy `IcebergSysExternalTable` 报 `ICEBERG_EXTERNAL_TABLE`（`:57`），flip 后通用 `PluginDrivenSysExternalTable` 报 `PLUGIN_EXTERNAL_TABLE`——**flip 行为偏差**，T07 批量登记 DV（非 T06 修，T06 仅核 DESCRIBE/SHOW 输出 parity）。
  - **节奏**（设计 §10）：T06（thrift 分叉 + DESCRIBE/SHOW parity + getScanNodeProperties 收口）→ T07（parity 审计 + DV 中央登记 + 对抗 parity workflow）→ T08（收口/汇总设计 + faithfulness 对抗验证 wf + gate 重跑 = **P6.5 DONE**）。**全程不碰 `SPI_READY_TYPES`，dormant 至 P6.6**。
- **TDD + 测试基建**：复用 T04/T05 真 `InMemoryCatalog` 表（`createMetadataTableInstance` 需真 `BaseTable`，`FakeIcebergTable` 不够）；连接器侧无 Mockito（fail-loud fake + `RecordingIcebergCatalogOps`/`RecordingConnectorContext`）。**mutation-check 必验**（dormant 路）。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.5 = T01/T02/T03/T04/T05 done，T06–T08 待）。翻闸前必修下述阻塞（**同需读/写路径共享 fe-core seam 的 holistic 修**）：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。**⚠️ P6.5 新增挂靠**：iceberg **元数据列**（`IcebergMetadataColumn`/`IcebergRowId`，P6.5 用户签字推迟）属此族——挂 nereids `instanceof IcebergExternalTable` + `getFullSchema()` 钩子、不受 `SPI_READY_TYPES` 控制、flip 后表变 `PluginDrivenExternalTable`→钩子失效→DML row-id 注入断，须在此 holistic 修一并长出（无 paimon 模板）。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-045]**（写路径，P6.4-T06 实证）= `rewrite_data_files` **执行半翻闸接线**（R-B，专门写路径 RFC）。① 事务半（`IcebergConnectorTransaction` REWRITE 变体）+ 规划半（`RewriteDataFilePlanner`）已建 dormant；**②③④ 执行半留 fe-core**。用户裁 Option 1：① 已做（dormant），②③④ 推后。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057`→改绑 `UnboundConnectorTableSink` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager`。= **DV-041 写路径阻塞同族**。

**[pre-flip 行为偏差中央登记]**：P6.4 = [DV-046]（correctness-bearing）+ [DV-047]（perf-cosmetic 批）。**P6.5 预登记**（T07 批量）：sys 表类型变更 `ICEBERG_EXTERNAL_TABLE→PLUGIN_EXTERNAL_TABLE` / position_deletes 文案专属→通用 not-found / thrift hms↔iceberg 分叉 / **serialized FileScanTask 字节形状潜伏（T05，P6.8 e2e 兜底——FE deserialize-round-trip UT 已核同进程可消费，跨版本/classloader 不可及）**。**注**：T02 偏差①（sys handle 保留 pin）+ T03 [D-063]（lazy 解析）+ T04 [D-064]（@snapshot sys 短路）+ T05 [D-065]（scan split 路范围 + 显式 FORMAT_JNI；`$snapshots` 忽略 useSnapshot 是 legacy parity 非偏差）**均非** pre-flip 行为偏差——legacy 同样 honor 时间旅行 + 懒构 metadata-table + meta-table schema 不随快照变 + 发射相同 serialized FileScanTask，是 parity-保留/更忠实的内部设计选择，**不登记 DV**。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 四路 dormant + sys split 已建、scan split 已发射；翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**T01–T06 + arg-move 已推**；**P6.4 T07/T08/T09 + P6.5 T01/T02/T03/T04/T05 待 push**（T05 commit 后 `git rev-list --count origin..HEAD`=8）。**用户未要求 push**——留用户裁量。
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，iceberg 389/0/1 + fe-core 30/0）。**P6.4 = ✅ DONE**（T01–T09，iceberg 494/0/1 + fe-core 14/0）。**P6.5 = 🔵 进行中**（T01 ✅ recon+设计+二签字；T02 ✅ `IcebergTableHandle` sys 变体；T03 ✅ `IcebergConnectorMetadata` 2 override；T04 ✅ `getTableSchema`/`getColumnHandles` sys 分支；**T05 ✅ `IcebergScanPlanProvider`/`IcebergScanRange` sys split 路，iceberg 527/0/1**；T06–T08 待）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.5-T05（`IcebergScanPlanProvider`/`IcebergScanRange` sys split 路，TDD），1 commit（2 产品 + 2 测 + 5 文档），待 push

- **改动 = 2 产品文件（连接器，dormant）`IcebergScanRange.java` + `IcebergScanPlanProvider.java` + 同两测试类 +6 测**。P6.5 **唯一全新一块**（连接器已有 FORMAT_JNI 默认 + `buildScan` useRef/useSnapshot；缺 serialized-split 发射）。
- **byte-shape 契约（recon 实证，parity 目标）**：sys split = `serialized_split`〔base64 `SerializationUtil.serializeToBase64(FileScanTask)`，iceberg 1.10.1〕**唯一**载荷 + `FORMAT_JNI` + `table_level_row_count=-1` + `table_format_type=iceberg`〔dummy path `/dummyPath`，无 file-level 字段〕。BE `iceberg_sys_table_jni_reader.cpp:37-42` 校验非空否则 InternalError；`IcebergSysTableJniScanner:62` `deserializeFromBase64`→`:87` `asDataTask().rows()`。legacy 对照：`IcebergScanNode.doGetSystemTableSplits:974-989` / `createIcebergSysSplit:899-905` / `setIcebergParams:285-295`〔isSystemTable 早返：`:290` FORMAT_JNI、`:291` row_count=-1、`:292` serialized_split〕 / `:1090-1091` getFileFormatType FORMAT_JNI。
- **产品 `IcebergScanRange`**：① `serializedSplit` 字段（非 transient String，默认 null）+ `Builder.serializedSplit(...)` + `getSerializedSplit()`；② `populateRangeParams` 顶 sys 分支 `if(serializedSplit!=null)` 镜像 legacy `setIcebergParams` 早返（**仅** FORMAT_JNI + row_count=-1 + serialized_split + setIcebergParams，`return`；normal range `serializedSplit==null` 走原路、**字节不变**）。
- **产品 `IcebergScanPlanProvider`**：① `planScanInternal` 顶 sys guard（`if(iceHandle.isSystemTable()) return planSystemTableScan(...)`，在 count-pushdown 之前——sys 表无 snapshot-summary count）；② 新 `planSystemTableScan` = `resolveSysTable(handle)`〔元数据表〕 → **复用** `buildScan(metaTable, handle, filter, session)`〔time-travel useRef/useSnapshot + 谓词，镜像 legacy `createTableScan`〕 → `scan.planFiles()` → 每 `FileScanTask` 经 `SerializationUtil.serializeToBase64(task)` → `IcebergScanRange{/dummyPath, serializedSplit}`；③ 新 `resolveSysTable` = `executeAuthenticated` 内 `MetadataTableUtils.createMetadataTableInstance(catalogOps.loadTable(base), MetadataTableType.from(sysName))`〔镜像 T04 `loadSysTable`〕。新常量 `SYS_TABLE_DUMMY_PATH="/dummyPath"`。imports 仅加 `MetadataTableType`/`MetadataTableUtils`/`util.SerializationUtil`（SDK 允许）。
- **[D-065] 两裁定**：① T05 **仅** scan split 路；`getScanNodeProperties` sys handle 收口（path_partition_keys/schema_evolution dict 从 base 构）**推迟 T06**（设计 §10 归 T06）；② sys 分支**显式** `setFormatType(FORMAT_JNI)`（不依赖 generic node `jni` 默认）= 忠实 legacy `:290` + 可单测。
- **关键发现（非 DV，legacy parity）**：`$snapshots`/`$history` **忽略** `useSnapshot`（恒列当前 metadata 全量；legacy 同被 `SnapshotsTable` 忽略）；**`$files`** 才时间旅行可观测（列 pinned 快照 live 文件）→ time-travel UT 用 `$files`（S1=1 文件、latest=2）。
- **TDD**：carrier API stub（field/builder/getter）+ 6 UT〔carrier 2：normal-range 不发 serialized_split〔guard〕/ sys minimal-shape；provider 4：serializes-each-task / **deserialize-round-trip 经 BE 路**〔`deserializeFromBase64(...).asDataTask().rows()` 镜像 `IcebergSysTableJniScanner` = 最强 FE-可达 byte-shape parity 核〕/ time-travel〔`$files` 1 vs 2〕/ loads-inside-auth-scope〕→ RED〔4 真红 + 2 guard 共享路 trivially-pass〕→ GREEN。
- **mutation-check（Rule 9/12，4 处不相交，每次 `cp` 复原→diff IDENTICAL）**：**A** 去 `resolveSysTable` auth → `LoadsMetadataInsideTheAuthScope`〔authCount 1→0〕红〔证 auth guard 由 sys 分支变 mutation-detecting，纠 RED trivially-pass〕；**B** `metadataTable.newScan()` 替 `buildScan`〔丢 pin〕→ `HonorsTheSnapshotPin`〔`$files` 1→2〕红；**C** 删 FORMAT_JNI → carrier〔FORMAT_JNI→null〕红；**D** 删早 `return`〔fall-through 设 formatVersion〕→ carrier `isSetFormatVersion`〔false→true〕红。
- **验证（重跑 surefire 实证，Rule 12）**：`IcebergScanRangeTest` **19/0/0**〔17+2〕、`IcebergScanPlanProviderTest` **61/0/0**〔57+4〕；连接器全量 **527/0/1**〔40 类，=521 基线+6，python 聚合 XML〕；checkstyle 0〔validate phase BUILD SUCCESS〕；import-gate exit 0〔SDK `util.SerializationUtil`/`MetadataTableUtils` 允许〕；`CatalogFactory:51` 未改 → iceberg 仍**不在** `SPI_READY_TYPES`。**新 1 D（[D-065]）；无新 DV**。**live-e2e 未跑**（dormant，P6.8 兜底）。
- **⚠️ 潜伏（Rule 12，勿在 dormant 码 claim parity done）**：serialized `FileScanTask` 字节跨版本/classloader 兼容 BE `IcebergSysTableJniScanner`——FE deserialize-round-trip UT 已核同进程可消费，**跨版本/classloader interop FE 不可及**，P6.8 docker e2e 兜底。T07 预登记此潜伏 DV。
- **文档同步五步**：task 表（P6.5 phase 行 T01–T04→T01–T05 + T05 行 ⬜→✅ + T05 实现记录 + 下一步 T06）/ PROGRESS（§一 P6 行 + §二 board iceberg 行〔UT 521→527 双处〕 + §四 dynamics bullet）/ connectors/iceberg.md（完成度 + 进度日志 P6.5-T05）/ decisions-log（**新 [D-065]**）/ deviations-log（无新 DV）；HANDOFF 覆盖式。

---

# 🗺️ 代码脚手架（iceberg）

- **P6.5 sys-table 进度**：连接器增量全 dormant，**镜像 P5-paimon B4**——
  - `IcebergTableHandle`（`connector.iceberg`，**✅ T02**）：sys 变体 = `sysTableName`〔非 transient〕+ `forSystemTable(db,table,sys,snapshotId,ref,schemaId)`〔**保留** snapshot pin〕+ `isSystemTable()`/`getSysTableName()` + equals/hashCode/toString/withSnapshot 纳入/保留 sysTableName。
  - `IcebergConnectorMetadata`（**✅ T03 + T04**）：T03 = `listSupportedSysTables` + `getSysTableHandle`〔LAZY [D-063]〕+ `isSupportedSysTable`；T04 = 3 sys 分支〔2-arg/3-arg @snapshot `getTableSchema` / `getColumnHandles`〕+ `loadSysTable`〔`executeAuthenticated` 内 `MetadataTableUtils.createMetadataTableInstance`〕。
  - `IcebergScanPlanProvider`/`IcebergScanRange`（**✅ T05**）：sys split 路——`planScanInternal` sys guard→`planSystemTableScan`〔`resolveSysTable` 元数据表 + 复用 `buildScan`〕→`planFiles()`→`SerializationUtil.serializeToBase64`→`IcebergScanRange` serializedSplit；`populateRangeParams` sys 早返（serialized_split + FORMAT_JNI + row_count=-1）。**T06 待**：`getScanNodeProperties` sys 收口〔[D-065] 推迟项：现对 sys handle 走 base 表 `:585`/`:594-597`/`:631-639`〕+ thrift 描述符 hms↔iceberg 分叉核〔偏差⑥，legacy `IcebergSysExternalTable.toThrift:116-131`〕+ DESCRIBE/SHOW parity。
  - **fe-core 零改动**：`PluginDrivenSysExternalTable`/`PluginDrivenSysTable`/`SysTableResolver`/`PluginDrivenScanNode`/`PluginDrivenExternalTable.getSupportedSysTables` 全复用（paimon 已验证）。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/IcebergSysExternalTable`(177)〔`getSysIcebergTable():83-97` 懒构 `MetadataTableUtils` + `getOrCreateSchemaCacheValue:162-176` + `toThrift` hms/iceberg 分叉 `:116-131` + 类型 `ICEBERG_EXTERNAL_TABLE` `:57`〕 + `datasource/systable/IcebergSysTable`(82) + `IcebergScanNode` sys 路（`doGetSystemTableSplits:974-989` / `createIcebergSysSplit:899-905` / `setIcebergParams:285-295` / `getFileFormatType:1090-1091`）+ BE `IcebergSysTableJniScanner`〔`be-java-extensions/iceberg-metadata-scanner`，`:62` deserializeFromBase64 / `:87` asDataTask().rows()〕 + BE `be/src/format/table/iceberg_sys_table_jni_reader.cpp:37-42/62-63`。**现 iceberg sys-table 仍走 legacy**（连接器路 dormant，直到 P6.6）。
- **元数据列（P6.5 推迟，DV-041 同族）**：`datasource/iceberg/{IcebergMetadataColumn,IcebergRowId}` + 消费方 `IcebergConflictDetectionFilterUtils`/`IcebergNereidsUtils`/`IcebergRowLevelDmlTransform`（全 DML，挂 nereids `instanceof IcebergExternalTable` 钩子）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→假错**）；offline 加 `-o`；连接器 = `fe-connector-iceberg`、SPI = `fe-connector-api`、dispatch = `fe-core`、arg 框架 = `fe-foundation`。checkstyle 在 `validate` phase 跑（**先于 compile/test——checkstyle 挂则无 surefire**）；验证读 surefire **XML**（聚合用 python ET）。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`**。**⚠️ 全量验证前 `rm -f target/surefire-reports/TEST-*.xml`**（单方法/单类跑留单测 XML 污染聚合）。
- **iceberg 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）。**T05 实测**：warm cache 单类 ~20s、全量 ~20s。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`；SDK `org.apache.iceberg.*`〔含 `MetadataTableUtils`/`MetadataTableType`/`util.SerializationUtil`〕允许）。测试：连接器侧无 Mockito（fail-loud fake + 真 `InMemoryCatalog`；`RecordingIcebergCatalogOps`〔`loadTable` 返 `.table`，记 `lastLoadDb/lastLoadTable`/`log`〕/`RecordingConnectorContext`〔`authCount`/`failAuth`〕；`FakeIcebergTable` **非** `HasTableOperations`，`createMetadataTableInstance` 用不了）；fe-core dispatch 侧用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- **mutation-check（Rule 9/12，验证测试 pin 了意图）**：dormant 路里「测试是否真 pin 某行」非显然——临时删/改该行、单跑该测、确认转红、再恢复（每 task 至少一坑）。**坑①**：guard 测（auth-scope / empty 期望 / 共享路）对未走 sys 分支的 RED 阶段 trivially-pass → 必经 mutation-check 验真（T05 实证：auth-scope 测 RED 阶段过、加 sys 分支后才 mutation-detecting）。**坑②**：变异行若超 120 列 → checkstyle 先挂、无 surefire（用 length-safe 变体）。**坑③**：iceberg `for (X ignored : it)` 触 checkstyle `UnusedLocalVariable` → 用显式 `Iterator` while-loop。
- cwd 跨 Bash 持久；一律绝对路径（heredoc `cd` 会改 cwd）。**`cp` 复原务必用绝对路径**（T05 实证：相对路径在错 cwd 静默失败）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。本 session 只动 2 产品/测 + 5 文档（task/PROGRESS/iceberg.md/decisions-log/HANDOFF）。
- message `[refactor](catalog) P6.5 iceberg: T05 — <subj>` + 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。**注意 T01–T06 + arg-move 已推 origin**（rebase/force-push 须谨慎；P6.4 T07/T08/T09 + P6.5 T01–T05 一并待 push）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + 实证）再信文档。**T05 实证**：设计/recon agent 误称 legacy sys 表「无 time-travel」，直读 `IcebergScanNode.createTableScan()` 才纠正（sys 表确 honor）；`$snapshots` 忽略 useSnapshot 而 `$files` honor（time-travel UT 须用 `$files`）。
- **大文件用 subagent/workflow 总结**（playbook §3.1）。PROGRESS.md 巨行（尤 §一 P6 行 + §二 board 行〔UT 计数双处易遗漏〕），编辑前用 Read 取精确行再 surgical Edit（Edit 前必先 Read 该文件）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/P6-iceberg-migration.md` 状态 + 实现记录 + `PROGRESS.md`〔§一 + §二 board〔**UT 计数双处**〕 + §四 dynamics〕 + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**。DV 中央登记延后到 T07 批量。
- **T06 起步先 recon**（grep 连接器现 `buildTableDescriptor`/`toThrift` 路 + 是否已对 base 表做 hms↔iceberg 分叉〔P6.1/P6.2 可能已建〕；legacy `IcebergSysExternalTable.toThrift:116-131` hms/iceberg 分叉字节形状 + `IcebergScanPlanProvider.getScanNodeProperties:585/594-597/631-639` sys handle 现走 base 表；`IcebergSchemaUtils.encodeSchemaEvolutionProp` 对 sys handle 是否抛）再写码。决策 A/B/[D-063]/[D-064]/[D-065] 已定，**无需再问**。
- **faithfulness 对抗 workflow 范式**：收口/汇总设计（T08）若引行号/commit/UT 计数必跑此 wf；**UT 计数 claim 必重跑 surefire 实证**（`@Test` 计数不能证绿，Rule 12）。**serialized-split 字节潜伏高危**——FE UT 不可及，勿 claim parity done（P6.8 e2e 兜底）。
