# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.5-T03 = `IcebergConnectorMetadata.listSupportedSysTables` + `getSysTableHandle`**（TDD）

**P6.5-T02 = ✅ DONE**（`IcebergTableHandle` sys 变体，TDD，单文件 + 9 UT，**未 push**）。**两个设计方向已用户签字**（进 T02 前 AskUserQuestion）：**决策 A = `forSystemTable` 保留 snapshot pin**（偏差①）；**决策 B = 无新 seam**（复用 `loadTable` + 连接器内 `MetadataTableUtils`）。T03 起照此实现，**无需再问**。

- **T03 内容**（连接器 `IcebergConnectorMetadata`，`connector.iceberg`，全 dormant）：
  - `listSupportedSysTables(session, baseHandle)` override = `MetadataTableType.values()` 去 `POSITION_DELETES` → 小写名 list（连接器-global，防御性 unmodifiable copy；镜像 legacy 静态集）。
  - `getSysTableHandle(session, baseHandle, sysName)` override = `isSupportedSysTable(sysName)` guard（大小写不敏感遍历 `MetadataTableType.values()` 去 POSITION_DELETES，私有，镜像 paimon `:398-408`）否则 `Optional.empty()`（含 position_deletes / null / unknown，Q2）；支持名 → `IcebergTableHandle.forSystemTable(base.getDbName(), base.getTableName(), sys, base.getSnapshotId(), base.getRef(), base.getSchemaId())`〔**决策 A 保留 pin** + T02 已就位的工厂〕。
  - **决策 B 落点**：metadata-table 构建 = `MetadataTableUtils.createMetadataTableInstance(base, MetadataTableType.from(sys))`，在 `context.executeAuthenticated` 内（base 加载复用 `catalogOps.loadTable` + meta 构建）；**无新 `IcebergCatalogOps` seam**。懒 vs eager 校验由实现 recon 定（设计 §5 留两选）。
  - **seam-identity UT**（无 4-arg Identifier 可捕获）：用 `RecordingIcebergCatalogOps` 捕获加载的 base 表身份 + 传入的 `MetadataTableType` + 「在 `executeAuthenticated` 内」（`RecordingConnectorContext`）。fixture **已存在**（T01 recon 实证）。
- **关键先读**：`designs/P6.5-T01-systable-design.md` §5（`getSysTableHandle`/`getTableSchema` seam 形态，**含决策 B 偏差③**）+ §3/§7 + recon `research/p6.5-iceberg-systable-recon.md`。代码模板：`PaimonConnectorMetadata.getSysTableHandle`/`listSupportedSysTables`/`isSupportedSysTable`（paimon 连接器，sys-table 先例）+ `IcebergConnectorMetadata`（现状，找 `executeAuthenticated`/`loadTable` 用法）+ `ConnectorTableOps.java:51-67`（SPI default-empty 两方法）。T02 产物 `IcebergTableHandle.forSystemTable`（已实现，T03 直接调）。
- **节奏**（设计 §10）：T03 `getSysTableHandle` → T04 `getTableSchema` sys 分支（parse metadata-table schema + mapping flag 透传，偏差⑤）→ T05 `IcebergScanPlanProvider`/`IcebergScanRange` sys split 路（FORMAT_JNI + serialized FileScanTask + time-travel useSnapshot/useRef，偏差①②）→ T06 thrift hms 分叉 + DESCRIBE/SHOW parity 核 → T07 parity 审计 + DV 中央登记 → T08 收口（= P6.5 DONE）。**全程不碰 `SPI_READY_TYPES`，dormant 至 P6.6**。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.5 = T01/T02 done，T03–T08 待）。翻闸前必修下述阻塞（**同需读/写路径共享 fe-core seam 的 holistic 修**）：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。**⚠️ P6.5 新增挂靠**：iceberg **元数据列**（`IcebergMetadataColumn`/`IcebergRowId`，P6.5 用户签字推迟）属此族——挂 nereids `instanceof IcebergExternalTable` + `getFullSchema()` 钩子、不受 `SPI_READY_TYPES` 控制、flip 后表变 `PluginDrivenExternalTable`→钩子失效→DML row-id 注入断，须在此 holistic 修一并长出（无 paimon 模板）。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-045]**（写路径，P6.4-T06 实证）= `rewrite_data_files` **执行半翻闸接线**（R-B，专门写路径 RFC）。① 事务半（`IcebergConnectorTransaction` REWRITE 变体）+ 规划半（`RewriteDataFilePlanner`）已建 dormant；**②③④ 执行半留 fe-core**。recon 证伪设计 §5 / D-062 R-A「从 pinned snapshot+WHERE 重规划」前提（连接器 scan SPI 无法表达 legacy bin-pack「分区内任意文件子集」→ over-scan **破坏 rewrite 正确性**；`FileScanTask` 侧信道翻闸后死；SPI 模块边界禁连接器 `RewriteDataGroup` 跨回 fe-core；multi-sink-per-txn 须重设计）。**用户裁 Option 1**：① 已做（dormant），②③④ 推后。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057`→改绑 `UnboundConnectorTableSink` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager`。= **DV-041 写路径阻塞同族**。

**[pre-flip 行为偏差中央登记]**：P6.4 = [DV-046]（correctness-bearing：auth-add Kerberos + DV-T05r-where）+ [DV-047]（perf-cosmetic 批）。**P6.5 预登记**（T07 批量）：sys 表类型变更 `ICEBERG_EXTERNAL_TABLE→PLUGIN_EXTERNAL_TABLE` / position_deletes 文案专属→通用 not-found / thrift hms↔iceberg 分叉 / serialized FileScanTask 字节形状潜伏（P6.8 e2e 兜底）。**注**：T02 偏差①（sys handle 保留 pin）**非** pre-flip 行为偏差——legacy `IcebergSysExternalTable` 同样 honor 时间旅行，是 parity-保留的内部设计选择，**不登记 DV**。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure 三路 dormant + sys-table 在建，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**T01–T06 + arg-move 已推**；**P6.4 T07/T08/T09 + P6.5 T01/T02 待 push**（T02 commit 后 `git rev-list --count origin..HEAD`=5）。**用户未要求 push**——留用户裁量。
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，iceberg 389/0/1 + fe-core 30/0）。**P6.4 = ✅ DONE**（T01–T09，iceberg **494/0/1** + fe-core `ConnectorExecuteActionTest` **14/0**）。**P6.5 = 🔵 进行中**（T01 ✅ recon+设计+用户二签字；**T02 ✅ `IcebergTableHandle` sys 变体，iceberg 503/0/1**；T03–T08 待）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.5-T02（`IcebergTableHandle` sys 变体，TDD），1 commit（单文件产品 + UT + 文档同步），待 push

- **进 T02 前用户二次签字（AskUserQuestion）**：决策 A = `forSystemTable` **保留** snapshot/ref/schemaId pin（≠ paimon 清零，偏差①）；决策 B = **无新 seam**（T03 复用 `loadTable` + 连接器内 `MetadataTableUtils`）——均选推荐。
- **改动 = 单文件 `IcebergTableHandle.java`（连接器，dormant）+ 其 UT**：加 `private final String sysTableName`（**非 transient**，小写 bare 名，`null`=普通表）+ `forSystemTable(db,table,sysName, long snapshotId,String ref,long schemaId)`〔保留 pin〕+ `isSystemTable()`/`getSysTableName()`；`equals`/`hashCode`/`toString` 纳入 `sysTableName`（snapshot 字段已在身份内→`t$snapshots@v1`≠`@v2`≠`t`）；`withSnapshot` **保留** `sysTableName`（copy 工厂不退化 sys→普通，镜像 paimon `withScanOptions`/`withBranch`）。
- **设计偏差修正（Rule 7/12，对照实码）**：设计 §4 工厂签名写 boxed `Long/Integer`，实码字段是 primitive `long`（`NO_PIN=-1L` sentinel）→ 实现用 `long` 对齐既有风格（conformance）。
- **TDD**：9 UT 先 RED（test-compile `cannot find symbol forSystemTable/isSystemTable/getSysTableName`，证缺特性非笔误）→ GREEN → **mutation-check**（坑：`forSystemTable` 清 pin→`IcebergTableHandleTest` 4 红〔`forSystemTableRetainsSnapshotPin`/`RetainsRefPin`/`sysHandleAtDifferentVersionsAreDifferent`/`SurvivesJavaSerializationRoundTrip`〕→复绿，证测真 pin 偏差① 不变式）。
- **验证（重跑 surefire 实证，非凭 `@Test` 计数，Rule 12）**：`IcebergTableHandleTest` **14/0/0**（5 旧 + 9 新，方法名核 XML）；连接器全量 **503/0/1**（39 类，=494 基线 + 9）；checkstyle 0；import-gate exit 0；`CatalogFactory` 未改 → iceberg 仍**不在** `SPI_READY_TYPES`。**无新 D / DV**（DV 延后 T07）。**live-e2e 未跑**（dormant 不达 live，P6.8 兜底）。
- **文档同步五步**：task 表（T02 行 ⬜→✅ + T02 实现记录）/ PROGRESS（header §一 P6 行 + §二 board iceberg 行〔UT 494→503〕 + §四 dynamics bullet）/ connectors/iceberg.md（完成度 + 进度日志 P6.5-T02）/ decisions-log（无新 D）/ deviations-log（无新 DV）；HANDOFF 覆盖式。

---

# 🗺️ 代码脚手架（iceberg）

- **P6.5 sys-table 进度**：连接器增量全 dormant，**镜像 P5-paimon B4**——
  - `IcebergTableHandle`（`connector.iceberg`，**✅ T02 已实现**）：sys 变体 = `sysTableName`〔非 transient〕+ `forSystemTable`〔**保留** snapshot pin〕+ `isSystemTable()`/`getSysTableName()` + equals/hashCode/toString/withSnapshot 纳入/保留 sysTableName。
  - `IcebergConnectorMetadata`（**T03–T04 待**）：加 `listSupportedSysTables`（`MetadataTableType.values()` 去 `position_deletes`，小写）+ `getSysTableHandle`（`isSupportedSysTable` guard + `executeAuthenticated` 内 `MetadataTableUtils.createMetadataTableInstance`，**无新 seam**=决策 B）+ `getTableSchema` sys 分支（parse metadata-table schema + mapping flag）。
  - `IcebergScanPlanProvider`/`IcebergScanRange`（**T05 待**）：sys split 路（`scan.useSnapshot/useRef` + `planFiles()` → `SerializationUtil.serializeToBase64` → `TIcebergFileDesc.setSerializedSplit` + FORMAT_JNI）。**唯一全新一块**（连接器有 FORMAT_JNI 默认、缺 serialized-split 发射）。
  - **fe-core 零改动**：`PluginDrivenSysExternalTable`/`PluginDrivenSysTable`/`SysTableResolver`/`PluginDrivenScanNode`/`PluginDrivenExternalTable.getSupportedSysTables` 全复用（paimon 已验证）。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/IcebergSysExternalTable`(177)〔`MetadataTableUtils` + `IcebergUtils.parseSchema` + `toThrift` hms/iceberg〕 + `datasource/systable/IcebergSysTable`〔`SUPPORTED_SYS_TABLES` = enum 去 POSITION_DELETES + `UNSUPPORTED_POSITION_DELETES_TABLE`〕 + `IcebergScanNode` sys 路（`doGetSystemTableSplits:974-989`/`setScanParams:287-295`/FORMAT_JNI `:1091`）+ BE `IcebergSysTableJniScanner`〔`asDataTask().rows()`〕。**现 iceberg sys-table 仍走 legacy**（连接器路 dormant：iceberg 表是 `IcebergExternalTable` 非 `PluginDrivenExternalTable`，直到 P6.6）。
- **元数据列（P6.5 推迟，DV-041 同族）**：`datasource/iceberg/{IcebergMetadataColumn,IcebergRowId}` + 消费方 `IcebergConflictDetectionFilterUtils`/`IcebergNereidsUtils`/`IcebergRowLevelDmlTransform`（全 DML，挂 nereids `instanceof IcebergExternalTable` 钩子）。
- **P6.4 终态**（procedure SPI + actions + rewrite 规划/事务半 + dispatch rewire）见 `git log` / `connectors/iceberg.md`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`DependencyResolutionException`/假错**）；offline 加 `-o`；连接器 = `fe-connector-iceberg`、SPI = `fe-connector-api`、dispatch = `fe-core`、arg 框架 = `fe-foundation`。checkstyle 在 `validate` phase 跑；`-q` 抑制 BUILD SUCCESS 行（grep `BUILD SUCCESS` 会落空，**读 surefire XML 才是实证**）；验证读 surefire **XML**（`<testsuite tests=.. failures=..>`）。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现。**⚠️ 单方法/单类跑会留单测 XML 污染聚合计数**——全量验证前 `rm -f target/surefire-reports/TEST-*.xml` 再跑。**⚠️ gensrc/version-build 阶段 ANTLR `mismatched input '->'` + `which` 噪声非 javac 错、不影响 exit 0**。
- **⚠️ 勿并发跑两个 `-am clean` 构建**（争抢共享上游 `fe-foundation` target → 互 corrupt 假错）。**顺序跑**。**⚠️ 后台 Bash 勿用内层 `( mvn ... ) &`**（双重 background→wrapper 立返 exit 0 假信号）；直接 `run_in_background: true` 跑裸 mvn。**⚠️ 勿 `| tail -N` 管道**（丢根因 + `PIPESTATUS[0]` 才是 maven 真 exit）。
- **iceberg 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）；test-compile 足够验 RED（缺符号编译错）。**fe-core 用 `test`**（含 `-am`；编译上游约 6–7min，给足超时或后台跑）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`；SDK `org.apache.iceberg.*`〔含 `MetadataTableUtils`/`MetadataTableType`/`SerializationUtil`〕允许）。测试：连接器侧无 Mockito（fail-loud fake + `InMemoryCatalog`；fixture `RecordingIcebergCatalogOps`/`RecordingConnectorContext`/`action/ActionTestTables`）；fe-core dispatch 侧用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- **mutation-check（验证测试 pin 了意图，Rule 9/12）**：dormant 路里「测试是否真 pin 某行」非显然——临时删/改该行、单跑该测、确认转红、再恢复（每 task 至少一坑）。T02 实证：`forSystemTable` 清 pin → 4 红。
- cwd 跨 Bash 持久；一律绝对路径（heredoc `cd` 会改 cwd → 后续相对路径失效）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。本 session 只动 2 产品/测 + 4 文档（PROGRESS/iceberg.md/task/HANDOFF）。
- message `[refactor](catalog) P6.5 iceberg: T02 — <subj>`（产品码改用 `[refactor]`/`[feature]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。**注意 T01–T06 + arg-move 已推 origin**（rebase/force-push 须谨慎；P6.4 T07/T08/T09 + P6.5 T01/T02 一并待 push）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。T02 实证：`IcebergTableHandle` 用 primitive `long`（非设计 §4 写的 boxed `Long/Integer`）；该 handle **无** transient SDK Table（≠ paimon），metadata-table 懒构（T03 决策 B）；pin 已在 equals/hashCode 身份内（≠ paimon 排除 scanOptions）。
- **大文件用 subagent 总结**（playbook §3.1）。PROGRESS.md 巨行，编辑前用 Read 取精确行再 surgical Edit。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/P6-iceberg-migration.md` 状态 + `PROGRESS.md`〔header §一 + §二 board〔**UT 计数易遗漏**〕 + §四 dynamics〕 + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**。DV 中央登记延后到 T07 批量。
- **faithfulness 对抗 workflow 范式**：收口/汇总设计（T08）若引行号/commit/UT 计数必跑此 wf；**UT 计数 claim 必重跑 surefire 实证**（`@Test` 计数不能证绿，Rule 12）。**绝不 overclaim**。recon（T01）用 4-reader + synthesize + completeness critic。
- **T03 设计方向已定**（用户签 A 保留 pin / B 无新 seam）——**无需再问**，直接 TDD 实现；逐 task 起步先 recon（grep paimon 模板 + 现状 IcebergConnectorMetadata）再写码。
