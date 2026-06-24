# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.5-T05 = `IcebergScanPlanProvider`/`IcebergScanRange` sys split 路**（TDD）

**P6.5-T04 = ✅ DONE**（`IcebergConnectorMetadata` 3 处 sys 分支〔2-arg `getTableSchema` / 3-arg `getTableSchema(@snapshot)` 短路 / `getColumnHandles`〕+ 新 helper `loadSysTable`，TDD，单文件产品 + 同测试类 +7 测，连接器 **521/0/1**，**未 push**）。决策本轮裁定 **[D-064]**（无需用户签字，HANDOFF 预授权「可能并入 T04 或留 T05」）= getColumnHandles sys 分支 + 3-arg @snapshot sys 短路**并入 T04**（同 `loadSysTable` helper、同潜伏 bug、paimon 模板亦配对 schema+columns）；@snapshot sys 行为 = meta-table schema 与快照无关（legacy 无 schema-at-snapshot for sys，时间旅行 pin 选 SCAN 行非 schema，落 T05）。决策 A（保留 snapshot pin，偏差①）+ 决策 B（无新 seam）不变。

- **T05 内容**（连接器 `IcebergScanPlanProvider`/`IcebergScanRange`，全 dormant）= **sys handle 的扫描计划走 serialized-split + FORMAT_JNI 路**（**P6.5 唯一全新一块**——连接器已有 FORMAT_JNI 默认 + `useSnapshot`/`useRef` time-travel，**缺 serialized FileScanTask 发射**）：
  - sys handle → `scan.planFiles()`（time-travel `useSnapshot`/`useRef` 已就绪，`IcebergScanPlanProvider:269-281`，偏差①②）→ 每个 `FileScanTask` 经 `SerializationUtil.serializeToBase64(task)` → `TIcebergFileDesc.setSerializedSplit(...)` + FORMAT_JNI。镜像 legacy `IcebergScanNode`：`doGetSystemTableSplits:974-989` + `setScanParams:279-292`〔`setFormatType(FORMAT_JNI)` + `fileDesc.setSerializedSplit(...)`〕+ `SerializationUtil.serializeToBase64(fileScanTask):902` + FORMAT_JNI `:1091`。
  - **关键先读**（连接器现状已埋钩，找「lands in a later task」注释）：`IcebergScanPlanProvider.java:582`〔「The serialized-table key (JNI system-table path) lands in a later task.」= T05 落点〕+ `:376`〔sys 表 FORMAT_JNI 注释〕+ `:331/735/738`〔`planFiles()`〕+ `:269-281`〔useRef/useSnapshot〕；`IcebergScanRange.java:228`〔「native reader format (JNI = system tables, P6.5)」〕+ `:366`〔serialized split 注释〕。BE 消费方 = `IcebergSysTableJniScanner`〔`asDataTask().rows()`〕。
  - **⚠️ 高潜伏风险（Rule 12，勿在 dormant 码 claim parity done）**：连接器发射的 serialized `FileScanTask` 字节须与 legacy 字节兼容 BE `IcebergSysTableJniScanner`——**FE UT 不可及**，P6.8 docker e2e 兜底。T07 预登记此潜伏 DV。
  - **TDD + 测试基建**：复用 T04 的真 `InMemoryCatalog` 表（`createMetadataTableInstance` 需真 `BaseTable`，`FakeIcebergTable` 不够——T04 已踩坑）；断言 = sys handle 产 split 带 `serialized_split` + FORMAT_JNI、time-travel handle 的 scan 用 `useSnapshot`/`useRef`。**mutation-check 必验**（dormant 路）。
- **节奏**（设计 §10）：T05（scan sys split 路）→ T06（thrift hms↔iceberg 分叉核 `buildTableDescriptor` for sys handle，偏差⑥ + DESCRIBE/SHOW parity 核）→ T07（parity 审计 + DV 中央登记 + 对抗 parity workflow）→ T08（收口/汇总设计 + faithfulness 对抗验证 wf + gate 重跑 = **P6.5 DONE**）。**全程不碰 `SPI_READY_TYPES`，dormant 至 P6.6**。T05/T06 视耦合可合并（设计 §10 注：T06 若 thrift 分叉无 gap 可并入 T05）。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.5 = T01/T02/T03/T04 done，T05–T08 待）。翻闸前必修下述阻塞（**同需读/写路径共享 fe-core seam 的 holistic 修**）：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。**⚠️ P6.5 新增挂靠**：iceberg **元数据列**（`IcebergMetadataColumn`/`IcebergRowId`，P6.5 用户签字推迟）属此族——挂 nereids `instanceof IcebergExternalTable` + `getFullSchema()` 钩子、不受 `SPI_READY_TYPES` 控制、flip 后表变 `PluginDrivenExternalTable`→钩子失效→DML row-id 注入断，须在此 holistic 修一并长出（无 paimon 模板）。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-045]**（写路径，P6.4-T06 实证）= `rewrite_data_files` **执行半翻闸接线**（R-B，专门写路径 RFC）。① 事务半（`IcebergConnectorTransaction` REWRITE 变体）+ 规划半（`RewriteDataFilePlanner`）已建 dormant；**②③④ 执行半留 fe-core**。recon 证伪设计 §5 / D-062 R-A「从 pinned snapshot+WHERE 重规划」前提（连接器 scan SPI 无法表达 legacy bin-pack「分区内任意文件子集」→ over-scan **破坏 rewrite 正确性**；`FileScanTask` 侧信道翻闸后死；SPI 模块边界禁连接器 `RewriteDataGroup` 跨回 fe-core；multi-sink-per-txn 须重设计）。**用户裁 Option 1**：① 已做（dormant），②③④ 推后。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057`→改绑 `UnboundConnectorTableSink` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager`。= **DV-041 写路径阻塞同族**。

**[pre-flip 行为偏差中央登记]**：P6.4 = [DV-046]（correctness-bearing：auth-add Kerberos + DV-T05r-where）+ [DV-047]（perf-cosmetic 批）。**P6.5 预登记**（T07 批量）：sys 表类型变更 `ICEBERG_EXTERNAL_TABLE→PLUGIN_EXTERNAL_TABLE` / position_deletes 文案专属→通用 not-found / thrift hms↔iceberg 分叉 / **serialized FileScanTask 字节形状潜伏（T05 新增，P6.8 e2e 兜底）**。**注**：T02 偏差①（sys handle 保留 pin）+ T03 [D-063]（lazy 解析）+ T04 [D-064]（@snapshot sys 短路 + getColumnHandles 并入）**均非** pre-flip 行为偏差——legacy 同样 honor 时间旅行 + 懒构 metadata-table + meta-table schema 不随快照变，是 parity-保留/更忠实的内部设计选择，**不登记 DV**。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure 三路 dormant + sys-table getSchema/getColumns 已建、scan split 在建，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（P6.4-T06）；**T01–T06 + arg-move 已推**；**P6.4 T07/T08/T09 + P6.5 T01/T02/T03/T04 待 push**（T04 commit 后 `git rev-list --count origin..HEAD`=7）。**用户未要求 push**——留用户裁量。
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，iceberg 389/0/1 + fe-core 30/0）。**P6.4 = ✅ DONE**（T01–T09，iceberg **494/0/1** + fe-core `ConnectorExecuteActionTest` **14/0**）。**P6.5 = 🔵 进行中**（T01 ✅ recon+设计+二签字；T02 ✅ `IcebergTableHandle` sys 变体；T03 ✅ `IcebergConnectorMetadata` 2 override〔list/getSysHandle〕；**T04 ✅ `getTableSchema`/`getColumnHandles` sys 分支，iceberg 521/0/1**；T05–T08 待）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.5-T04（`getTableSchema` + `getColumnHandles` sys 分支，TDD），1 commit（单文件产品 + 同测试类 +7 测 + 5 文档），待 push

- **改动 = 单文件产品 `IcebergConnectorMetadata.java`（连接器，dormant）+ 同测试类 `IcebergConnectorMetadataSysTableTest` +7 测**。镜像 paimon `getTableSchema`/`getColumnHandles` sys 分支，但 iceberg 无 paimon 4-arg sys Identifier / transient SDK Table——sys 表经 `MetadataTableUtils` 从 base 懒构（决策 B 无新 seam，偏差③）。**SDK = iceberg 1.10.1**（`fe/pom.xml:348`；`MetadataTableType.from` = `valueOf(toUpperCase(Locale.ROOT))`，大小写不敏感、unknown→null。**注：T03 旧记录写「SDK 1.6.1」是过时标签——实际编译版本 1.10.1；count 15 不变**）。
- **产品 3 sys 分支 + 1 helper（[D-064]）**：
  - **新 `loadSysTable(handle)`** = `context.executeAuthenticated(() -> { Table base = catalogOps.loadTable(db, table); return MetadataTableUtils.createMetadataTableInstance(base, MetadataTableType.from(handle.getSysTableName())); })`〔base 加载 + meta 构建在**同一** auth scope，Kerberos UGI 覆盖远程 base 加载；镜像 legacy `IcebergSysExternalTable.getSysIcebergTable`〕。`getSysTableName()` 是 `getSysTableHandle` 已验证的小写名→`from` 永不 null。唯一新 import `org.apache.iceberg.MetadataTableUtils`。
  - **2-arg `getTableSchema`**：`if (iceHandle.isSystemTable())` → `buildTableSchema(tableName, loadSysTable(h), sysTable.schema())`〔复用既有 `buildTableSchema`→`parseSchema`，mapping flag 从 `properties` 自动透传，偏差⑤——**已核实** `parseSchema:530-535` 从 `properties.getOrDefault` 读〕。镜像 legacy `getOrCreateSchemaCacheValue:162-176`。
  - **3-arg `getTableSchema(@snapshot)` sys 短路**：cast 提到顶，`if (iceHandle.isSystemTable()) return getTableSchema(session, handle)`〔meta-table schema 固定、与快照/schema-version 无关——legacy 无 schema-at-snapshot for sys；时间旅行 pin〔偏差①〕选 SCAN 行非 schema，落 T05。否则 sys handle 携 schemaId≥0 会误入 base 路 `table.schemas().get(schemaId)`〕。
  - **`getColumnHandles` sys 分支**：`Table table = iceHandle.isSystemTable() ? loadSysTable(iceHandle) : loadTable(iceHandle);`〔通用 `PluginDrivenScanNode.buildColumnHandles` 按名查 slot，sys handle 必返 meta-table 列；与 getTableSchema 同潜伏 bug、共享 helper〕。
- **测试基建坑（recon 实证，T05 复用）**：`MetadataTableUtils.createMetadataTableInstance(base, type)` 需 `base` 是 `HasTableOperations`（真 `BaseTable`）——`FakeIcebergTable` **不是**（仅 `implements Table`、无 `operations()`）→ 抛。故 base = **真 `InMemoryCatalog` 表**（`new InMemoryCatalog().createTable(...)` 返 `BaseTable`），经 `RecordingIcebergCatalogOps.table` 注入 seam〔base 列 `id/name` 故意 ≠ 任何 meta 列，读错 schema 可检〕；空表足够读 meta-table `.schema()`（静态、无需快照）。
- **TDD**：先写 7 UT（RED 实证：5 真红〔snapshots/history 列、mapping-flag committed_at 缺、@snapshot meta 列、getColumnHandles meta 列——current 产品对 sys handle 返 base 列 `[id,name]`〕+ 2 auth-scope guard〔对共享 auth 路 trivially-pass，mutation-check 验真〕）→ 实现（GREEN）。
- **mutation-check（Rule 9/12，guard 测必验真；每次 `cp` 复原→diff IDENTICAL）**：**A** 去 `loadSysTable` auth 包裹 → `LoadsBaseInsideAuthScope`〔authCount 0≠1〕+`RunsInsideAuthenticator`〔failAuth 不挡 load→log 非空〕双红；**B** load 用 `getSysTableName()` 替 `getTableName()` → `LoadsBaseInsideAuthScope`〔lastLoadTable "snapshots"≠"t1"〕红〔A 经 authCount 红、B 经 name 红，互补〕；**C** 硬编码 `MetadataTableType.SNAPSHOTS` → `UsesSysNameTypeNotHardcoded`〔history 期望 made_current_at 得 committed_at〕红、snapshots 测仍绿〔证 `from(getSysTableName())` 线〕。**坑**：初版 B 把 load 名改 `getTableName()+"$"+sys` 致行 126>120 → checkstyle LineLength 挂、build 失败未达测（无 surefire）——必须用 length-safe 变体重跑。
- **验证（重跑 surefire 实证，非凭 `@Test` 计数，Rule 12）**：`IcebergConnectorMetadataSysTableTest` **18/0/0**〔11 T03 + 7 T04，方法名核 XML〕；连接器全量 **521/0/1**〔40 类，= 514 基线 + 7，python 聚合 XML〕；checkstyle 0〔validate phase BUILD SUCCESS〕；import-gate exit 0〔SDK `MetadataTableUtils` 允许〕；`CatalogFactory:51` 未改 → iceberg 仍**不在** `SPI_READY_TYPES`。**新 1 D（[D-064]）；无新 DV**（DV 延后 T07 批量）。**live-e2e 未跑**（dormant 不达 live，P6.8 兜底）。
- **文档同步五步**：task 表（P6.5 phase 行 T01–T03→T01–T04 + T04 行 ⬜→✅ + T04 实现记录 + 下一步 T05）/ PROGRESS（header §一 P6 行 + §二 board iceberg 行〔UT 514→521 双处〕 + §四 dynamics bullet）/ connectors/iceberg.md（完成度 + 进度日志 P6.5-T04）/ decisions-log（**新 [D-064]**）/ deviations-log（无新 DV）；HANDOFF 覆盖式。

---

# 🗺️ 代码脚手架（iceberg）

- **P6.5 sys-table 进度**：连接器增量全 dormant，**镜像 P5-paimon B4**——
  - `IcebergTableHandle`（`connector.iceberg`，**✅ T02**）：sys 变体 = `sysTableName`〔非 transient〕+ `forSystemTable`〔**保留** snapshot pin〕+ `isSystemTable()`/`getSysTableName()` + equals/hashCode/toString/withSnapshot 纳入/保留 sysTableName。
  - `IcebergConnectorMetadata`（**✅ T03 + T04**）：T03 = `listSupportedSysTables`〔`MetadataTableType.values()` 去 `position_deletes` 小写 unmodifiable〕+ `getSysTableHandle`〔guard + 保留 pin，LAZY 零 catalog 往返 [D-063]〕+ 私有 `isSupportedSysTable`。**T04** = 3 sys 分支〔2-arg `getTableSchema` / 3-arg `getTableSchema(@snapshot)` 短路 / `getColumnHandles`〕+ 新 `loadSysTable`〔`executeAuthenticated` 内 `MetadataTableUtils.createMetadataTableInstance`，决策 B 无新 seam〕+ import `MetadataTableUtils`。
  - `IcebergScanPlanProvider`/`IcebergScanRange`（**T05 待**）：sys split 路（`scan.useSnapshot/useRef` 已就绪〔`IcebergScanPlanProvider:269-281`〕→ `planFiles()` → `SerializationUtil.serializeToBase64` → `TIcebergFileDesc.setSerializedSplit` + FORMAT_JNI）。**唯一全新一块**（连接器有 FORMAT_JNI 默认 + time-travel，**缺** serialized-split 发射）。现状埋钩注释：`IcebergScanPlanProvider:582`〔「lands in a later task」〕/ `:376` / `IcebergScanRange:228/366`。
  - **fe-core 零改动**：`PluginDrivenSysExternalTable`/`PluginDrivenSysTable`/`SysTableResolver`/`PluginDrivenScanNode`/`PluginDrivenExternalTable.getSupportedSysTables` 全复用（paimon 已验证）。**契约实证**：`PluginDrivenSysExternalTable.resolveConnectorTableHandle` 先 `getTableHandle`（base 预检）再 `getSysTableHandle`。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/IcebergSysExternalTable`(177)〔`getSysIcebergTable():83-97` **懒构** `MetadataTableUtils` + `getOrCreateSchemaCacheValue:162-176` `IcebergUtils.parseSchema` + `toThrift` hms/iceberg 分叉 `:116-131`〕 + `datasource/systable/IcebergSysTable`(82)〔`SUPPORTED_SYS_TABLES` = enum 去 POSITION_DELETES 小写 map〕 + `IcebergScanNode` sys 路（`doGetSystemTableSplits:974-989` / `setScanParams:279-292`〔FORMAT_JNI + setSerializedSplit〕 / `serializeToBase64:902` / FORMAT_JNI `:1091`）+ BE `IcebergSysTableJniScanner`〔`asDataTask().rows()`〕。**现 iceberg sys-table 仍走 legacy**（连接器路 dormant：iceberg 表是 `IcebergExternalTable` 非 `PluginDrivenExternalTable`，直到 P6.6）。
- **元数据列（P6.5 推迟，DV-041 同族）**：`datasource/iceberg/{IcebergMetadataColumn,IcebergRowId}` + 消费方 `IcebergConflictDetectionFilterUtils`/`IcebergNereidsUtils`/`IcebergRowLevelDmlTransform`（全 DML，挂 nereids `instanceof IcebergExternalTable` 钩子）。
- **P6.4 终态**（procedure SPI + actions + rewrite 规划/事务半 + dispatch rewire）见 `git log` / `connectors/iceberg.md`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`DependencyResolutionException`/假错**）；offline 加 `-o`；连接器 = `fe-connector-iceberg`、SPI = `fe-connector-api`、dispatch = `fe-core`、arg 框架 = `fe-foundation`。checkstyle 在 `validate` phase 跑（**先于 compile/test——checkstyle 挂则无 surefire**）；`-q` 抑制 BUILD SUCCESS 行；验证读 surefire **XML**（聚合用 python ET）。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现。**⚠️ 单方法/单类跑会留单测 XML 污染聚合计数**——全量验证前 `rm -f target/surefire-reports/TEST-*.xml` 再跑。**⚠️ gensrc/version-build 阶段 ANTLR `mismatched input '->'` + `which` 噪声非 javac 错、不影响 exit 0**。
- **⚠️ 勿并发跑两个 `-am clean` 构建**（争抢共享上游 `fe-foundation` target → 互 corrupt 假错）。**顺序跑**。**⚠️ 后台 Bash 报的「exit code」是末条命令（echo）的、非 maven 的**——读输出里的 `MVN_EXIT=${PIPESTATUS[0]}` 或 `BUILD SUCCESS/FAILURE` 行才是真信号。**⚠️ 勿 `| tail -N` 丢根因**。
- **iceberg 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）。**fe-core 用 `test`**（含 `-am`；编译上游约 6–7min，给足超时或后台跑）。**T04 实测**：warm build cache 下单类 ~30s、全量 ~数分钟。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`；SDK `org.apache.iceberg.*`〔含 `MetadataTableUtils`/`MetadataTableType`/`SerializationUtil`〕允许）。测试：连接器侧无 Mockito（fail-loud fake + **真 `InMemoryCatalog`**；fixture `RecordingIcebergCatalogOps`〔`loadTable` 返注入的 `.table`〕/`RecordingConnectorContext`〔`authCount`/`failAuth`/`log`〕；`FakeIcebergTable` **非** `HasTableOperations`，`createMetadataTableInstance` 用不了它）；fe-core dispatch 侧用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- **mutation-check（验证测试 pin 了意图，Rule 9/12）**：dormant 路里「测试是否真 pin 某行」非显然——临时删/改该行、单跑该测、确认转红、再恢复（每 task 至少一坑）。**坑①**：guard 测（auth-scope / empty 期望）对共享路/空 SPI default trivially-pass → 必经 mutation-check 验真。**坑②**：变异行若超 120 列 → checkstyle 先挂、无 surefire、不是有效测-红（用 length-safe 变体，T04 实证）。
- cwd 跨 Bash 持久；一律绝对路径（heredoc `cd` 会改 cwd → 后续相对路径失效）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。本 session 只动 2 产品/测 + 5 文档（task/PROGRESS/iceberg.md/decisions-log/HANDOFF）。
- message `[refactor](catalog) P6.5 iceberg: T04 — <subj>`（产品码改用 `[refactor]`/`[feature]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。**注意 T01–T06 + arg-move 已推 origin**（rebase/force-push 须谨慎；P6.4 T07/T08/T09 + P6.5 T01/T02/T03/T04 一并待 push）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。T04 实证：①设计/HANDOFF 行号大体准但 SDK 版本标签过时（实际 1.10.1 非 1.6.1）；②`FakeIcebergTable` 不是 `HasTableOperations`→`createMetadataTableInstance` 须真 `InMemoryCatalog` 表；③`MetadataTableType.from` 大小写不敏感、unknown→null。
- **大文件用 subagent/workflow 总结**（playbook §3.1）。PROGRESS.md 巨行（尤 header §一行 + board 行），编辑前用 Read 取精确行再 surgical Edit（Edit 前必先 Read 该文件，否则报错）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/P6-iceberg-migration.md` 状态 + `PROGRESS.md`〔header §一 + §二 board〔**UT 计数双处易遗漏**〕 + §四 dynamics〕 + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**。DV 中央登记延后到 T07 批量。
- **faithfulness 对抗 workflow 范式**：收口/汇总设计（T08）若引行号/commit/UT 计数必跑此 wf；**UT 计数 claim 必重跑 surefire 实证**（`@Test` 计数不能证绿，Rule 12）。**绝不 overclaim**。
- **T05 起步先 recon**（grep legacy `IcebergScanNode.doGetSystemTableSplits`/`setScanParams`/`serializeToBase64` 字节形状 + 连接器 `IcebergScanPlanProvider:582` 埋钩点 + `TIcebergFileDesc.serialized_split` thrift 字段 + 试 sys handle 经 `planFiles()` 序列化是否字节兼容 BE `IcebergSysTableJniScanner`）再写码。决策 A/B/[D-063]/[D-064] 已定，**无需再问**。**serialized-split 字节潜伏高危**——FE UT 不可及，勿 claim parity done（Rule 12，P6.8 e2e 兜底）。
