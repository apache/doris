# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 C3b-core（① handles + ② cast/dual-mode + ③ row-id 注入）TDD**（C3b 起步 recon 完成、用户双裁 ④b=A/③=ii；**C3b-pre ✅ DONE 本 session [D-071]**）

**P6.1–P6.5 = ✅ 全 DONE**。**P6.6 翻闸进行中：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre（前置-a partition_columns + ④b parallel-write）✅ DONE → C3b-core（①②③ coupled）待办**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），每 C 自带 UT+mutation，**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## C3 本 session 完成 = 起步对抗 recon + 设计（`tasks/designs/P6.6-C3-ws-write-design.md`）
- **RFC §6.WS-WRITE 被证伪**（仿 C1/C2）：其「移植合成列物化+分布到通用 `visitPhysicalConnectorTableSink`、替 :634 UNPARTITIONED」= **category error + 死代码**（11-agent 对抗 wf `wf_148dce6f-f70` 4 verdict 全 high；`:634` 是终端输出分区与 merge 分布正交；合成列/分布只在 iceberg 专用 sink 道，翻闸后无计划流经通用 sink）。
- **翻闸后写真实状况**：INSERT 通但分布退化(GATHER vs legacy RANDOM/hash)；INSERT OVERWRITE/@branch 回归被拒；DELETE/UPDATE/MERGE 全断(`IcebergRowLevelDmlTransform.handles:69 instanceof IcebergExternalTable` 对兄弟类 `PluginDrivenMvccExternalTable` false)。
- **用户裁 Option C**（2026-06-25）：C3 做 ④+①②③，**保留 iceberg 专用 sink 道**（`PhysicalIcebergMergeSink`/`DeleteSink`），经连接器取真 iceberg 表，不改路由到通用 sink。
  - **④** INSERT 侧三能力缺口（覆盖写/写分支/并行写）= **C3a**（连接器侧、真休眠、最小）。
  - **①** `handles()`+per-op gate → 连接器能力探测；**②** `(IcebergExternalTable)` 强转 → 经连接器取真表（新中立 SPI 描述符 + dual-mode helper `getIcebergPartitioning`；放宽 sink/command/executor 字段类型）；**③** `$row_id` 隐藏列条件注入（与①同步）= **C3b**（coupled、非纯休眠）。

## ✅ 子决策（2026-06-25 session 2 recon 修订；详 [D-071] + 设计 §9）
- **[D1 → 推翻 → Option A]** ④b 分区 INSERT parity：recon 实证 legacy iceberg hash 分布是**死代码**（`PhysicalIcebergTableSink:124` 读 `getPartitionNames()`，iceberg 从不 override→恒空集→恒 `SINK_RANDOM_PARTITIONED`）。**用户裁 Option A（随机真 parity）= 仅加 `SUPPORTS_PARALLEL_WRITE`**（不加新 capability/分支，不依赖 partition_columns 前置，0 新 DV）。**✅ C3b-pre 已做**。
- **[D2=i]** 写 @branch：**C3a 已完成**（[D-070]，闭合 DV-T06-branch）。
- **[D3=iii → ctx Option ii]** row-id 注入：「合成写列」连接器能力 + fe-core 通用注入（仿 C2 classifyColumn）；**ctx 信号 = Option ii 中立化重命名** `ConnectContext.icebergRowIdTargetTableId`（用户裁 session 2）。⚠️ 比 classifyColumn 重：须经中立 SPI 透传完整 STRUCT 列定义。**留 C3b-core**。
- **两前置核实结果**：(a) ✅ **前置-a = real gap**（buildTableSchema 不 emit partition_columns）→ **C3b-pre 已修**（legacy `loadTableSchemaCacheValue` 语义，**非** `getIdentityPartitionColumns` helper）。(b) ✅ **前置-b ok**（post-flip getType=PLUGIN→`BindRelation:653`→`computePluginDrivenOutput:235` from getFullSchema；`BindRelation:621` cast 仅 legacy-class 路径，无 CCE）。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。**C3 新代码（① handles 分支、② 新 SPI+helper post-flip 分支、④ capability）全用中立能力/描述符**；保留的 `PhysicalIcebergMergeSink`/`IcebergRowLevelDmlTransform`/`Iceberg*Command` = **legacy 豁免**（P6.3-T07 created）。③ 是唯一可能触铁律处 → D3 倾向 SPI 化。

## C3b-core 起步指引（C3b-pre 已完成；①②③ coupled）
1. **读 `tasks/designs/P6.6-C3-ws-write-design.md` §9**（session-2 recon：verified anchors + drifts + ② cast 全量清单 §9.5；§3.3/§3.4 旧机制仍参考，但 ④b 已 superseded、③ 已细化 ii）。
2. **C3b-pre 已完成**（本 session，[D-071]）——见下「本 session 完成」。**前置 a/b 都已核**（a=real gap 已修 / b=ok）。④b 已做（Option A），**不**在 C3b-core 内。
3. **C3b-core 顺序（coupled，CCE 风险：① 放宽 handles 而 ②③ 未配齐则首个 CCE 在 `IcebergRowLevelDmlTransform:90`）**：① `IcebergRowLevelDmlTransform.handles:69` 泛化（仿 `allowInsertOverwrite` 320-329：`instanceof PluginDrivenExternalTable && supportsDelete||supportsMerge`，legacy `instanceof IcebergExternalTable` 留 OR）→ ② ~30 强转放宽 `→ ExternalTable` + 新中立 SPI partition 描述符 + `getIcebergPartitioning` dual-mode helper（最深=`PhysicalIcebergMergeSink:188/195` + executor→transaction 层 typed IcebergExternalTable）→ ③ ctx 中立化重命名(ii) + 合成写列连接器能力(STRUCT carrier) + fe-core 通用注入（`PluginDrivenExternalTable.getFullSchema`/`needInternalHiddenColumns` override + neutralize `IcebergRowIdInjector:159` guard）。**pre-flip parity 回归（DELETE/UPDATE/MERGE plan byte-identical）必做** + mutation。
4. **每 C 完**：更 HANDOFF + commit。**C5 前切忌动 `SPI_READY_TYPES`**。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2 已闭，C3–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。**C1/C2 已 DONE**（详 `tasks/designs/P6.6-C1-ws-pin-design.md` + [D-068]、`P6.6-C2-ws-synth-read-design.md` + [D-069]）。

- **[C1 ✅]** sys 表时间旅行 pin-feed：`PluginDrivenScanNode.pinMvccSnapshot` 加 `resolveSysTableSnapshotPin()` fallback。recon 推翻 D4/D5。零 SPI/零连接器改。5 UT + mutation 绿。
- **[C2 ✅]** 合成列读路径 classifyColumn **SPI 化**：新 `ConnectorColumnCategory` + `ConnectorScanPlanProvider.classifyColumn(name)` default DEFAULT；fe-core `PluginDrivenScanNode.classifyColumn`（GLOBAL_ROWID 留 fe-core + 委派 `classifyColumnByConnector` seam）；iceberg `IcebergScanPlanProvider.classifyColumn` override。**recon 推翻 RFC BE「DCHECK 崩」（已完整处理→零 BE 改）+ D7（paimon 被 `MaterializeProbeVisitor` 精确类白名单挡、不触达 GLOBAL_ROWID→对 paimon 怎样都安全）**。对 live 连接器零行为变更。连接器 4 UT + fe-core 6 UT + mutation 绿，回归 63/0/0 + 67/0/0。
- **[C3 进行中]** Option C（保留 iceberg 专用 sink 道、经连接器取真表）。**C3a ✅**（④a/④c，[D-070]）。**C3b-pre ✅**（前置-a partition_columns + ④b parallel-write，[D-071]；recon 推翻 D1=ii→④b=Option A 随机真 parity）。**C3b-core 待办**（①②③ coupled，详设计 §9）。GAP-B(=③ row-id 注入)= C3b-core ③（D3=iii + ctx Option ii）。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 `IcebergExecuteActionFactory:89` throw）；**唯一深水 = per-group 读端子设计**。`IcebergRewriteDataFilesAction:173,196` `(IcebergExternalTable)` 翻闸即 ClassCastException（**不能纯 defer**）。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory:137 case` / `pluginCatalogTypeToEngine` iceberg / **GSON compat 8 catalog+db+table** / mvcc+partition-stats capability 核 / Show* parity。**C5 是唯一不可逆点**（GSON 身份变→不可降级回读 image）；C5 前须 C1–C4 全绿 + 用户二签。

## 🆕 C2 recon 挖出的两处翻闸前置项（RFC 完全漏，已登记防失）
- **[GAP-A → C5]** 翻闸后 iceberg 表类 `IcebergExternalTable`→`PluginDrivenMvccExternalTable`（**与 paimon 同类**），掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（`:58-63` **精确类**白名单）→ iceberg lazy-top-N（GLOBAL_ROWID）静默失效 + C2 的 GLOBAL_ROWID 分类对 iceberg 死码。修须 **capability/engine 判别（非 class，因与 paimon 同类）**，仿 `:129-133` HMS `getDlaType()`；宜一并去该文件今天的 `import IcebergExternalTable` fe-core iceberg 泄漏。**用户裁登记入 C5**（RFC §6.WS-1 已记）。
- **[GAP-B → 随翻闸追踪]** 隐藏列**注入**：`ICEBERG_ROWID_COL`（show_hidden/DML）+ v3 row-lineage 现由 legacy `IcebergExternalTable.initSchema:297-301` 注入；翻闸后 `PluginDrivenExternalTable.initSchema:172` 仅从连接器 native `getTableSchema`→`parseSchema` 建、**不注入** → 翻闸后这些列不存在（DML 绑定失败 / show_hidden 不暴露 / v3 row-lineage 空），且使 C2 的 ICEBERG_ROWID/row-lineage 分类**无列可分**。须迁注入到连接器 `getTableSchema`（**rowid 条件注入**依赖查询上下文 show_hidden/DML，与连接器无状态 getTableSchema 有张力，需设计）。**用户裁随翻闸追踪**；**C3 设计期须先核 C3↔GAP-B 依赖**。

**[pre-flip 行为偏差中央登记]**：P6.4 = DV-045/046/047；P6.5 = DV-048/049。**C1/C2/C3a/C3b-pre 无新 DV**（均 dormant，对 live 连接器零行为变更；C3a 闭合 DV-T06-branch；C3b-pre ④b=Option A 真 parity）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`（=`bdc38b14810` 后续，详 [D-068/T09 修正]）；**P6.4 T07/T08/T09 + P6.5 T01–T08 + P6.6-RFC + C1 + C2 + C3a + 本 session C3b-pre 已 commit 未 push**（确切数用 `git rev-list --count origin/catalog-spi-10-iceberg..HEAD`）。**用户未要求 push**——留用户裁量。工作树 tracked 干净（除既有 untracked scratch，见下 commit 须知）。
- **P6.1–P6.5 = ✅ DONE**。**P6.6 翻闸进行中：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ DONE → C3b-core（①②③）待办**。
- iceberg **不在** `SPI_READY_TYPES`（仍走 switch-case `:137 case "iceberg"`，pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 = P6.6 C3b 起步 recon + C3b-pre（前置-a partition_columns + ④b parallel-write；连接器侧 dormant）

- **起步对抗 recon**（6-slice wf `wf_feecba0f-854`，5/5 有效 slice + 主 session 亲核 2 前置/④b 死代码/② cast 全量）→ 详 [D-071] + 设计 §9。**核心发现**：① **D1=ii 前提被推翻**——legacy iceberg 分区 INSERT hash 是死代码（`PhysicalIcebergTableSink:124` getPartitionNames 恒空→恒 SINK_RANDOM_PARTITIONED）→ **用户裁 ④b=Option A（仅 SUPPORTS_PARALLEL_WRITE = 随机真 parity）**；② **③ ctx=Option ii（中立化重命名）**；③ 前置-a=real gap（buildTableSchema 不 emit partition_columns）、前置-b=ok；④ ① 比 doc 简单（唯一 live gate handles:69，per-op guard 死，translator 无需 ordering 改）。
- **C3b-pre 实现（连接器侧 dormant，遵铁律全中立，2 文件）**：
  - **前置-a**：`IcebergConnectorMetadata.buildTableSchema`（partitioned 时）emit `partition_columns` CSV（legacy `IcebergUtils.loadTableSchemaCacheValue:1742-1751` 语义：current spec / **无 identity 过滤** / 源列 `schema().findField(field.sourceId()).name()` lowercased / 无 dedupe；**非** `IcebergPartitionUtils.getIdentityPartitionColumns` 的 all-specs+identity-only）。+`import PartitionField`。
  - **④b**：`IcebergConnector.getCapabilities` 加 `ConnectorCapability.SUPPORTS_PARALLEL_WRITE`（→ `PhysicalConnectorTableSink:195` 返 SINK_RANDOM_PARTITIONED = legacy runtime parity；**不** SINK_REQUIRE_PARTITION_LOCAL_SORT）。
- **验证（TDD red→green + mutation，Rule 9/12）**：连接器 `IcebergConnectorMetadataTest` **26/0/0**（+2：`getTableSchemaEmitsPartitionColumnsForIdentityPartition` unpart-null+identity；`getTableSchemaEmitsNonIdentityPartitionSourceColumns` bucket(id)→"id" 守 no-identity-filter parity；**identity-only mutation→非identity test 红证判别、identity test 仍绿**）+ `IcebergConnectorTest` **6/0/0**（+1 `declaresParallelWriteCapability`，RED 见证）；**全模块 553/0/0+1 live-skip**（`IcebergLiveConnectivityTest#liveMetadataRoundTrip` env-gated assumption）；import-gate PASS；`SPI_READY_TYPES` 未改。
- **文档同步**：design §9（recon 全量+决策修订+② cast 清单 §9.5）+ §8 切分 C3b-pre/C3b-core / decisions-log [D-071] / HANDOFF 覆盖。**0 新 DV**（dormant + Option A 真 parity）。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant）

- **partition_columns + parallel-write（C3b-pre 新增，dormant）**：连接器 `IcebergConnectorMetadata.buildTableSchema`〔partitioned 时 emit `partition_columns` CSV = current-spec 源列名 lowercased，legacy `loadTableSchemaCacheValue` 语义；`PluginDrivenExternalTable.toSchemaCacheValue:212` 消费〕 + `IcebergConnector.getCapabilities` +`SUPPORTS_PARALLEL_WRITE`〔→ `PhysicalConnectorTableSink:195` SINK_RANDOM_PARTITIONED〕。④b=Option A（不加 partition-hash 分支）。
- **写 @branch + INSERT OVERWRITE（C3a 新增，dormant）**：SPI `ConnectorWriteOps.supportsWriteBranch()`〔default false〕+ `ConnectorWriteHandle.getBranchName()`〔default empty〕；连接器 `IcebergConnectorMetadata.supportsInsertOverwrite()/supportsWriteBranch()=true` + `IcebergWritePlanProvider:197`〔读 `handle.getBranchName()`〕；fe-core `PluginDrivenInsertCommandContext.branchName` + `PluginDrivenTableSink.bindDataSink`〔透传 branch→handle〕 + `InsertIntoTableCommand`/`InsertOverwriteTableCommand`〔@branch guard 泛化 helper + 插入站点 setBranchName〕。链路 pre-flip 走 legacy `PhysicalIcebergTableSink`（guard 短路）；post-flip 走通用 sink。
- **合成列分类（C2 新增，dormant）**：`ConnectorColumnCategory`〔`fe-connector-api/.../scan/`，中立枚举〕+ `ConnectorScanPlanProvider.classifyColumn(name)`〔default DEFAULT〕+ `PluginDrivenScanNode.classifyColumn`〔fe-core override：GLOBAL_ROWID 前缀→SYNTHESIZED + `classifyColumnByConnector` seam〕+ `IcebergScanPlanProvider.classifyColumn`〔连接器 override：`__DORIS_ICEBERG_ROWID_COL__`/`_row_id`/`_last_updated_sequence_number`〕。BE iceberg reader 已处理（`iceberg_reader.cpp:162-208/444-489`，**勿改**）。
- **sys 时间旅行 pin-feed（C1，dormant）**：`PluginDrivenScanNode.pinMvccSnapshot`〔context 空→`resolveSysTableSnapshotPin()` 委派源表 `loadSnapshot`〕。链路 `BindRelation:467-474`→`PhysicalPlanTranslator:802-805`→guard→pin→`IcebergScanPlanProvider.buildScan:338-342`。
- **连接器**：`IcebergTableHandle`/`IcebergConnectorMetadata`〔sys 分支 + `getTableSchema`/`getColumnHandles`〕/`IcebergScanPlanProvider`〔`planSystemTableScan`/`supportsSystemTableTimeTravel()=true`/`classifyColumn()`〕/`IcebergScanRange`。
- **fe-core**：`PluginDrivenScanNode`〔`checkSysTableScanConstraints` capability-aware / `pinMvccSnapshot` sys fallback / `classifyColumn` SPI 委派〕 + `PluginDrivenExternalTable.getEngine` iceberg case + `PluginDrivenExternalDatabase.buildTableInternal`〔SUPPORTS_MVCC_SNAPSHOT→`PluginDrivenMvccExternalTable`〕。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：`datasource/iceberg/source/IcebergScanNode`〔`classifyColumn:907-919` C2 移植源 / `createTableScan:569` sys TT〕 + `IcebergExternalTable.initSchema:297-301`〔GAP-B 隐藏列注入源〕 + `IcebergUtils.isIcebergRowLineageColumn:1756`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B` 即可。**fe-core -am 单类/多类 ~2.5min**。后台 task 通知的 exit code 是 echo 的非 maven 的，读 surefire XML。**注**：fe-core 构建尾常有 antlr `->`/`super::` "mismatched input" 行 = parser-gen 噪音（每次都有、exit 0、测全绿），非本改动。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `catalog|common|datasource|qe|analysis|nereids|planner`）。**故连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`）+ fe-core contract UT pin 值防漂移（C2 范式）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS` + stub 包私 seam（如 `classifyColumnByConnector`/`sysTableSupportsTimeTravel`），勿设 private final 字段〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow/verifyNoInteractions 测易 trivially-pass → 必变异验真（C2 已逐条做：连接器 M4/M5、fe-core M1+M2+M3）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。本 session = **C3b-pre 改（连接器侧 4 文件 + 4 文档）**：
  - 连接器：`fe/fe-connector/fe-connector-iceberg/.../iceberg/IcebergConnectorMetadata.java`〔buildTableSchema emit partition_columns + import PartitionField〕· `.../iceberg/IcebergConnector.java`〔getCapabilities +SUPPORTS_PARALLEL_WRITE〕· `.../src/test/.../iceberg/IcebergConnectorMetadataTest.java`〔+2 partition_columns UT〕· `.../src/test/.../iceberg/IcebergConnectorTest.java`〔+1 declaresParallelWriteCapability〕
  - 文档：`plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔§8 切分 + §9 recon〕· `plan-doc/decisions-log.md`〔D-071〕· `plan-doc/HANDOFF.md`（无 deviations-log 改 = 0 新 DV）
- message `[refactor](catalog) P6.6 iceberg: C3b-pre — <subj>` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。P6.7 删 legacy 时尤其。
- **HANDOFF/设计/RFC/audit-spec 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep + 实证）再信文档。**C1 推 D4/D5；C2 推 RFC BE「DCHECK 崩」+ D7（问错方向）+ 挖出 GAP-A/GAP-B**——C3 起步**同样先对抗 recon 核 RFC §6.WS-WRITE** 再 TDD。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)`** —— iceberg 逻辑落连接器经 SPI（capability / 中立 default 方法）。C3 的 sink 合成列/分布须 connector-guard，不能 `instanceof Iceberg*`（大概率须新 SPI 钩子，仿 C2 `classifyColumn`）。
- **大文件用 subagent/workflow 总结**；`decisions-log.md`（>30k token）/巨行编辑前先 grep 定位精确子串再 surgical Edit（Edit 前必先 Read 一次该文件，哪怕只读锚点几行）。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗 + 先 code 独立判断、后交叉核历史结论（C2 即此范式：主 session 亲读 classifyColumn/LazyMaterializeTopN/MaterializeProbeVisitor/BE reader 链路，再用 8-agent wf 交叉核 + 3 adversarial verify）。
- **P6.6 进行中**：C1 ✅ / C2 ✅ DONE。下个 session 进 **C3 WS-WRITE**（先 recon + 核 C3↔GAP-B 依赖）。**大阶段分多 session**，每 C 完即更 HANDOFF + commit。翻闸（C5）是全有或全无不可逆点，C5 前须 C1–C4 全绿 + 用户二签。
