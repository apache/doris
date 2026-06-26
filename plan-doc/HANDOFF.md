# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 commit-bridge（step 6）末步：S5（dispatch dual-mode，最深、C5 阻塞）** —— S4 part 2 = supply 接线（β stash）✅ 本 session

**本 session = S4 part 2 supply 接线（β stash）✅**：并行对抗 recon（wf `wf_86cc1a20-681`，5-slice 锚点核 + 1 对抗 verify=**sound-with-caveats / CONDITIONAL GO**，三必堵项全落实）→ 实现 supply 接线（新 `IcebergRewritableDeleteStash` 单例暂存 + scan 供给 + write 消费）。**受影响 4 类 136/0/0 + 全量 clean `package` 588/0/0/1 + checkstyle 全绿；8-mutation 全杀；iron-law PASS；独立对抗 review GO 0-blocker**（亲核 `viceberg_delete_sink.cpp` 实证三处 path 串一致）。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ → commit-bridge：recon ✅ + S1 ✅ + S2/S3 ✅（option D）+ S4 part 1=Fix B ✅ + S4 part 2=supply 接线 ✅ → S5 待办（末步）**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（S4 part 2 = supply 接线 β stash；详设计 §11.7.7）
**问题**：v3 iceberg DELETE/MERGE 对每个被改 data-file，BE 写新 DV 须把该文件的**旧非-equality 删（旧 DV+旧 position-delete）OR-merge 进来**，否则旧删行**静默复活**。BE 不读 iceberg 元数据→FE 须在下发 sink 时填 `rewritable_delete_file_sets`。legacy 从 `IcebergScanNode` 字段在 finalize 收（计划级、随计划 GC 不泄漏）；SPI 里 scan/write 提供者互不相识、用完即弃→唯一跨 scan→write 活够久的=`IcebergConnector` 单例。

**机制（β stash，连接器内部、零新 fe-core→连接器 SPI、iron-law 最干净）**：新 `IcebergRewritableDeleteStash`（挂单例，仿 `manifestCache`）暂存 `queryId → Map<rawOriginalPath, List<TIcebergDeleteFileDesc>>`。scan 端 `IcebergScanRange` 加包私 `getOriginalPath()`+`rewritableDeleteDescs()`（滤 content!=2），`IcebergScanPlanProvider.planScanInternal` 对 v3 range `accumulate`（新 5-arg ctor 注入 stash）；write 端 `IcebergWritePlanProvider.planWrite` 起手 `retrieveAndRemove(queryId)`（**所有 write op 都取+驱逐**）→ `buildDeleteSink`/`buildMergeSink` 盖 `rewritable_delete_file_sets`（gate `formatVersion>=3 && !empty`，镜像 legacy `IcebergDeleteSink.toThrift`）。

**recon-verify 三必堵项（全 silent-resurrection 级，已落实）**：① **key=RAW `originalPath`**（=`dataFile.path().toString()`，**非**归一化路径）—— BE 按 per-row rowid 的 raw `file_path` `std::string` 精确查（`viceberg_delete_sink.cpp:179/61`），归一化 key 全 miss→复活（review 亲核三串一致）；② **blank queryId 跳过**（null ConnectContext→""，并发撞键）；③ **泄漏有界 + 绝不驱逐活条目** —— 纯 SELECT 在单例留泄漏，用**惰性 TTL 清扫**（300s，仅 new-query 首达时扫，只清超时未触碰、绝不清活条目=清活=复活）。**deviation from 旧 β 文字「ConcurrentHashMap+txn-evict」**（txn-evict 漏纯 SELECT 泄漏；改有界 stash+planWrite 即取即驱逐，更稳无 txn 耦合）。MERGE 双表同 queryId 按 path accumulate（非按 queryId 覆盖）；over-supply 无害（BE 按 path 过滤）。

**[DV] 无新 pre-flip DV**：iceberg 未翻闸→提供者运行期休眠→pre-flip byte-identical。S4 part 2 = [DV-S2-rederive] 的 **supply 半边**（remove 侧 commit-time re-derive@S_read + supply 侧 scan-time stash@S_read，Fix B 后同快照）→ post-flip DV-merge 两侧一致、闭 [SHOULD-2]。

## commit-bridge 子步拆解（每完即 green+mutation+HANDOFF+commit；PR squash；详 design §11.7.2/§11.7.5/§11.7.6/§11.7.7）
1. **S1 ✅**（WriteOperation 透传）。**S2/S3 ✅**（option D commit-time re-derive；legacy-map seam 整个消失，[DV-S2-rederive] 登记）。
2. **S4 part 1 = Fix B ✅**（写入遵循读快照，`baseSnapshotId=S_read`，闭 [SHOULD-2] remove 侧）。**S4 part 2 = supply 接线 ✅ 本 session**（β stash，详上 + design §11.7.7；supply 侧 @S_read 与 remove 侧一致）。
3. **S5 ⟵ 下个 session 起点（最后、最深、C5 阻塞）**：`IcebergRowLevelDmlTransform` 5 方法 dual-mode（`newExecutor`→`PluginDrivenInsertExecutor`/`setupConflictDetection`→no-op〔中立路供给〕/`finalizeSink`→连接器/`checkMode`·`synthesize`→中立 SPI〔漏算项〕）+ translator `visitPhysicalIcebergMergeSink:601`/`DeleteSink:589` dual-mode（post-flip→`PluginDrivenTableSink`+S1 WriteOperation；**MERGE slot-name loop `:613-615` 上提至 instanceof 分叉之上**〔BE `viceberg_merge_sink.cpp` 按 expr_name 解〕；**DELETE 不需** slot-name loop〔BE 按 block-name 解〕）。**[DEC-S5] 冲突过滤 parity 到 S5 再裁**（widen vs byte-parity）。

## 🟡 待裁决策（动到时请用户裁；中文先讲清）
- **[DEC-S2] ✅ 裁定 = option D**（见 design §11.7.4–5）。
- **[SHOULD-2] ✅ 裁定 = 方案 B（写入遵循读快照）+ 已实现 Fix B**（本 session，design §11.7.6）。
- **[DEC-S5] 冲突过滤 parity**：中立 `ConnectorPredicate`→native Expression 仅 DROP/widen（安全向、不漏真冲突，但更宽→更多假冲突 retry=perf 非正确性）→裁「接受 widen」vs「要 byte-parity」。**到 S5 再裁**。

## 🟡 已登记 follow-up（非阻塞，勿在本增量做）
- **[SHOULD-2] ✅ 全闭**：remove 侧（Fix B + option D commit-time re-derive @S_read）+ supply 侧（S4 part 2 scan-time stash @S_read）都锚 `S_read`。残留 [FU-Bnit-ref]（buildWriteContext 不读 getRef，DML 读钉为具体 snapshotId 故 OK，翻闸确认）+ [FU-Bnit-update]（UPDATE 同分支已覆盖）。
- **[FU-supply-e2e → flip e2e]** S4 part 2 supply 全 UT 锁（含真 `InMemoryCatalog`+真 DV/pos-delete commit 的 scan e2e + 真 v3 表的 write e2e）+ 8-mutation 全杀 + review 亲核 BE 三串一致，但 **live-e2e（真翻闸 DELETE/MERGE 旧删不复活）未跑**（CI-gated）。
- **[FU-supply-leak → flip 观测]** 纯 SELECT 在 v3-有删表会留 stash 泄漏条目（TTL-300s 惰性清扫兜底，不驱逐活条目）；翻闸后可观测 stash size 验证清扫如期、无活条目误清。
- **[FU-step1-nullconn → cutover]** `IcebergRowLevelDmlTransform.pluginConnectorSupportsRowLevelDml`（step-1）unguarded `getConnector()`；step-3 同名 helper 已 guard → cutover 前对齐。
- **[FU-order → flip e2e]** post-flip `getFullSchema` 列序 [base,v3-lineage,row-id]≠legacy [base,row-id,v3-lineage]。**benign**（按名/field-id 匹配）+ dormant。
- **[FU-remap → cutover]** `PluginDrivenExternalTable.toSchemaCacheValue:188` remap 丢 invisible/uniqueId（对 iceberg 安全）。
- **[FU-S1-update-test]** reviewer nit：未补 UPDATE 测（`PluginDrivenTableSink` 对 op 不分支、UPDATE 同 MERGE 路）→不补，记此。
- **[FU-dualdelete-e2e → flip e2e]** v2→v3 升级表 legacy+DV 双删 removeDeletes 两者：UT 已锁（`...RemovesBothLegacyAndDeletionVectorForUpgradedTable`）+ reviewer commit-probe 实证 clean，但 live-e2e 未跑。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。保留的 `PhysicalIcebergMergeSink`/`DeleteSink`/`IcebergRowLevelDmlTransform`/`Iceberg*Command`/`IcebergNereidsUtils`/`IcebergTransaction`/`IcebergRewritableDeletePlanner`/`Iceberg*Executor` = **legacy 豁免**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支落这些豁免类合法）；V3 DeleteFile seam 全连接器内（无 fe-core native 耦合，S2-a 下无 `instanceof IcebergConnectorTransaction` downcast）。**S1 注入点 `PluginDrivenTableSink` 是通用 fe-core 类（非豁免）→ 仅引中立 `WriteOperation`，已合规。**

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre 已闭，C3b-core 设计+recon 闭、step1-3 已实现，step6+C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。
- **[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。
- **[C3a ✅]** INSERT OVERWRITE 能力 + 完整接通 @branch（[D-070]，闭 DV-T06-branch）。
- **[C3b-pre ✅]** partition_columns emit + ④b parallel-write（[D-071]）。
- **[C3b-core step1-3 ✅ / commit-bridge: recon ✅ + S1 ✅ + S2/S3 ✅（option D）/ S4-S5 待办]**（[D-072]+[D-073]）：① handles ✅ + ② getWritePartitioning helper ✅ + ③ row-id 双注入 ✅ + commit-bridge：S1 WriteOperation 透传 ✅ → [DEC-S2]=option D（Trino-style commit-time re-derive）+ S2/S3 ✅ → S4（BE supply 侧）/S5 待。详 §10+§11（§11.7.4–5 = 本 session recon+[DEC-S2]+S2/S3）。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 throw）；`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / mvcc+partition-stats capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（精确类白名单）→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅ 本 session 闭**：隐藏列注入——v3-lineage 经 carrier=A 连接器 emit（③-infra part2）；请求级 STRUCT row-id 经 fe-core `getFullSchema` override + injector guard（step3）。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core(③-infra+step1+step2+step3)+commit-bridge S1+S4 part1(Fix B)+S4 part2(supply) 无新 DV。**commit-bridge S2/S3=[DV-S2-rederive]（post-flip：旧删源 scan-map→commit-time base-snapshot manifest 读，快照自洽；+有意偏离 Trino：升级表 legacy+DV 双删两者全 removeDeletes，因 BE union 两类）**；S4 part 2 是其 supply 半边（scan-time stash @S_read），与 remove 侧一致、不新增 DV。S5 预计可能再引入 DV——做到时评估。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant + step6 实现锚点）

- **commit-bridge S5 实现锚点（impl 期 re-grep 防漂移；HEAD = 本 session S4 part 2 commit 后）**：
  - **S2/S3 ✅ DONE（option D）**：连接器 `IcebergConnectorTransaction.collectRewrittenDeleteFiles` → `readExistingFileScopedDeletes(table, baseSnapshotId, touched)`（base 快照 delete-manifest 纯元数据读）；字段+setter 已删。
  - **S4 part 1 = Fix B ✅ DONE**：`PhysicalPlanTranslator.visitPhysicalConnectorTableSink`〔+`applyMvccSnapshotPin(...,MvccUtil.getSnapshotFromContext(targetTable))`〕；`PluginDrivenScanNode.applyMvccSnapshotPin` public；`IcebergWriteContext.readSnapshotId`；`IcebergWritePlanProvider.buildWriteContext` 提取 `getSnapshotId()`；`IcebergConnectorTransaction.applyBeginGuards`〔DELETE/UPDATE/MERGE 臂 `baseSnapshotId=pin>=0?pin:current`〕。
  - **S4 part 2 = supply 接线 ✅ DONE（β stash）**：新 `IcebergRewritableDeleteStash`（单例字段，仿 `manifestCache`）；`IcebergScanRange.getOriginalPath()`+`rewritableDeleteDescs()`（包私，滤 content!=2）；`IcebergScanPlanProvider`〔5-arg ctor + `planScanInternal` 主循环对 v3 range `accumulate`〕；`IcebergWritePlanProvider`〔4-arg ctor + `planWrite` 起手 `retrieveAndRemove` + `buildDeleteSink`/`buildMergeSink`(rewritable 参) + `buildRewritableDeleteFileSets` gate v3+!empty〕；`IcebergConnector` 注入 stash 入 scan/write 两 provider。key=raw originalPath；blank queryId 跳过；TTL-300s 惰性清扫不驱逐活条目。
  - **S5（下个 session 起点）**：`IcebergRowLevelDmlTransform`〔`checkMode:99`/`synthesize:115`/`newExecutor:135`/`setupConflictDetection:191`/`finalizeSink:203`，`handles:71-75` step-1 已纳 PluginDriven〕dual-mode；translator `visitPhysicalIcebergMergeSink:601`/`DeleteSink:589`（仿 `visitPhysicalConnectorTableSink:630`，slot-name loop `:613-615` MERGE 上提/DELETE 不需）；`PluginDrivenInsertExecutor.beginTransaction:81`/`getConnectorTransactionOrNull`；`RowLevelDmlCommand.run:69-109`〔`applyWriteConstraintIfPresent:131-138` 中立冲突路已接〕。`IcebergDmlCommandUtils.checkMergeMode:52`（native `getIcebergTable().properties()`，漏算项→中立 SPI）。
- **S1 实现位（本 session）**：`PluginDrivenTableSink`〔新 7-arg ctor + `WriteOperation` 字段 + 两 handle 站点透传 + inner `PluginDrivenWriteHandle.getWriteOperation()` override〕；`ConnectorWriteHandle.getWriteOperation`（api，默认 INSERT）；`WriteOperation` enum（INSERT/OVERWRITE/DELETE/UPDATE/MERGE/REWRITE）。
- **① handles（step1）/ ② getWritePartitioning+cast（step2）/ ③ row-id 注入（step3）/ partition_columns+parallel-write（C3b-pre）/ 写@branch+OVERWRITE（C3a）/ classifyColumn（C2）/ sys pin-feed（C1）**：dormant，详 git log + design §11.4/§11.5/§11.6。
- **连接器**：`IcebergConnectorMetadata`〔`supportsDelete:496`/`supportsMerge:501`/`buildTableSchema:227`〕/`IcebergConnectorTransaction`〔全 commit + `collectRewrittenDeleteFiles`→`readExistingFileScopedDeletes`（option D commit-time manifest 读；字段/setter 已删）〕/`IcebergWritePlanProvider`〔`planWrite:135`〔INSERT/OVERWRITE/DELETE:157/MERGE:162 全分派〕/`buildWriteContext:270`/`getWritePartitioning`/`getSyntheticWriteColumns`〕/`IcebergScanPlanProvider`〔`buildRange:444`/`buildDeleteFiles:514`；S4 part 2 加 `planScanInternal` 主循环 v3 `accumulate` 入 stash〕/`IcebergRewritableDeleteStash`〔新，单例暂存 supply〕。
- **fe-core legacy 豁免（pre-flip 路 + P6.7 删）**：`IcebergTransaction` + `IcebergDeleteExecutor`/`MergeExecutor`/`InsertExecutor` + `IcebergNereidsUtils` + `IcebergRewritableDeletePlanner` + `IcebergExternalTable.getFullSchema:293`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan/sink/command/executor/transaction/PluginDrivenExternalTable=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B`。**fe-core -am 单类 ~2.5min（首次 -am 全链 ~5-7min）**。后台 task exit code 是 echo 的非 maven 的，读 surefire XML。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（**脚本在仓根 `tools/`，非 `fe/tools/`**；exit 0=PASS）。**连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`）+ fe-core contract UT pin 值防漂移（C2 范式）。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS`+stub 包私 seam；jmockit `Deencapsulation` import = `org.apache.doris.common.jmockit.Deencapsulation`〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 易 trivially-pass → 必变异验真。批量变异脚本范式：cp 备份→exact-string mutate→`mvn test -Dtest=<class>`→查 surefire 期望测试 fail（KILLED）→restore（见 scratchpad/mutate.py）。**OR 能力须双 arm 各测一遍**（merge-only 漏测教训）。**⚠️变异须用「行为禁用形」（如 `… && false`）而非「删引用形」**：删唯一引用某 import/local 的表达式→checkstyle UnusedImport 阻断 build→无 surefire XML→脚本误判「SURVIVED」假阴（step-3 M3b-gate/ctx 踩过：删 `Util.showHiddenColumns()`/`ctx…` 触发；改 `&&false` 保引用复验 KILLED）。同理 surefire XML 缺失须与「测试通过」区分（脚本 failed() 返 None=inconclusive）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔review 重申勿提交，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **本 session commit（commit-bridge S4 part 2 = supply 接线 β）= 4 改 + 1 新 main + 3 改 + 1 新 test + 2 doc**（全连接器内、零 fe-core）：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`下 `IcebergConnector.java`、`IcebergScanPlanProvider.java`、`IcebergScanRange.java`、`IcebergWritePlanProvider.java`、新 `IcebergRewritableDeleteStash.java`；测试同目录 `IcebergScanPlanProviderTest.java`、`IcebergScanRangeTest.java`、`IcebergWritePlanProviderTest.java`、新 `IcebergRewritableDeleteStashTest.java`；+`plan-doc/HANDOFF.md`、`plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔§11.7.7〕。**勿** `git add -A`（尤其勿提交仓根游离 `fe/IcebergScanPlanProvider.java`）。
- commit message：见 `git log` 上一条范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 = ✅ DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ / commit-bridge recon+S1+S2/S3（option D）+S4 part 1=Fix B+S4 part 2=supply 接线 ✅ → S5（末步）待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 接近满（2.0T；本 session 起步 72G→全程 86G free，全程 OK）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`（测行为非 style）。**⚠️ 多次增量 build+mutation 后 .class 可能 stale → 误判 flaky；全量验证用 clean `package`（删 target/classes+test-classes）**。**⚠️ python3.6（无 `capture_output`/`text=`）**：mutation 脚本用 `stdout=subprocess.DEVNULL`（本 session mutate_supply.py 首跑因 `capture_output` 崩，改 DEVNULL 即 8-mutation 全杀）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)` / `instanceof Iceberg*`（新 seam）** —— dual-mode helper 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法；通用类（`PluginDrivenExternalTable`）须全经中立 SPI。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（本 session recon `wf_d1116240-aa8` 5-slice+4-verify+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **commit-bridge 是大活、最深、跨 session**：fresh session 全 budget 做；S1/S2-S3/S4 part1/S4 part2 ✅，**S5（dispatch dual-mode）= 末步、最深、C5 阻塞**；逐 sub-step green+mutation；**C5 前切忌动 `SPI_READY_TYPES`**。
- **下个 session 起步 = S5（dispatch dual-mode）**：先读 design §11.7.2（S5 拆解）+ §11.7.1（11.7.1 漏算项=`checkMode`/`synthesize` native CCE→中立 SPI）+ 本 HANDOFF「[DEC-S5] 冲突过滤 parity」（动 setupConflictDetection 时请用户裁 widen vs byte-parity，中文先讲清）。动码前 re-grep `IcebergRowLevelDmlTransform` 5 方法 + translator `visitPhysicalIcebergMergeSink/DeleteSink` 锚点（行号可能漂）。S5 涉 BE sink dialect 解析（MERGE 按 expr_name / DELETE 按 block-name）须核 `viceberg_merge_sink.cpp`/`viceberg_delete_sink.cpp`。
- **上下文超 30% 即交接**。本 session = S4 part 2（supply 接线 β，136/0/0+全量 588/0/0/1+8-mutation 全杀+对抗 review GO），在干净节点交接。
