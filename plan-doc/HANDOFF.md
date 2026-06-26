# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 commit-bridge（step 6）续：S4 part 2（supply 接线 β）→ S5（dispatch dual-mode）** —— S4 part 1 = Fix B（写入遵循读快照，闭 [SHOULD-2] 复活闸门）✅ 本 session

**本 session = S4 [SHOULD-2] supply/remove 一致性对抗 recon（wf `wf_f26bc215-324`，4-slice+2 对抗 verify 全 CONFIRMED，亲核 iceberg-core 1.10.1 OCC 语义）→ 用户裁定方案 B → 实现 Fix B（写入端遵循语句 MVCC 读快照，`baseSnapshotId=S_read`）✅**（5 文件 additive/dormant；连接器 86/0/0+全量 clean 566/0/0/1，fe-core 9/0/0，全 checkstyle；3-mutation 全杀；对抗 review GO-WITH-NITS 无 blocker）。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ → commit-bridge：recon ✅ + S1 ✅ + S2/S3 ✅（option D）+ S4 part 1=Fix B ✅ → S4 part 2（supply 接线）/S5 待办**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（Fix B，[SHOULD-2] 闸门修复；详设计 §11.7.6）
**[SHOULD-2] 对抗复核 = CONFIRMED 真闸门**：option D 把 remove 侧改成 commit-time 按 `baseSnapshotId`(=`beginWrite` fresh-load 的 current=`S_write`)重派生，supply 侧(BE union 进新 DV 的旧删)仍 scan-time(`S_read`)。两快照无强制相等（扫描 `currentSnapshot()`@plan vs `beginWrite` fresh-load@finalize）→ 并发/缓存过期落 `(S_read,S_write]` → supply≠remove → **删行静默复活**；OCC `validateFromSnapshot(S_write)` 锚错快照（1.10.1 `ancestorsBetween` 左开区间，落 `S_write` 当点的并发提交看不见）不 abort。legacy(remove 也 S_read)反无此复活。**根因=写入端不遵循语句 MVCC 快照钉**（扫描端经 `PluginDrivenScanNode.pinMvccSnapshot` 遵循，`beginWrite` 无视 fresh-load current）。

**Fix B ✅（方案 B，用户裁定；design §11.7.6）= 写入遵循读快照**：① fe-core `PhysicalPlanTranslator.visitPhysicalConnectorTableSink` 把语句 MVCC 钉透传到 write handle（复用**扫描端同一** `PluginDrivenScanNode.applyMvccSnapshotPin`〔package-private→public〕+`MvccUtil.getSnapshotFromContext(targetTable)`；通用零 iceberg 引用）；② 连接器 `IcebergWriteContext`+`readSnapshotId`(4-arg ctor 委派 -1)；③ `IcebergWritePlanProvider.buildWriteContext` 从 `((IcebergTableHandle)handle.getTableHandle()).getSnapshotId()` 提取；④ `IcebergConnectorTransaction.applyBeginGuards`(DELETE/UPDATE/MERGE)`baseSnapshotId = pin>=0 ? Long.valueOf(pin) : getSnapshotIdIfPresent(table)`(两臂 boxed 避空表 NPE)。⇒ supply/remove/OCC锚点 全 = `S_read`，从根消除复活，无重试。**pre-flip byte-identical**（jdbc/es/maxcompute/trino 非 MvccTable→pin 空 no-op；paimon 无 SPI write provider→永不达 pin 行；iceberg 未翻闸休眠）。

**[FU-Bnit-ref → flip e2e]** `buildWriteContext` 只读 `getSnapshotId()` 不读 `getRef()`：DML 读钉解析为具体 snapshotId(不带 FOR TIME AS OF)故 OK；翻闸确认 iceberg `loadSnapshot` 对普通 DML 读产出具体 snapshotId 非 bare ref。**[FU-Bnit-update]** UPDATE 与 DELETE/MERGE 同 `||` 分支已覆盖，不单独测。**[测试-gap]** translator call-site(`:677-678`)未单元测——同 codebase 惯例（DV-019 e2e；helper `PluginDrivenScanNodeMvccPinTest`+消费侧连接器测已覆盖）。

## commit-bridge 子步拆解（每完即 green+mutation+HANDOFF+commit；PR squash；详 design §11.7.2/§11.7.5/§11.7.6）
1. **S1 ✅**（WriteOperation 透传）。**S2/S3 ✅**（option D commit-time re-derive；legacy-map seam 整个消失，[DV-S2-rederive] 登记）。
2. **S4 part 1 = Fix B ✅ 本 session**（写入遵循读快照，`baseSnapshotId=S_read`，闭 [SHOULD-2]）。**S4 part 2 ⟵ 下个 session 起点 = supply 接线（β stash）**：连接器 scan provider plan 时 stash `queryId → Map<originalPath, List<TIcebergDeleteFileDesc>>`(**仅非-equality** content!=2) 到 `IcebergConnector` 单例；`planWrite` 按 queryId 取出、盖 `rewritable_delete_file_sets` 到 `TIcebergDeleteSink`/`TIcebergMergeSink`、即 evict(txn 结束/abort 兜底)。BE union 已验(`viceberg_delete_sink.cpp:578-588` `merged_rows |= previous_rows`，按 `referenced_data_file_path` 精确查，**漏供给=复活**)。key=`originalPath`(BE `RewriteBitmapVisitor` 精确串比)；desc 字段 path+content(DV content==3 须 offset+size；pos-delete 须 file_format)。**Fix B 已使 baseSnapshotId=S_read → supply(S_read)落地即一致**。Trino 不可照搬(BE 执行期写 DV→seam 不可免)。partition-scoped 删 per-data-file 挂接+BE path filter 已处理(翻闸实证)。机制 β 已定（唯一合铁律+时序正确，仿 [DEC-S2] 模式），无须再裁。详 design §11.7.6 尾。
3. **S5（最后、最深、C5 阻塞）**：`IcebergRowLevelDmlTransform` 5 方法 dual-mode（`newExecutor`→`PluginDrivenInsertExecutor`/`setupConflictDetection`→no-op〔中立路供给〕/`finalizeSink`→连接器/`checkMode`·`synthesize`→中立 SPI〔漏算项〕）+ translator `visitPhysicalIcebergMergeSink:601`/`DeleteSink:589` dual-mode（post-flip→`PluginDrivenTableSink`+S1 WriteOperation；**MERGE slot-name loop `:613-615` 上提至 instanceof 分叉之上**〔BE `viceberg_merge_sink.cpp` 按 expr_name 解〕；**DELETE 不需** slot-name loop〔BE 按 block-name 解〕）。

## 🟡 待裁决策（动到时请用户裁；中文先讲清）
- **[DEC-S2] ✅ 裁定 = option D**（见 design §11.7.4–5）。
- **[SHOULD-2] ✅ 裁定 = 方案 B（写入遵循读快照）+ 已实现 Fix B**（本 session，design §11.7.6）。
- **[DEC-S5] 冲突过滤 parity**：中立 `ConnectorPredicate`→native Expression 仅 DROP/widen（安全向、不漏真冲突，但更宽→更多假冲突 retry=perf 非正确性）→裁「接受 widen」vs「要 byte-parity」。**到 S5 再裁**。

## 🟡 已登记 follow-up（非阻塞，勿在本增量做）
- **[SHOULD-2] ✅ 已闭（本 session Fix B）**：supply/remove 都锚 `S_read`。残留 [FU-Bnit-ref]（buildWriteContext 不读 getRef，DML 读钉为具体 snapshotId 故 OK，翻闸确认）+ [FU-Bnit-update]（UPDATE 同分支已覆盖）。
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

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core(③-infra+step1+step2+step3)+commit-bridge S1 无新 DV。**commit-bridge S2/S3=[DV-S2-rederive]（post-flip：旧删源 scan-map→commit-time base-snapshot manifest 读，快照自洽；+有意偏离 Trino：升级表 legacy+DV 双删两者全 removeDeletes，因 BE union 两类）**。S4/S5 预计可能再引入 DV——做到时评估。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant + step6 实现锚点）

- **commit-bridge S4-S5 实现锚点（impl 期 re-grep 防漂移；HEAD = 本 session Fix B commit 后）**：
  - **S2/S3 ✅ DONE（option D）**：连接器 `IcebergConnectorTransaction.collectRewrittenDeleteFiles` → `readExistingFileScopedDeletes(table, baseSnapshotId, touched)`（base 快照 delete-manifest 纯元数据读）；字段+setter 已删。**`IcebergScanPlanProvider.buildDeleteFiles` 未动**。
  - **S4 part 1 = Fix B ✅ DONE**：`PhysicalPlanTranslator.visitPhysicalConnectorTableSink`〔providerTableHandle 后 +`applyMvccSnapshotPin(...,MvccUtil.getSnapshotFromContext(targetTable))`〕；`PluginDrivenScanNode.applyMvccSnapshotPin` public；`IcebergWriteContext.readSnapshotId`；`IcebergWritePlanProvider.buildWriteContext` 提取 `getSnapshotId()`；`IcebergConnectorTransaction.applyBeginGuards`〔DELETE/UPDATE/MERGE 臂 `baseSnapshotId=pin>=0?pin:current`，行 ~213〕。
  - **S4 part 2 = supply 接线（β，下个 session）**：连接器 scan provider plan 时 build+stash 非-equality `Map<originalPath,List<TIcebergDeleteFileDesc>>`（锚点 `IcebergScanPlanProvider.buildDeleteFiles:514`/`convertDelete:541` + `IcebergScanRange.DeleteFile.toThrift`/`getContent()`；key=`buildRange` 的 `originalPath`=`dataFile.path().toString()`）→ `IcebergConnector` 单例新 `ConcurrentHashMap<queryId,...>` stash（scan/write provider 需新 ctor 传单例 `this`）；`IcebergWritePlanProvider.planWrite` 起手 retrieve+evict、盖到 `buildDeleteSink`/`buildMergeSink` 的 `setRewritableDeleteFileSets`。`ConnectorSession.getQueryId()` 已存在。**supply(S_read)与 remove(S_read，Fix B 后)天然一致**。
  - **S5**：`IcebergRowLevelDmlTransform`〔`checkMode:99`/`synthesize:115`/`newExecutor:135`/`setupConflictDetection:191`/`finalizeSink:203`，`handles:71-75` step-1 已纳 PluginDriven〕dual-mode；translator `visitPhysicalIcebergMergeSink:601`/`DeleteSink:589`（仿 `visitPhysicalConnectorTableSink:630`，slot-name loop `:613-615` MERGE 上提/DELETE 不需）；`PluginDrivenInsertExecutor.beginTransaction:81`/`getConnectorTransactionOrNull`；`RowLevelDmlCommand.run:69-109`〔`applyWriteConstraintIfPresent:131-138` 中立冲突路已接〕。`IcebergDmlCommandUtils.checkMergeMode:52`（native `getIcebergTable().properties()`，漏算项→中立 SPI）。
- **S1 实现位（本 session）**：`PluginDrivenTableSink`〔新 7-arg ctor + `WriteOperation` 字段 + 两 handle 站点透传 + inner `PluginDrivenWriteHandle.getWriteOperation()` override〕；`ConnectorWriteHandle.getWriteOperation`（api，默认 INSERT）；`WriteOperation` enum（INSERT/OVERWRITE/DELETE/UPDATE/MERGE/REWRITE）。
- **① handles（step1）/ ② getWritePartitioning+cast（step2）/ ③ row-id 注入（step3）/ partition_columns+parallel-write（C3b-pre）/ 写@branch+OVERWRITE（C3a）/ classifyColumn（C2）/ sys pin-feed（C1）**：dormant，详 git log + design §11.4/§11.5/§11.6。
- **连接器**：`IcebergConnectorMetadata`〔`supportsDelete:496`/`supportsMerge:501`/`buildTableSchema:227`〕/`IcebergConnectorTransaction`〔全 commit + `collectRewrittenDeleteFiles`→`readExistingFileScopedDeletes`（option D commit-time manifest 读；字段/setter 已删）〕/`IcebergWritePlanProvider`〔`planWrite:135`〔INSERT/OVERWRITE/DELETE:157/MERGE:162 全分派〕/`buildWriteContext:270`/`getWritePartitioning`/`getSyntheticWriteColumns`〕/`IcebergScanPlanProvider`〔`buildDeleteFiles:514`，**S2/S3 未动**〕。
- **fe-core legacy 豁免（pre-flip 路 + P6.7 删）**：`IcebergTransaction` + `IcebergDeleteExecutor`/`MergeExecutor`/`InsertExecutor` + `IcebergNereidsUtils` + `IcebergRewritableDeletePlanner` + `IcebergExternalTable.getFullSchema:293`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan/sink/command/executor/transaction/PluginDrivenExternalTable=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B`。**fe-core -am 单类 ~2.5min（首次 -am 全链 ~5-7min）**。后台 task exit code 是 echo 的非 maven 的，读 surefire XML。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（**脚本在仓根 `tools/`，非 `fe/tools/`**；exit 0=PASS）。**连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`）+ fe-core contract UT pin 值防漂移（C2 范式）。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS`+stub 包私 seam；jmockit `Deencapsulation` import = `org.apache.doris.common.jmockit.Deencapsulation`〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 易 trivially-pass → 必变异验真。批量变异脚本范式：cp 备份→exact-string mutate→`mvn test -Dtest=<class>`→查 surefire 期望测试 fail（KILLED）→restore（见 scratchpad/mutate.py）。**OR 能力须双 arm 各测一遍**（merge-only 漏测教训）。**⚠️变异须用「行为禁用形」（如 `… && false`）而非「删引用形」**：删唯一引用某 import/local 的表达式→checkstyle UnusedImport 阻断 build→无 surefire XML→脚本误判「SURVIVED」假阴（step-3 M3b-gate/ctx 踩过：删 `Util.showHiddenColumns()`/`ctx…` 触发；改 `&&false` 保引用复验 KILLED）。同理 surefire XML 缺失须与「测试通过」区分（脚本 failed() 返 None=inconclusive）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔review 重申勿提交，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **本 session commit（commit-bridge S4 part 1 = Fix B）= 5 改/测 + 2 doc**：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`、`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java`、`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergWriteContext.java`、`.../IcebergWritePlanProvider.java`、`.../IcebergConnectorTransaction.java`、测试 `.../IcebergConnectorTransactionTest.java`、`.../IcebergWritePlanProviderTest.java`、+`plan-doc/HANDOFF.md`、`plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔§11.7.6〕。**勿** `git add -A`（尤其勿提交仓根游离 `fe/IcebergScanPlanProvider.java`）。
- commit message：见 `git log` 上一条范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 = ✅ DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ / commit-bridge recon+S1+S2/S3（option D）+S4 part 1=Fix B ✅ → S4 part 2（supply 接线 β）/S5 待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 接近满（2.0T；本 session 起步 76G free，全程 OK）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`（测行为非 style）。**⚠️ 多次增量 build+mutation 后 .class 可能 stale → 误判 flaky；全量验证用 clean `package`（删 target/classes+test-classes）**（本 session 踩过：planWriteThreadsPinned 假 flaky=stale class，clean rebuild 即 566/0/0）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)` / `instanceof Iceberg*`（新 seam）** —— dual-mode helper 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法；通用类（`PluginDrivenExternalTable`）须全经中立 SPI。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（本 session recon `wf_d1116240-aa8` 5-slice+4-verify+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **commit-bridge 是大活、最深、跨 session**：fresh session 全 budget 做；**S2/S3（O2 V3 DeleteFile 迁连接器）= 下个 session 起点，scan↔tx seam 专门 recon**；逐 sub-step green+mutation；**C5 前切忌动 `SPI_READY_TYPES`**。
- **下个 session 起步**：先读 design §11.7（recon+S1+S2-S5 拆解）+ 本 HANDOFF「待裁决策 [DEC-S2]/[DEC-S5]」；动 S2/S3 前请用户裁 [DEC-S2]（S2-a vs S2-b）+ 做 scan↔tx seam recon。
- **上下文超 30% 即交接**。本 session = step 3（③ row-id 注入，27/16/6/60+14 全绿 + 11-mutation 全杀 + 对抗 review GO），在干净节点交接。**下个 session 起点 = step 6（commit-bridge）**：先读 design §10.2/§10.6（O1·O2）+ §11.4-bridge + 本 HANDOFF「impl-recon 决定性结论」；动码前 re-grep translator/executor/collectFor* 锚点；O2 专门 recon。
