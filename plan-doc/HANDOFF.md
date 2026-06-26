# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 commit-bridge（step 6，最后一步、最深）续：S2/S3（O2 V3 DeleteFile 迁连接器）** —— step6 起步 recon + S1 ✅ 本 session

**本 session = recon（独立 + 对抗 wf `wf_d1116240-aa8`）+ commit-bridge 子步 S1 ✅**（1 增量 additive/dormant；`PluginDrivenTableSink` WriteOperation 透传；8/0/0+2/0/0 全绿 + 3-mutation 全杀 + reviewer GO）。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ → commit-bridge：recon ✅ + S1 ✅ → S2/S3/S4/S5 待办**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（commit-bridge recon + S1；详设计 §11.7）
**recon 决定性结论（动 S2–S5 前必读 design §11.7.1）**：连接器 commit 机器**全建好、休眠**；commit-bridge 几乎全是「接线 + dual-mode 改道」，**不需新 commit 管线**。
- **冲突检测中立路已接好、休眠**：`RowLevelDmlCommand.applyWriteConstraintIfPresent:131-138` 在 executor 暴露非 null `ConnectorTransaction` 时即 fire `applyWriteConstraint(ConnectorPredicate)`；`extractWriteConstraint:210` 已返中立。⇒ conflict-detection 迁移**基本完成**（S5 只需 plugin-arm `setupConflictDetection` 不再 fire native）。
- **连接器 `planWrite:150` 已分派 DELETE→`buildDeleteSink:157`/UPDATE·MERGE→`buildMergeSink:162`**（`:170` default-throw 非缺口）；`buildWriteContext:270` 读 `handle.getWriteOperation()`。
- **O2 真相**：post-flip scan=`PluginDrivenScanNode`（与 `IcebergScanNode` 都 extends `FileQueryScanNode`=siblings），`IcebergRewritableDeletePlanner.collect:64` `instanceof IcebergScanNode` 过滤→静默 empty→V3 DV 不 removeDeletes=**静默正确性回归**。native map 来源=fe-core `IcebergScanNode:354-360`；连接器 `buildDeleteFiles:514` 有 native `task.deletes()`+`rawDataPath:490` 但转 Serializable 后**丢弃 native**。消费侧 `collectRewrittenDeleteFiles:872`/`setRewrittenDeleteFilesByReferencedDataFile:271`（**仅测试调**）**全建**→修=**连接器自留 native map 喂自己 transaction**。
- **⚠️ 新发现（HANDOFF 旧 step-6 范围漏算）**：`IcebergRowLevelDmlTransform.checkMode:99`→`checkMergeMode:52` 读 `getIcebergTable().properties()`（native，判 CoW/MoR）+ `synthesize:115` cast `(IcebergExternalTable)` → **post-flip 必 CCE**，native 表过不了 classloader → 须中立 SPI 取 write-mode +（判 synthesize 仅需 Doris 级 column API）。**并入 S5**。

**S1 ✅（WriteOperation 透传，design §11.7.3）**：`PluginDrivenTableSink` 加中立 `WriteOperation` 字段 + 7-arg ctor（5→6→7，6-arg 委派 INSERT）+ 两 handle 站点透传 + inner `PluginDrivenWriteHandle` 补 `@Override getWriteOperation()`（原漏→恒 SPI default INSERT→planWrite 永建 INSERT sink）。唯一生产 caller `visitPhysicalConnectorTableSink:677` 用 6-arg→INSERT 默认→**byte-identical**。`PluginDrivenTableSinkTest` **8/0/0**（+4：bind 默认INSERT/MERGE/DELETE + explain MERGE）+ 姊妹 `...BindingTest` 2/0/0；**3-mutation 全杀**（MUT bind-site / explain-site / handle-field，行为禁用形）；iron-law 干净（仅引中立 `WriteOperation`）；reviewer GO；`SPI_READY_TYPES` 未改；0 新 DV。

## commit-bridge 子步拆解 S1–S5（每完即 green+mutation+HANDOFF+commit；PR squash；详 design §11.7.2）
1. **S1 ✅ DONE 本 session**（WriteOperation 透传）。
2. **S2/S3 ⟵ 下个 session 起点（O2，最深/危险）**：连接器 `IcebergScanPlanProvider.buildDeleteFiles:514` 自留 native `Map<rawDataPath,List<DeleteFile>>`（滤非-equality 仿 `IcebergScanNode:356`，保 PUFFIN dedup 元数据）+ 生产 caller 喂 `setRewrittenDeleteFilesByReferencedDataFile:271`。**🔴 须专门 recon = 连接器 read-scan↔write `IcebergConnectorTransaction` 的实例/线程交接 seam**（legacy 经 `planner.getScanNodes()`，连接器等价 seam 须定位或设计；per-request ctx 自留 vs `beginWrite` pinned 重扫，皆须 pinned，荐自留）。**决策见下 [DEC-S2]**。
3. **S4**：连接器 sink 盖 `rewritable_delete_file_sets` thrift（BE V3 通道）；legacy 在 finalize 后盖（`IcebergDeleteExecutor.finalizeSinkForDelete:73`），连接器 `planWrite` 在 bind 期 → **open：bind 期盖 vs 需 post-finalize 钩子**。S3+S4 须同落否则 V3 半对。
4. **S5（最后、最深、C5 阻塞）**：`IcebergRowLevelDmlTransform` 5 方法 dual-mode（`newExecutor`→`PluginDrivenInsertExecutor`/`setupConflictDetection`→no-op〔中立路供给〕/`finalizeSink`→连接器/`checkMode`·`synthesize`→中立 SPI〔漏算项〕）+ translator `visitPhysicalIcebergMergeSink:601`/`DeleteSink:589` dual-mode（post-flip→`PluginDrivenTableSink`+S1 WriteOperation；**MERGE slot-name loop `:613-615` 上提至 instanceof 分叉之上**〔BE `viceberg_merge_sink.cpp` 按 expr_name 解〕；**DELETE 不需** slot-name loop〔BE 按 block-name 解〕）。

## 🟡 待裁决策（动 S2–S5 前请用户裁；中文先讲清）
- **[DEC-S2] O2 SPI 策略**：**S2-a**（连接器整体自留 native map、**无**新 fe-core→连接器 SPI；iron-law 最干净，synthesis 荐）vs **S2-b**（中立 opaque-byte SPI on `ConnectorTransaction`，仿 `addCommitData(byte[])`，fallback）。
- **[DEC-S5] 冲突过滤 parity**：中立 `ConnectorPredicate`→native Expression 仅 DROP/widen（安全向、不漏真冲突，但更宽→更多假冲突 retry=perf 非正确性）→裁「接受 widen」vs「要 byte-parity」。

## 🟡 已登记 follow-up（非阻塞，勿在本增量做）
- **[FU-step1-nullconn → cutover]** `IcebergRowLevelDmlTransform.pluginConnectorSupportsRowLevelDml`（step-1）unguarded `getConnector()`；step-3 同名 helper 已 guard → cutover 前对齐。
- **[FU-order → flip e2e]** post-flip `getFullSchema` 列序 [base,v3-lineage,row-id]≠legacy [base,row-id,v3-lineage]。**benign**（按名/field-id 匹配）+ dormant。
- **[FU-remap → cutover]** `PluginDrivenExternalTable.toSchemaCacheValue:188` remap 丢 invisible/uniqueId（对 iceberg 安全）。
- **[FU-S1-update-test]** reviewer nit：未补 UPDATE 测（`PluginDrivenTableSink` 对 op 不分支、UPDATE 同 MERGE 路）→不补，记此。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。保留的 `PhysicalIcebergMergeSink`/`DeleteSink`/`IcebergRowLevelDmlTransform`/`Iceberg*Command`/`IcebergNereidsUtils`/`IcebergTransaction`/`IcebergRewritableDeletePlanner`/`Iceberg*Executor` = **legacy 豁免**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支落这些豁免类合法）；V3 DeleteFile seam 全连接器内（无 fe-core native 耦合，S2-a 下无 `instanceof IcebergConnectorTransaction` downcast）。**S1 注入点 `PluginDrivenTableSink` 是通用 fe-core 类（非豁免）→ 仅引中立 `WriteOperation`，已合规。**

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre 已闭，C3b-core 设计+recon 闭、step1-3 已实现，step6+C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。
- **[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。
- **[C3a ✅]** INSERT OVERWRITE 能力 + 完整接通 @branch（[D-070]，闭 DV-T06-branch）。
- **[C3b-pre ✅]** partition_columns emit + ④b parallel-write（[D-071]）。
- **[C3b-core step1-3 ✅ / commit-bridge: recon ✅ + S1 ✅ / S2-S5 待办]**（[D-072]+[D-073]）：① handles ✅ + ② getWritePartitioning helper ✅ + ③ row-id 双注入 ✅ + commit-bridge[Option(a) 路由连接器]：S1 WriteOperation 透传 ✅ → S2/S3（O2 迁连接器）/S4/S5 待。详 §10+§11（§11.7 = 本 session recon+S1）。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 throw）；`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / mvcc+partition-stats capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（精确类白名单）→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅ 本 session 闭**：隐藏列注入——v3-lineage 经 carrier=A 连接器 emit（③-infra part2）；请求级 STRUCT row-id 经 fe-core `getFullSchema` override + injector guard（step3）。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core(③-infra+step1+step2+step3)+commit-bridge S1 无新 DV。**S2-S5 预计可能引入 DV（post-flip 路由）——做到时评估**。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant + step6 实现锚点）

- **commit-bridge S2-S5 实现锚点（impl 期 re-grep 防漂移；HEAD = 本 session S1 commit 后）**：
  - **S2/S3（O2）**：连接器 `IcebergScanPlanProvider.buildDeleteFiles:514`（native `task.deletes()` + `rawDataPath:490`，现丢弃 native）→ 自留 `Map<rawDataPath,List<DeleteFile>>`（滤非-equality 仿 fe-core `IcebergScanNode:356`）喂 `IcebergConnectorTransaction.setRewrittenDeleteFilesByReferencedDataFile:271`（消费 `collectRewrittenDeleteFiles:872`/`shouldRewritePreviousDeleteFiles:840`/`removeDeletes:557,612` 全建）。**🔴 scan↔tx seam 专门 recon**。fe-core `IcebergRewritableDeletePlanner.collect:52-86`（legacy 豁免，post-flip 不作源）。
  - **S4**：连接器 `IcebergWritePlanProvider.buildDeleteSink`/`buildMergeSink`（盖 `rewritable_delete_file_sets` thrift）。
  - **S5**：`IcebergRowLevelDmlTransform`〔`checkMode:99`/`synthesize:115`/`newExecutor:135`/`setupConflictDetection:191`/`finalizeSink:203`，`handles:71-75` step-1 已纳 PluginDriven〕dual-mode；translator `visitPhysicalIcebergMergeSink:601`/`DeleteSink:589`（仿 `visitPhysicalConnectorTableSink:630`，slot-name loop `:613-615` MERGE 上提/DELETE 不需）；`PluginDrivenInsertExecutor.beginTransaction:81`/`getConnectorTransactionOrNull`；`RowLevelDmlCommand.run:69-109`〔`applyWriteConstraintIfPresent:131-138` 中立冲突路已接〕。`IcebergDmlCommandUtils.checkMergeMode:52`（native `getIcebergTable().properties()`，漏算项→中立 SPI）。
- **S1 实现位（本 session）**：`PluginDrivenTableSink`〔新 7-arg ctor + `WriteOperation` 字段 + 两 handle 站点透传 + inner `PluginDrivenWriteHandle.getWriteOperation()` override〕；`ConnectorWriteHandle.getWriteOperation`（api，默认 INSERT）；`WriteOperation` enum（INSERT/OVERWRITE/DELETE/UPDATE/MERGE/REWRITE）。
- **① handles（step1）/ ② getWritePartitioning+cast（step2）/ ③ row-id 注入（step3）/ partition_columns+parallel-write（C3b-pre）/ 写@branch+OVERWRITE（C3a）/ classifyColumn（C2）/ sys pin-feed（C1）**：dormant，详 git log + design §11.4/§11.5/§11.6。
- **连接器**：`IcebergConnectorMetadata`〔`supportsDelete:496`/`supportsMerge:501`/`buildTableSchema:227`〕/`IcebergConnectorTransaction`〔全 commit + `setRewrittenDeleteFilesByReferencedDataFile:271`〕/`IcebergWritePlanProvider`〔`planWrite:135`〔INSERT/OVERWRITE/DELETE:157/MERGE:162 全分派〕/`buildWriteContext:270`/`getWritePartitioning`/`getSyntheticWriteColumns`〕/`IcebergScanPlanProvider`〔`buildDeleteFiles:514`〕。
- **fe-core legacy 豁免（pre-flip 路 + P6.7 删）**：`IcebergTransaction` + `IcebergDeleteExecutor`/`MergeExecutor`/`InsertExecutor` + `IcebergNereidsUtils` + `IcebergRewritableDeletePlanner` + `IcebergExternalTable.getFullSchema:293`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan/sink/command/executor/transaction/PluginDrivenExternalTable=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B`。**fe-core -am 单类 ~2.5min（首次 -am 全链 ~5-7min）**。后台 task exit code 是 echo 的非 maven 的，读 surefire XML。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（**脚本在仓根 `tools/`，非 `fe/tools/`**；exit 0=PASS）。**连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`）+ fe-core contract UT pin 值防漂移（C2 范式）。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS`+stub 包私 seam；jmockit `Deencapsulation` import = `org.apache.doris.common.jmockit.Deencapsulation`〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 易 trivially-pass → 必变异验真。批量变异脚本范式：cp 备份→exact-string mutate→`mvn test -Dtest=<class>`→查 surefire 期望测试 fail（KILLED）→restore（见 scratchpad/mutate.py）。**OR 能力须双 arm 各测一遍**（merge-only 漏测教训）。**⚠️变异须用「行为禁用形」（如 `… && false`）而非「删引用形」**：删唯一引用某 import/local 的表达式→checkstyle UnusedImport 阻断 build→无 surefire XML→脚本误判「SURVIVED」假阴（step-3 M3b-gate/ctx 踩过：删 `Util.showHiddenColumns()`/`ctx…` 触发；改 `&&false` 保引用复验 KILLED）。同理 surefire XML 缺失须与「测试通过」区分（脚本 failed() 返 None=inconclusive）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔review 重申勿提交，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **本 session commit（commit-bridge S1）= 1 改 + 1 测 + 2 doc**：`fe/fe-core/src/main/java/org/apache/doris/planner/PluginDrivenTableSink.java`、`fe/fe-core/src/test/java/org/apache/doris/planner/PluginDrivenTableSinkTest.java`、`plan-doc/HANDOFF.md`、`plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔§11.7〕。**勿** `git add -A`。
- commit message：见 `git log` 上一条范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 = ✅ DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ / commit-bridge recon+S1 ✅ → S2/S3/S4/S5 待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 接近满（2.0T，本 session 一度仅 ~27M free → maven checkstyle cache-persist `No space left` 致 mutation INCONCLUSIVE）。用户已腾空间（~20G）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`（测行为非 style）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)` / `instanceof Iceberg*`（新 seam）** —— dual-mode helper 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法；通用类（`PluginDrivenExternalTable`）须全经中立 SPI。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（本 session recon `wf_d1116240-aa8` 5-slice+4-verify+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **commit-bridge 是大活、最深、跨 session**：fresh session 全 budget 做；**S2/S3（O2 V3 DeleteFile 迁连接器）= 下个 session 起点，scan↔tx seam 专门 recon**；逐 sub-step green+mutation；**C5 前切忌动 `SPI_READY_TYPES`**。
- **下个 session 起步**：先读 design §11.7（recon+S1+S2-S5 拆解）+ 本 HANDOFF「待裁决策 [DEC-S2]/[DEC-S5]」；动 S2/S3 前请用户裁 [DEC-S2]（S2-a vs S2-b）+ 做 scan↔tx seam recon。
- **上下文超 30% 即交接**。本 session = step 3（③ row-id 注入，27/16/6/60+14 全绿 + 11-mutation 全杀 + 对抗 review GO），在干净节点交接。**下个 session 起点 = step 6（commit-bridge）**：先读 design §10.2/§10.6（O1·O2）+ §11.4-bridge + 本 HANDOFF「impl-recon 决定性结论」；动码前 re-grep translator/executor/collectFor* 锚点；O2 专门 recon。
