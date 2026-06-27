# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C4 步骤 R7（WHERE lowering — nereids Expression→ConnectorPredicate）**

**本 session = C4 R6（fe-core 分布式 rewrite driver，整合 R1–R5 + THE CRUX stash 中立化 + beginWrite begin-once 护栏）= ✅ DONE（单提交）**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core+commit-bridge 全闭 ✅ → C4 进行中（R1/R2/R3/R4/R5/R6 ✅，R7–R8 待）**。翻闸 = 5 commit-stream（C1/C2/C3 ✅ / C4 进行中 / C5 FLIP 不可逆待）。iceberg **仍不在** `SPI_READY_TYPES`。

## 📖 起步必读（动 R7 前）
1. **`plan-doc/tasks/designs/P6.6-C4-ws-rewrite-design.md`** §0 [DEC-C4-3]（WHERE = 现在就做）+ §7 R7 + §3 D2/B1。
2. 本 HANDOFF「R6 完成」+「R7 起步」。**动码前 re-grep 锚点**（设计行号可能漂移）。
3. memory `r6-rewrite-driver-beginwrite-once`（本 session recon 的两个坑 + R6 完整工作集锚点）。

## ✅ 本 session 完成（C4 R6，单提交，dormant）
> recon = `wf_bbebcf58-621`（6 reader + synthesis + adversarial critic）；实证推翻设计两处、补两个正确性坑。**11 改 + 4 新**（2 main + 2 test）。

- **派发**：`ConnectorExecuteAction.execute` 按中立 `procedureOps.getExecutionMode(actionType)` 分流——`DISTRIBUTED`→新 `ConnectorRewriteDriver`；`SINGLE_CALL`→既有 `procedureOps.execute`。**WHERE 拒绝留在最前**（R6 无 WHERE；保 `verifyNoInteractions` 既有契约——R7 才把 WHERE 拒绝拆进 SINGLE_CALL 臂 + DISTRIBUTED 走 lowering）。
- **driver**（`ConnectorRewriteDriver`，fe-core `…commands.execute`）：① `planRewrite`→N `ConnectorRewriteGroup`，空组早返 4 列零行（不开事务）；② `metadata.beginTransaction`+`PluginDrivenTransactionManager.begin` 开 1 共享事务；③ N×`ConnectorRewriteGroupTask` 经 `TransientTaskManager` 并发（CountDownLatch collector，首错 cancel 余组）；④ **组跑完后**`registerRewriteSourceFiles(union)`（坑2）；⑤ `commit(txnId)`（失败不再 rollback——manager commit 自带 finally 清理）；⑥ post-commit `getRewriteAddedDataFilesCount` + 组统计 → 4 列（rewritten_data_files_count INT / added_data_files_count INT / rewritten_bytes_count INT / removed_delete_files_count BIGINT，legacy 序+类型，bytes=INT 故意保 quirk）。
- **group task**（`ConnectorRewriteGroupTask`，仿 legacy `RewriteGroupTask` 中立化）：每组 fresh ConnectContext+StatementContext，stash 作用域 path-set + 共享事务，建 `UnboundConnectorTableSink(rewrite=true)`+`RewriteTableCommand`，`initPlan`（finalizeSink 绑共享事务到 sink session）→ `getCoordinator().setTxnId(sharedTxnId)`→`executeSingleInsert`。**无** target-file-size/GATHER strategy（GATHER 经 sink isRewrite；sizing defer，见 FU）。
- **THE CRUX stash 中立化**：`StatementContext` 加并行中立 `List<String> rewriteSourceFilePaths`（**不动** legacy iceberg-typed `icebergRewriteFileScanTasks`，其两处 pre-flip live 读写 `RewriteGroupTask:117`/`IcebergScanNode:498` 保持）+ `ConnectorTransaction rewriteSharedTransaction` stash；`PluginDrivenScanNode.pinRewriteFileScope`（**非消费式**，仿 `applyMvccSnapshotPin`，3 取数点 getSplits/startSplit/getOrLoadPropertiesResult 均接 pinMvccSnapshot 之后）；新中立 SPI `ConnectorMetadata.applyRewriteFileScope`（default 返回 handle，`IcebergConnectorMetadata` override→`withRewriteFileScope`）；**空/null path-set = no-pin（非 match-nothing）**。
- **坑1（设计漏，我独立发现）= beginWrite N×并发 churn**：`IcebergWritePlanProvider.planWrite:158` 每次无条件 `beginWrite`→N 组并发各调一次→重建共享 SDK 事务 + 重定 OCC 锚点（数据竞争）。修=`IcebergConnectorTransaction.beginWrite` 加 **synchronized begin-once 护栏**（`writeStarted`，成功后才置；首组载表+定快照，余 no-op）；普通单条写零影响（Trino「begin once」对齐）。
- **坑2（critic 发现，已实证）= register 顺序 NPE**：`registerRewriteSourceFiles:303` 立即 `table.newScan()`，table 只 beginWrite 载→driver 登记挪到组跑完后、commit 前；+ register 加 `table==null` fail-loud。
- **sink REWRITE 串接**：`PhysicalConnectorTableSink` 加 public `isRewrite()`；`PhysicalPlanTranslator.visitPhysicalConnectorTableSink` 仿 DELETE/MERGE `buildPluginRowLevelDmlSink` 用 7-arg `PluginDrivenTableSink` ctor 串 `WriteOperation.REWRITE`（isRewrite 时）；`ConnectorRewriteExecutor` 加 `finalizeSink` override 从 `ctx.getStatementContext().getRewriteSharedTransaction()` 绑共享事务到 sink session（仿 `PluginDrivenInsertExecutor.finalizeSink`）。

## ✅ 本 session 验证
- 连接器：`IcebergConnectorTransactionTest` **64/0**、`IcebergConnectorMetadataTest` **35/0**（3 新，无 Mockito 真 InMemoryCatalog）。compile+checkstyle 绿。
- fe-core blast-radius 回归 **18 类/99 用例 0 fail**（含全部 `PluginDrivenScanNode*` + `PhysicalPlanTranslator*` + `ConnectorExecuteActionTest` + `PhysicalConnectorTableSinkTest`）；新 `PluginDrivenScanNodeRewriteFileScopePinTest` **3/0**、`ConnectorRewriteDriverTest` **2/0**。
- **变异（Rule 9/12）全 KILLED**：3 连接器（begin-once 护栏 / register table==null / applyRewriteFileScope override）+ 2 fe-core（scope pin 应用 / driver 空计划早返）。脚本 `scratchpad/mutate_r6_connector.py`·`mutate_r6_fecore.py`（已 restore，无 .bak/无 MUTANT）。
- **真分布式写路径未跑**（组执行 / pin 3 注入点 / 共享事务绑定 / 并发 begin-once / register 顺序 / 真 commit OCC）= UT-mock + 读核，真 e2e = R8 flip rehearsal（诚实标注，勿谎称）。

## 🚦 R7 起步（WHERE lowering，DEC-C4-3 末步）
> 现状：R6 在派发入口对**任何** EXECUTE 的 WHERE 都 fail-loud（最前拒）。R7 = 让 DISTRIBUTED rewrite 接 WHERE。
1. `ConnectorExecuteAction`：把顶部 WHERE 拒绝**拆**——SINGLE_CALL 八个纯 SDK 过程仍拒；DISTRIBUTED 把 nereids `Optional<Expression>` lower 成中立 `ConnectorPredicate` 传给 `driver`（再传 `planRewrite` 第 5 参，现 R6 传 null）。**注意**：这会让 DISTRIBUTED+WHERE 路径碰 `getExecutionMode`/连接器——须新增/调整 `ConnectorExecuteActionTest`（现 `verifyNoInteractions` 假设 WHERE→不碰连接器，对 SINGLE_CALL 仍真，对 DISTRIBUTED 须改）。
2. lowering 复用连接器侧既有 `IcebergPredicateConverter` 的中立前端：fe-core 出 `ConnectorPredicate`，连接器 `IcebergProcedureOps.planRewrite`→`RewriteDataFilePlanner` 已收 where（`tableScan.filter`）。re-grep `ConnectorPredicate` 构造 + `applyWriteConstraint:276` 范式（delete/merge 已有 nereids→ConnectorPredicate 先例，找它）。
3. driver `planRewrite(..., predicate, ...)` 把 lowered predicate 透传（现 hardcode null）。
4. green：mock 连接器收到非 null predicate；e2e WHERE rewrite 留 R8。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（C4 dead 子树 `IcebergRewriteDataFilesAction`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor` + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink` + `StatementContext` 旧 iceberg-typed stash）保留 iceberg 引用合法。**R6 新增通用类**（`ConnectorRewriteDriver`/`ConnectorRewriteGroupTask`/中立 stash/`applyRewriteFileScope`/`pinRewriteFileScope`/dispatch 按 `executionMode`/sink isRewrite 串 `WriteOperation.REWRITE`）全经中立 SPI，**无** instanceof Iceberg（已核）。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 进行中，C5 待）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 进行中** / C5 FLIP 待）。

- **[C4 进行中 = 当前]** rewrite_data_files 翻闸就绪（Option B 全对等）。**R1/R2/R3/R4/R5/R6 ✅（executionMode SPI / scan path-set 作用域 / planRewrite SPI / sink-bind+GATHER / transaction rewrite SPI gap / 分布式 driver+CRUX stash 中立化+begin-once 护栏）→ R7（WHERE lowering）→ R8（flip rehearsal，flip-gated）**。详设计 §7。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]。**C4 R1–R6 无新 DV**（dormant，行为零变更）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞，勿在 C4 增量做）
- **[FU-rewrite-output-sizing]（R6 新登）** 中立 driver **未**线程 target-file-size + 自适应并行度（legacy `RewriteGroupTask` 经 iceberg 会话变量 `iceberg_write_target_file_size_bytes`〔TQueryOptions 字段，非 sink〕传，并按 `totalSize/targetSize` vs availableBe 选 GATHER/parallelism）。R6 各组一律经 sink `isRewrite`→GATHER 收单写者（正确但大组慢、输出文件不按大小调优）。**仅影响真 BE 写盘（R8 rehearsal 才触及）**。翻闸前修：planRewrite 出 target-file-size（中立 wrapper 或 group 字段）+ driver 设会话变量 + 复算并行度；勿让 fe-core 解析 iceberg 属性名。
- **[FU-flip-e2e]（R6 扩）** commit-bridge + C4 R1–R6 全程 pre-flip UT 锁，但真翻闸端到端（旧删不复活 / operation·row_id BE 解析 / OCC / **rewrite 每组只扫自己文件〔pin 3 注入点〕/ 共享事务跨组绑定 / 并发 begin-once / register 顺序 / GATHER 输出文件数 / register re-scan 路径匹配**）**未跑**（CI-gated/flip-gated，勿谎称）。
- **[FU-connector-bind-visibility]** 见 git log 历史（R4 范围外，翻闸前若需对齐普通插入语义引中立「是否 row-lineage 写入列」表能力，禁 `IcebergUtils.isIcebergRowLineageColumn`）。
- **[FU-rewrite-rescan-perf]** R5 `registerRewriteSourceFiles` commit 前 re-scan `planFiles()`（O(表文件数)）；翻闸后大表慢可按 queryId 缓 DataFile（仿 rewritableDeleteStash）。
- **[FU-broker-write]** 连接器三 write builder 未填 `setBrokerAddresses`；翻闸前若需 broker 写盘三 builder 一并补。
- **[FU-getRowIdColumn]** `IcebergMergeCommand.getRowIdColumn(562)` 仍 `IcebergExternalTable`；翻闸/P6.7 核是否 dead。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）。**连接器模块**（`fe-connector-api`/`-iceberg`）build 快（~30s），可前台；**fe-core -am 单类首次 ~2-3min**→`run_in_background:true` 再读 surefire **XML**（python ET）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。后台 build 日志里 `mismatched input '->'`/`which: syntax error` = gensrc codegen 噪声，非编译错（看 `BUILD FAILURE`/`cannot find symbol` + .class mtime）。
- **⚠️ checkstyle 全量 build 跑**：import 同组无空行 + 组内**字母序**（本 session 多处加 import 已按序：`java.util.Set` 在 Optional 后、`connector.api.handle.ConnectorTransaction` 在 common 后 datasource 前）。加 import 后 `mvn checkstyle:check -pl :<mod>` 快验。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器**无 Mockito**（真 InMemoryCatalog + `RecordingIcebergCatalogOps.log`/`RecordingConnectorContext.authCount`）；fe-core **Mockito**（`mockito-inline`，mock `ConnectContext`/`PluginDrivenExternalCatalog`/SPI；静态 pin 直接 mock `ConnectorMetadata` 仿 `PluginDrivenScanNodeMvccPinTest`）。live-e2e CI-gated，勿谎称。
- **mutation-check（Rule 9/12）**：范式 `scratchpad/mutate_r6_connector.py`/`mutate_r6_fecore.py`：cp 备份→「行为禁用形」`if(false)`/翻枚举/`return handle`→`mvn test -Dtest=<class>`→查 surefire `Failures:`/`Errors:`（KILLED）→restore + `os.utime`。**⚠️ exact-string 锚点须唯一**；**⚠️python3.6 无 capture_output/text=**；**⚠️ commit 前核已 restore**（无 `&& false`/`if(false) MUTANT`/`.bak`）。
- **cwd 会被 harness 重置**→一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔真文件在 `fe/fe-connector/...`，勿提交〕·`plan-doc/reviews/P5-paimon-rereview3-*`）。
- **R1=`1bddf3426d6` / R2=`0a0d5b8de83` / R3=`a7c2732d984` / R4a=`a3d7210e892` / R4b=`12fe50ee88e` / R5=`e956f0edc45` / R6=`0735aac280e`**；HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 R1–R6）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core+commit-bridge 全闭 ✅ → C4 R1/R2/R3/R4/R5/R6 ✅ / R7–R8 待 → C5 翻闸**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session ~86G free，96% used）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。多次增量 build 后 .class 可能 stale→误判；fe-core 验证用 clean。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式/可达性可能过时或错** —— 动码前先 recon（grep+实证）再信文档。R6 recon 实证推翻设计「stash 改类型」（实为新增并行字段，旧 live 不动）+ 补两个设计未覆盖正确性坑（begin-once / register 顺序）。
- **clean-room 对抗 review 偏好**：大改动 recon = 多 reader 对抗 + synthesis + critic（R6 = `wf_bbebcf58-621`），再先 code 独立核（本 session 独立读 stash/dispatch/legacy 机制，与 critic 交叉验，发现 critic 漏的 N×beginWrite 坑、纠 critic 的 consume-once-pin 自相矛盾）。
- **既有 Doris/Trino 决策优先于新设计**：begin-once 经用户裁（幂等护栏，Trino 对齐）；register 顺序、result 4 列、collector 范式照搬 legacy `RewriteDataFileExecutor`。
- **C4 逐子步**：R7→R8，每步 additive/dormant + green + mutation + commit + HANDOFF。**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**。本 session 完成 C4 R6（单提交，11 改 + 4 新），在干净节点交接 R7。
