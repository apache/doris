# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **C4 步骤 R6（fe-core 分布式 rewrite driver — 整合 R1–R5）**

**本 session = C4 R4（sink-bind 中立化 + GATHER override）= R4a ✅ + R4b ✅ + R5（transaction 中立 rewrite SPI gap）✅**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core+commit-bridge 全闭 ✅ → C4 进行中（R1/R2/R3/R4/R5 ✅，R6–R8 待）**。翻闸 = 5 commit-stream（C1/C2/C3 ✅ / C4 进行中 / C5 FLIP 不可逆待）。iceberg **仍不在** `SPI_READY_TYPES`。

## 📖 起步必读（动 R6 前）= **R6 是 fe-core 重整合步，build 慢，先 recon**
1. **`plan-doc/tasks/designs/P6.6-C4-ws-rewrite-design.md`** §1（翻闸后路由）+ §3 D2（THE CRUX stash）+ §6（executionMode routing）+ §7 R6。
2. 本 HANDOFF「C4 关键认知」+「R6 起步」。**动码前 re-grep 锚点**（设计行号可能漂移）。
3. R6 整合 R1–R5 既有件：R1 `ProcedureExecutionMode`/`getExecutionMode`、R2 `IcebergTableHandle.withRewriteFileScope`、R3 `planRewrite`→`ConnectorRewriteGroup`、R4 `UnboundConnectorTableSink(rewrite)`+`ConnectorRewriteExecutor`、R5 `registerRewriteSourceFiles`+`getRewriteAddedDataFilesCount`。**动前先读这 5 件的真实签名**。

## ✅ 本 session 完成（C4 R4 + R5）
- **R4a（commit `a3d7210e892`，dormant）**：中立 `rewrite` 标记穿连接器 sink 链（`UnboundConnectorTableSink`+rewrite 字段〔10 参规范构造，保留 5/9 参重载默认 false，3 个 `UnboundTableSinkCreator` 调用方零改动〕→ `LogicalConnectorTableSink`+rewrite〔纳入 equals/hashCode 身份〕→ `PhysicalConnectorTableSink`+isRewrite〔`getRequirePhysicalProperties` 顶部 `if(isRewrite) return GATHER` 短路，赢过分区 shuffle〕；`BindSink:878` 传 `sink.isRewrite()`〔**无** row-lineage 改动，见下〕；实现规则透传）。**用户裁（2026-06-27）去掉 row-lineage 绑定分支**：通用绑定 `selectConnectorSinkBindColumns` 用 `getBaseSchema(true)` 已含隐藏列，rewrite 不丢 row-lineage；加 `IcebergUtils.isIcebergRowLineageColumn` 违铁律且无 live caller。验证 `PhysicalConnectorTableSinkTest` 7/0 + `BindConnectorSinkStaticPartitionTest` 5/0；GATHER override 1-mutation KILLED。
- **R4b（commit `12fe50ee88e`，dormant）**：写入半。新 `ConnectorRewriteExecutor`（fe-core，仿 `IcebergRewriteExecutor` extends `BaseExternalTableInsertExecutor`，no-op beforeExec/doBeforeCommit〔事务由 coordinator 外部持有〕，`transactionType`→中立 `UNKNOWN`）；`RewriteTableCommand:188` 加中立 `else if (physicalSink instanceof PhysicalConnectorTableSink)→ConnectorRewriteExecutor`（按 sink 类型路由不加 instanceof Iceberg；rewrite 走 sink 不走 ctx 故无 setRewriting）；连接器 `IcebergWritePlanProvider.planWrite` 加 `case REWRITE`+`buildRewriteSink`（复用 buildSink，仅 `write_type=REWRITE` + fv≥3 row-lineage schema 两 delta，byte-identical 于 legacy `planner.IcebergTableSink.bindDataSink`；rewrite/overwrite 互斥 fail-loud）。验证 `IcebergWritePlanProviderTest` 36/0（3 新）；3-mutation KILLED。
- **R5（commit `e956f0edc45`，dormant）**：连接器 transaction 中立 rewrite SPI gap。`ConnectorTransaction` SPI 加 default `registerRewriteSourceFiles(Set<String>)`+`getRewriteAddedDataFilesCount()`（default-throw 范式）；`IcebergConnectorTransaction` 实现：register 在 `startingSnapshotId`（OCC 锚）处 re-scan `planFiles()` 把 RAW String 路径解析回 `DataFile`（镜像 `commitReplaceTxn` re-scan + commit-time re-derive 决策；按 path 去重；任一缺失 fail-loud），喂既有 `filesToDelete`；getRewriteAddedDataFilesCount 委托既有 `getFilesToAddCount`。**recon 实证简化**：legacy `RewriteDataFileExecutor:117-128` 仅 `addedDataFilesCount` 取自 transaction，余三列（rewritten_data_files / rewritten_bytes / removed_delete_files）来自 `ConnectorRewriteGroup`〔R3 出，含 dataFilePaths + 3 统计〕→"统计提升"收窄为单个 added-files-count。验证 `IcebergConnectorTransactionTest` 62/0（4 新）；3-mutation KILLED。

## 🔑 C4 关键认知（动码前必懂）
- **翻闸后 fe-core `IcebergRewriteDataFilesAction` 子树整条不可达**（`ExecuteActionFactory:61` instanceof PluginDriven **先**分流到 `ConnectorExecuteAction`）。该子树（action/`RewriteDataFileExecutor`/`RewriteGroupTask`/`IcebergRewriteExecutor`）= legacy-exempt，留 pre-flip 活路径，P6.7 删。
- **真 fix 落点** = `ConnectorExecuteAction` 按 `executionMode` 分派 → **新 fe-core 分布式 rewrite driver（R6 建）**。连接器 `IcebergExecuteActionFactory:88` 对 rewrite **保持 throw**（不走 `execute` 单调用路）。
- **THE CRUX（R6 必解）**：每组 INSERT-SELECT 须只扫自己 bin-pack 那批文件。R2 已建连接器侧过滤（`IcebergTableHandle.withRewriteFileScope`→`planScanInternal:332`）；**但 fe-core 侧 stash 仍 iceberg-typed**（`StatementContext:322` `List<FileScanTask>`〔自带 TODO〕，pre-flip `IcebergScanNode:498` 消费；翻闸后 scan 走 `PluginDrivenScanNode` 不读该信道）→ R6 须把 stash 改 `List<String>` raw-path + `PluginDrivenScanNode` 读取并经 pin SPI（仿 `applyMvccSnapshotPin:647`）把 path-scope 下传到连接器 scan handle。
- **GATHER 信号双轨**：pre-flip iceberg 走 `StatementContext.useGatherForIcebergRewrite`；post-flip 连接器走 **sink 字段** `PhysicalConnectorTableSink.isRewrite`（R6 driver 建 `UnboundConnectorTableSink(rewrite=true)` 起源，经 bind→logical→physical 携带）。

## 🚦 R6 起步（fe-core 分布式 rewrite driver，dormant；StmtExecutor 半留 fe-core）
> R6 = 编排 R1–R5：在 `ConnectorExecuteAction` 按 executionMode 分派到新 driver，driver 跑 1 REWRITE txn + N×INSERT-SELECT（带 path-scope）+ commit + 结果行。详设计 §7 R6。

R6 工作集（动码前逐一 re-grep 锚点）：
1. **`ConnectorExecuteAction.execute`**：按 `procedureOps.getExecutionMode(name)`（R1）分派——`DISTRIBUTED`→新 driver；`SINGLE_CALL`→现 `procedureOps.execute` 单调用路。**re-grep `ConnectorExecuteAction` 现结构 + :121 WHERE reject + :135 execute**。
2. **新 fe-core 分布式 rewrite driver**：① 连接器 `planRewrite`（R3）→ N `ConnectorRewriteGroup`；② 1 REWRITE `IcebergConnectorTransaction` 经 `PluginDrivenTransactionManager.begin`；③ `registerRewriteSourceFiles`（R5）= 全组 `getDataFilePaths()` 并集；④ 每组 INSERT-SELECT：scan 带 path-scope（THE CRUX stash 中立化）+ sink=`UnboundConnectorTableSink(rewrite=true)`（R4）+ 共享 txn 绑各组 session+`setTxnId`（仿 `RewriteGroupTask:179`）→ `ConnectorRewriteExecutor`（R4b）；⑤ commit → `getRewriteAddedDataFilesCount`（R5）+ 组统计 → 结果行（4 列：rewritten_data_files / added_data_files / rewritten_bytes / removed_delete_files）。
3. **THE CRUX stash 中立化**：`StatementContext` 的 `List<FileScanTask>` 信道改 `List<String>` raw-path + setter + `PluginDrivenScanNode` 读取下传（pin SPI 仿 `applyMvccSnapshotPin`）。**这是 R6 最重、最易踩 [INV-M1] 路径基准的部分**——RAW `dataFile.path().toString()` 两侧对齐（R2 已亲核）。
4. WHERE lowering = **R7**（不在 R6 做）；R6 先让无 WHERE 的 rewrite 跑通。
- fe-core 测试 Mockito（mock 连接器 `procedureOps`/`ConnectorTransaction`）。**fe-core build 慢**→后台跑读 surefire XML，验证用 clean。
- **dormant 边界**：R6 装上 driver 但 `ConnectorExecuteAction` 仅在表是 PluginDriven 时走（iceberg 翻闸后才是）→ pre-flip 仍 dormant（iceberg 不在 SPI_READY_TYPES）。R6 对 **mock 连接器** green；真 iceberg e2e = R8 flip rehearsal。

---

# ⚠️⚠️ 用户铁律：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经中立 SPI。**legacy 豁免类**（C4 dead 子树 + commit-bridge 旧清单 + `PhysicalIcebergTableSink`/`bindIcebergTableSink`）保留 iceberg 引用合法。**通用 fe-core 类**（C4 新增：`ConnectorRewriteExecutor`/`UnboundConnectorTableSink`/`LogicalConnectorTableSink`/`PhysicalConnectorTableSink` isRewrite + `RewriteTableCommand` 按 `PhysicalConnectorTableSink` 中立键路由 + R6 driver 按 `executionMode` 中立键路由）须全经中立 SPI，**不**加 instanceof Iceberg。

---

# 🔴🔴 开放 — P6.6 翻闸（C1+C2+C3 全闭，C4 进行中，C5 待）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 ✅ / **C4 进行中** / C5 FLIP 待）。

- **[C4 进行中 = 当前]** rewrite_data_files 翻闸就绪（Option B 全对等）。**R1/R2/R3/R4/R5 ✅（executionMode SPI / scan path-set 作用域 / planRewrite SPI / sink-bind+GATHER / transaction rewrite SPI gap）→ R6（fe-core 分布式 driver，整合 R1–R5 + THE CRUX stash 中立化）→ R7（WHERE lowering）→ R8（flip rehearsal，flip-gated）**。详设计 §7。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅** 隐藏列注入已闭。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049；commit-bridge=[DV-S2-rederive]。**C4 R1–R5 无新 DV**（dormant，行为零变更）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🟡 已登记 follow-up（非阻塞，勿在 C4 增量做）
- **[FU-connector-bind-visibility]** 通用连接器绑定 `selectConnectorSinkBindColumns` 用 `getBaseSchema(true)`（含隐藏列）→ 翻闸后**普通**（非 rewrite）连接器 INSERT 也会把隐藏 row-lineage 列纳入 bindColumns（iceberg 专用路径默认只取可见列）。R4 范围外。翻闸前若需对齐普通插入语义，须引入**中立**「是否 row-lineage/隐藏写入列」表能力（设计 D1 选 A：`PluginDrivenExternalTable.isRowLineageColumn(Column)` 默认 false，iceberg 插件表 override），**禁** `IcebergUtils.isIcebergRowLineageColumn`（铁律）。
- **[FU-rewrite-rescan-perf]** R5 `registerRewriteSourceFiles` 在 commit 前 re-scan `planFiles()` 解析路径→DataFile（O(表文件数)）。legacy 直接收规划器 DataFile 无此扫描。属中立 seam 代价（与 delete commit-time re-derive 同源），翻闸后若 rewrite 大表慢可优化（如连接器 planRewrite→commit 跨对象 stash 缓 DataFile，按 queryId 键，仿 rewritableDeleteStash）。
- **[FU-broker-write]** 连接器三 write builder 均未填 `setBrokerAddresses`；翻闸前若需 broker 写盘三 builder 一并补。
- **[FU-flip-e2e]** commit-bridge + C4 全程 pre-flip UT 锁，但真翻闸端到端（旧删不复活 / operation·row_id BE 解析 / OCC / rewrite 每组只扫自己文件 / GATHER 输出文件数 / register re-scan 路径匹配）**未跑**（CI-gated/flip-gated，勿谎称）。
- **[FU-getRowIdColumn]** `IcebergMergeCommand.getRowIdColumn(562)` 仍 `IcebergExternalTable`；翻闸/P6.7 核是否 dead。
- **[FU-step1-nullconn / order/remap/dualdelete/...]** 见 git log 历史 HANDOFF。

---

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）。**连接器模块**（`fe-connector-api`/`-iceberg`）build 快（~30s，无 fe-core），可前台；**fe-core -am 单类首次 ~2-7min**（本 session 实测 fe-core 编译 ~1:49–2:14）→`run_in_background:true` 再读 surefire XML。验证读 surefire **XML**（python ET）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **⚠️ checkstyle 全量 build 跑（除非 `-Dcheckstyle.skip=true`）**：import 同组无空行 + 组内**字母序**（本 session R5 踩：新加 `java.util.HashSet` 误置于 `java.util.HashMap` 前→`CustomImportOrderCheck` "Wrong lexicographical order"）。加 import 后用 `mvn checkstyle:check -pl :<mod>` 快验（连接器秒级）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（repo 根跑）。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core **Mockito**（`mockito-inline`+`mockStatic`；`Deencapsulation` 读写私有 final——R4a 用 `Deencapsulation.setField(sink,"isRewrite",true)`）。live-e2e CI-gated，勿谎称。
- **mutation-check（Rule 9/12）**：dormant 路必变异验真。范式 `scratchpad/mutate_r4a.py`/`mutate_r4b.py`/`mutate_r5.py`：cp 备份→「行为禁用形」`&& false`/翻枚举/归零→`mvn test -Dtest=<class> -Dcheckstyle.skip=true`→查 surefire fail+err>0（KILLED）→restore + `os.utime(f,None)`（避 stale .class）。**⚠️ exact-string 锚点须唯一**；**⚠️python3.6 无 `capture_output`/`text=`**→`stdout=subprocess.PIPE`。**⚠️ commit 前必核已 restore（无 `&& false`/无 `.bak`）**。
- **cwd**：跨 Bash 持久但**会被 harness 重置**（本 session 见 cwd 从 repo 根回到 `fe/`）→ 一律绝对路径，勿信相对前缀。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔真文件在 `fe/fe-connector/...`，勿提交〕·`plan-doc/reviews/P5-paimon-rereview3-*`）。
- **R1=`1bddf3426d6` / R2=`0a0d5b8de83` / R3=`a7c2732d984` / R4a=`a3d7210e892` / R4b=`12fe50ee88e` / R5=`e956f0edc45`**；HANDOFF 单独 commit。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含 commit-bridge + C4 R1–R5）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 ✅**。**P6.6：C1/C2/C3a/C3b-pre/C3b-core+commit-bridge 全闭 ✅ → C4 R1/R2/R3/R4/R5 ✅ / R6–R8 待 → C5 翻闸**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线 CLOSED（勿读）。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，本 session ~86G free）。**下个 session 起步先 `df -h /mnt/disk1`**；空间紧时 mutation 加 `-Dcheckstyle.skip=true`。多次增量 build 后 .class 可能 stale→误判；fe-core 验证用 clean。

# 🧠 给下一个 agent 的 meta

- **删除/parity/动码前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**；**HANDOFF/设计/RFC 的依赖名/行号/不变式/可达性可能过时或错** —— 动码前先 recon（grep+实证）再信文档。R5 recon 实证「统计仅 added-files-count 需 transaction」把设计的"统计 accessor 提升"收窄，省工。
- **铁律落地优先于照搬设计 finding**：R4 reader 初稿要在通用绑定路径塞 iceberg 专用 row-lineage 判断=违铁律；synthesis/code-first 独核推翻。参 finding 前先核铁律 + 现有 Doris 行为契约（Rule 7 surface conflict）。
- **既有 Doris 决策优先于新设计**：R5 rawPath→DataFile 解析照搬项目既有 commit-time re-derive 决策（[DV-S2-rederive]）+ in-file `commitReplaceTxn` re-scan 范式，不另起 stash（consult 既有决策，memory「consult-trino-before-spi-design」）。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（R4 recon `wf_93bb76a7-6ec` 5-reader+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **C4 逐子步**：R6→R8，每步 additive/dormant + green + mutation + commit + HANDOFF。**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**。本 session 完成 C4 R4（R4a+R4b）+ R5，在 R5 收官干净节点交接 R6（fe-core 重整合步，宜新 session 起）。
