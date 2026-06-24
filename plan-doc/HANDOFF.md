# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.5 = iceberg sys-table（`$`-后缀 metadata 表）**

**P6.4 = ✅ DONE**（T01–T09 全完成）。**下一 = P6.5 sys-table**，仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。

- **P6.5 内容**：iceberg `$`-后缀 metadata 表（`$snapshots`/`$history`/`$files`/`$manifests`/`$partitions`/…），**复用 P5-B4 live 机制**——连接器 `listSupportedSysTables`/`getSysTableHandle` + fe-core 通用 `PluginDrivenSysExternalTable`（见 [DV-023]/[D-039]），**非** RFC §10 原设计。**与 P6.4 procedure 不重叠**：`publish_changes`/`expire_snapshots` 的 `snapshots()` 迭代是 SDK `Table` API 内部，不走 Doris MetadataTable。
- **关键先读**（playbook §3.1）：P5 paimon sys-table 先例（连接器 `listSupportedSysTables`/`getSysTableHandle` 实现 + fe-core `PluginDrivenSysExternalTable`，grep 实证现状）+ legacy fe-core iceberg sys-table 路径（`datasource/iceberg/` 的 metadata-table 渲染，待 recon）+ `deviations-log.md` [DV-023] + `decisions-log.md` [D-039]。
- **节奏**：recon（grep + unzip 实证 legacy iceberg sys-table 形态 vs paimon 先例）→ 设计 + 用户签字 → 逐 task TDD（连接器 UT 无 Mockito）→ Tn8 parity 审计 + DV 中央登记 → Tn9 收口（= P6.5 DONE）。**全程不碰 `SPI_READY_TYPES`**。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.5 未做）。翻闸前必修下述三大 BLOCKER（**同需读/写路径共享 fe-core seam 的 holistic 修**）：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-045]**（写路径，T06 实证）= `rewrite_data_files` **执行半翻闸接线**（R-B，专门写路径 RFC）。① 事务半（`IcebergConnectorTransaction` REWRITE 变体）+ 规划半（`RewriteDataFilePlanner`）已建 dormant；**②③④ 执行半留 fe-core**。recon 证伪设计 §5 / D-062 R-A「从 pinned snapshot+WHERE 重规划」前提（连接器 scan SPI 无法表达 legacy bin-pack「分区内任意文件子集」→ over-scan **破坏 rewrite 正确性**；`FileScanTask` 侧信道 `RewriteGroupTask:117`→`IcebergScanNode.getFileScanTasksFromContext`〔def `:492`/caller `:929`〕翻闸后死；SPI 模块边界禁连接器 `RewriteDataGroup` 跨回 fe-core；multi-sink-per-txn 须重设计）。**用户裁 Option 1**：① 事务半已做（dormant），②③④ 推后。P6.6 接线点：per-group file-level scan-range 中立 SPI / `BindSink.bind(UnboundIcebergTableSink):1057`→改绑 `UnboundConnectorTableSink` / `RewriteGroupTask:175` + executor `instanceof PhysicalIcebergTableSink` / `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转→通用 `PluginDrivenTransactionManager` / DV-T08-factory-advertise canary + DV-T07-where WHERE-lowering（→DV-T05r-where 激活）。= **DV-041 写路径阻塞同族**。

**[pre-flip 行为偏差（T08 中央登记 = [DV-046] correctness-bearing + [DV-047] perf-cosmetic）]**：[DV-046] = auth-add Kerberos〔8 mutator 裹 `executeAuthenticated`，`context!=null`〕 + DV-T05r-where〔经 EXECUTE 双闸不可达 dormant〕；[DV-047] = cache-to-dispatch〔context!=null〕· session 参 · DV-T08-loadwrap · DV-T08-factory-advertise · DV-T06r-{scanpool,zone,rollback} · DV-T07-{where,name-order,exc-contract} · PARTITION(*) · null-row · per-conjunct filter。常见 WHERE/路径零差异、休眠至 P6.6 docker 真值闸。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure 三路 dormant + sys-table 未做，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态（T09 faithfulness 校正——旧 HANDOFF「所有 commit 均未 push」不实）**：`origin/catalog-spi-10-iceberg` = `bdc38b14810`（T06）；**T01/T02/T03 + arg-move `b045c9db45b` + T04/T05/T06 七 commit 已推**；**T07 `4c84ebf33f8` + T08 `34766150f17`（+ 本 session T09）待 push**（`git rev-list --count origin/catalog-spi-10-iceberg..HEAD`=2，T09 commit 后=3）。
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，iceberg 389/0/1 + fe-core 30/0）。**P6.4 = ✅ DONE**（T01–T09，iceberg **494/0/1** + fe-core `ConnectorExecuteActionTest` **14/0** + fe-connector-api 37/0 + fe-foundation arg 框架 40/0；T09 重跑实证）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.4-T09（收口/汇总设计 + faithfulness 对抗验证 + gate 重跑 ⇒ **P6.4 DONE**），1 commit，待 push

**T09**（纯文档 0 产品码，镜像 P6.3-T09）：
- **新 `designs/P6.4-T09-procedure-summary-design.md`**（7 节）：①架构总览（procedure dispatch flow）+ T01–T08 逐 task 索引（含 commit + 对抗 wf 结论）；②**procedure SPI 收口核对**——P6.4 净 **+1 SPI** = `ConnectorProcedureOps`〔`getSupportedProcedures`/`execute` + `ConnectorProcedureResult` + `Connector.getProcedureOps()` default-null〕，对比 P6.2「净 0 新 SPI」/ P6.3「SPI 统一收敛」；取最小 S-1 扁平 + arg 框架升 `fe-foundation` 共享而非长在 SPI 上；③DV-045/046/047 回指；④翻闸阻塞（= DV-045，DV-041 同族）；⑤验收门；⑥下一阶段 P6.5。
- **faithfulness 对抗验证 `wf_986bd3db-68b`**（7 cluster verifier refute-by-default + completeness critic，65 claim）= **3 真错 DISCREPANCY + 1 内部矛盾**（全已修，Rule 12）：① **「九 commit 待 push」实为二**（见上推送状态——同步修本 HANDOFF）；② **`BaseExecuteAction` 非字节不变**（arg-move `b045c9db45b` 改 `NamedArguments` import + try/catch rewrap +10/-3，**行为保留**〔error 型/串不变〕但非字节不变；唯 `ExecuteActionCommand`/`ExecuteAction` 真字节不变）；③ **stale `IcebergScanNode.getFileScanTasksFromContext:498`**→ def `:492` / rewrite-consuming caller `:929`（同步修 `deviations-log.md:108` DV-045；recon.md 三处 frozen 不动）；④ critic 抓 §3 内部矛盾「`NamedArguments` 校验留 fe-core」vs「升 fe-foundation」（修=移出 fe-core-resident 列）。critic 另证 44→47 DV / 475→494+1=20 gap-fill / 9-procedure 名集 / import-gate 禁 `common` / R-A→R-B 回退记述 **全 reconciled-OK**。
- **gate 核（重跑实证非凭 `@Test` 计数，Rule 12——critic 指 count 不能证绿）**：iceberg **不在** `SPI_READY_TYPES`；`tools/check-connector-imports.sh` exit 0；`git diff 52e25fb25e9..HEAD` = **0 BE / 0 gensrc / 0 CatalogFactory / 1 pom**〔`fe-connector-iceberg/pom.xml` 加 `fe-foundation` 依赖，arg-move `b045c9db45b`——**P6.4 累计非 0 pom**，T08 自身 0 pom；§6 如实记〕；**iceberg UT 重跑** `clean package`（cache off）`BUILD SUCCESS` surefire 39 类 **tests=494 failures=0 errors=0 skipped=1**；**fe-core `ConnectorExecuteActionTest` 重跑** `Tests run: 14, Failures: 0, Errors: 0, Skipped: 0`（补 T08 record 留的「运行中确认」）。
- **文档同步五步**：task 表（P6.4 phase 行 ❌→✅ + T09 行 ⬜→✅ + 实现记录）/ PROGRESS（header + §一/§二 board〔T08 仅改 header、board 遗留「T08–T09 未做」一并修〕 + progress-log〔T08 此前漏 bullet，本次 T08+T09 合并补〕）/ connectors/iceberg.md（当前状态 + 完成度 ~90% + 进度日志 T09 bullet）/ decisions-log（**无新 D**）/ deviations-log（**无新 DV**；唯 DV-045 stale `:498` 校正）。

---

# 🗺️ 代码脚手架（iceberg，P6.4 终态）

- **procedure SPI（`fe-connector-api`）**：`procedure/{ConnectorProcedureOps, ConnectorProcedureResult}`〔S-1 扁平〕 + `Connector.getProcedureOps()` default-null（`Connector.java:59-61`）。`handle/WriteOperation` 含 `REWRITE`（`:48`，净 0 新事务 verb）。
- **连接器（`fe-connector-iceberg`，`connector.iceberg`）**：`IcebergProcedureOps`〔**直属 `connector.iceberg`，非 `procedure/` 子包**；dispatch + `runInAuthScope`〔`context!=null` 才 executeAuthenticated + post-commit invalidate；`context==null` 离线测跳过〕 + loadTable-fail 串 "Failed to load iceberg table"〕；`IcebergConnector.getProcedureOps()`〔`:204`，镜像 `getWritePlanProvider`〕；`action/{BaseIcebergAction, IcebergExecuteActionFactory, 8 pure-SDK actions〔Iceberg{RollbackToSnapshot,RollbackToTimestamp,SetCurrentSnapshot,CherrypickSnapshot,FastForward,ExpireSnapshots,PublishChanges,RewriteManifests}Action〕, RewriteManifestExecutor}`；`rewrite/{RewriteDataFilePlanner〔规划半，T05〕, RewriteDataGroup, RewriteResult}`；`IcebergConnectorTransaction`〔`WriteOperation.REWRITE` 变体：`commitRewriteTxn`/`updateRewriteFiles`/`filesToDelete`·`filesToAdd`·`startingSnapshotId`，dormant，T06 ①〕。
- **arg 框架（`fe-foundation`）**：`org.apache.doris.foundation.util.{NamedArguments, ArgumentParsers, ArgumentParser}`（引擎 + 连接器**共享一份**——import-gate 禁连接器 import `org.apache.doris.common`；`validate` 抛 unchecked `IllegalArgumentException` 两侧各自 re-wrap）。
- **dispatch（T07，fe-core `nereids/.../commands/execute/`）**：新 adapter `ConnectorExecuteAction implements ExecuteAction`〔PluginDriven 派发 + `wrapResult`〔空 schema/rows→null + 单行 `checkState`〕 + `DorisConnectorException`→plain `UserException` re-wrap + WHERE 拒 `DdlException`〕；`ExecuteActionFactory` `createAction:61`/`getSupportedActions:87` 加 PluginDriven 分支，保 legacy `IcebergExternalTable` 分支。`ExecuteActionCommand`/`ExecuteAction` 字节不变；`BaseExecuteAction` 仅随 arg-move 改 import+rewrap（行为保留非字节不变）。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/action/`（`BaseIcebergAction`+9 action+`IcebergExecuteActionFactory`）+ `rewrite/`（6 文件，含 `RewriteDataFileExecutor`/`RewriteGroupTask`/`RewriteTableCommand`/`IcebergRewriteExecutor`，= DV-045 ②③④）。**现 iceberg EXECUTE/rewrite 仍走 legacy**（连接器 procedure/rewrite dormant：`IcebergExternalTable` 非 PluginDriven，直到 P6.6）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`DependencyResolutionException`/假错**）；offline 加 `-o`；procedure-SPI art = `fe-connector-api`、连接器 = `fe-connector-iceberg`、dispatch = `fe-core`、arg 框架 = `fe-foundation`。checkstyle 在 `validate` phase 跑；`-q` 抑制 BUILD SUCCESS 行；验证读 surefire **XML**（`<testsuite tests=.. failures=..>`）。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现。**⚠️ 单方法跑会留单测 XML 污染聚合计数**——全量验证前 `rm -f target/surefire-reports/TEST-*.xml` 再 clean 跑。**⚠️ gensrc/version-build 阶段会喷 ANTLR `mismatched input '->'` + `error importing function definition for 'which'` 噪声，非 javac 错、不影响 exit 0**。
- **⚠️ 勿并发跑两个 `-am clean` 构建**（T09 实证：iceberg + fe-core 两 `-am clean` 同时跑会争抢共享上游 `fe-foundation` 的 target → 互相 corrupt → BUILD FAILURE 假错）。**顺序跑**。**⚠️ 后台 Bash 勿用内层 `( mvn ... ) &`**（双重 background→wrapper 立返 exit 0 假信号、maven 被 reap）；直接 `run_in_background: true` 跑裸 mvn。**⚠️ 勿 `| tail -N` 管道**（丢根因 + `PIPESTATUS[0]` 才是 maven 真 exit，wrapper exit 非）。
- **iceberg 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）。**fe-core 用 `test` 即可**（含 `-am`；`-am` 编译上游约 6–7min，给足超时或后台跑）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`）。测试：连接器侧无 Mockito（fail-loud fake + InMemoryCatalog；fixture `RecordingIcebergCatalogOps`/`RecordingConnectorContext`/`action/ActionTestTables`）；fe-core dispatch 侧用 Mockito（含 `mockito-inline` mockStatic；priv 测须 stub `ctx.getState()`→mock `QueryState` 否则 NPE）。live-e2e CI-gated（docker），勿谎称跑过。
- **mutation-check（验证测试 pin 了意图，Rule 9/12）**：dormant 写/派发路里「测试是否真 pin 某行」非显然——临时删该行、单跑该测、确认转红、再恢复。T06/T07/T08 各实证一坑。
- cwd 跨 Bash 持久；一律绝对路径（heredoc `cd` 会改 cwd → 后续相对路径失效）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。
- message `[doc](catalog) P6.4 iceberg: T09 — <subj>`（纯文档用 `[doc]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。**注意 T01–T06 已推 origin**（rebase/force-push 须谨慎；本 session T09 + 未推的 T07/T08 一并 push）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。T09 实证：旧 HANDOFF「所有 commit 均未 push」不实（7 已推）；汇总设计初稿 `BaseExecuteAction byte-parity`/`:498`/`procedure 子包` 三处过时，对抗验证抓出。
- **大文件用 subagent 总结**（playbook §3.1）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md`〔header + §一/§二 board + progress-log，**board 易遗漏**〕 + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**。DV 中央登记延后到 Tn8 批量。
- **faithfulness 对抗 workflow 范式**（T09 = `wf_986bd3db-68b`，7 cluster verifier refute-by-default + completeness critic）：每 claim 须引源文件 file:line 或判 DISCREPANCY；critic 查「漏验 / 不可证伪 / 内部矛盾 / 未配对的数」。**收口汇总设计若引行号/commit/UT 计数必跑此 wf**（T09 抓 3 真错+1 矛盾）；**UT 计数 claim 必重跑 surefire 实证**（`@Test` 计数不能证绿，Rule 12）。**绝不 overclaim**。
