# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.4-T08 = parity-UT 审计 + gap-fill + DV 中央登记**

**P6.4a（T01–T04）+ P6.4b（T05 规划半 + T06 事务半①）+ T07（dispatch rewire）已完成**（均未 push）。**下一 = T08 = 收口审计**：把 T04–T07 累积的前向引用 DV **批量中央登记**进 `deviations-log.md`（同 P6.3-T08 / P6.2-T11 体例）+ 补 parity gap-fill UT + 核 gate。仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。

- **T08 内容**（设计 [`designs/P6.4-T01-procedure-spi-design.md`](./tasks/designs/P6.4-T01-procedure-spi-design.md) §8 测试策略 / §9 T08 行 / §10 deviation 预登记 + task 表 §P6.4-T08）：
  1. **DV 中央登记**（进 `deviations-log.md`，分层镜像 P6.2-T11 DV-038/039/040 / P6.3-T08 DV-041..044）——以下 DV 现仅在 code 注释 + task record 前向引用，T08 须落中央条目：
     - **(T04)** auth 补〔8 snapshot mutator commit 现裹 `executeAuthenticated`，legacy 缺〕/ cache 失效搬 dispatch 级〔`context.getMetaInvalidator()`〕+ 短路时多一次幂等失效 / `executeAction` 加 `ConnectorSession` 参〔会话 TZ〕。
     - **(T05)** `DV-T05r-where` = `rewrite_data_files` WHERE 走 conflict-mode 通路（用户签字 Option A）⇒ 不可转节点静默丢（legacy fail-loud 抛）= rewrite 变宽/极端全表 + conflict-matrix 收窄。
     - **(T06)** `DV-T06r-rb`（🔴 翻闸阻塞，见下）/ `DV-T06r-scanpool`〔丢 `scanManifestsWith` perf-only〕/ `DV-T06r-zone`〔rewrite-added 分区值 session-TZ 解析，既有 DV-T04-f 路新触发，benign〕/ `DV-T06r-rollback`〔`rollback()` 不清 rewrite 列表，单 txn/语句生命周期中性〕。
     - **(T07)** `DV-T07-where` = 连接器 EXECUTE 派发 WHERE 拒（fail-loud，lowering 推后 R-B；唯一吃 WHERE 的 rewrite 不走此派发，8 pure-SDK 本就拒）/ `DV-T07-name-order` = 未知 procedure 名校验时序 priv-first（legacy createAction-time 抛=priv 前；**有意发散**：priv-first 更安全，不向无权用户泄漏 procedure 存在性，设计「引擎保 priv／连接器拥 body 含名派发」支持）/ `DV-T07-exc-contract` = 连接器侧非-`DorisConnectorException` 逃逸的 byte-parity 边界（连接器契约=arg 失败已 re-wrap `DorisConnectorException`；单行 `IllegalStateException` 有意逃逸=与 legacy `BaseExecuteAction` 同）。
  2. **parity gap-fill UT**（连接器侧无 Mockito，fe-core 侧 Mockito）：byte-parity（每 procedure result schema 列数·列名·值 + error 串字节 vs legacy 期望，非只断类名）；单行不变式（每 procedure 列数==行 size，如 expire 6×BIGINT）；auth 加非丢；bug-for-bug 保留（publish STRING+`"null"`、fast_forward 无 guard+反序、rewrite_data_files `rewritten_bytes_count` INT 声明 long 求和溢出、死 `output-spec-id` 参）。
  3. **mutation-check**（Rule 9/12）：dormant 写/派发路里「测试是否真 pin 某行」非显然——临时删行、单跑、确认转红、再恢复（T06/T07 实证两次此法各抓一坑：T06 OCC 测不 pin `validateFromSnapshot`、T07 priv 测旧断言掩盖 mock NPE）。
  4. **gate 核**：iceberg 仍不在 `SPI_READY_TYPES`；checkstyle 0；import-gate 0；0 BE 改。
- **关键先读**（playbook §3.1）：`deviations-log.md`（DV 体例 + DV-041 分层）+ task 表 §P6.4 全 T0x 实现记录（DV 前向引用清单）+ 连接器 `connector.iceberg.{action/*, rewrite/*, IcebergProcedureOps, IcebergConnectorTransaction}` + fe-core `nereids/.../commands/execute/{ConnectorExecuteAction, ExecuteActionFactory}` + 各 task 的 faithfulness wf 结论。
- **节奏**（playbook §5.1 / 7.3）：T08 主要是文档（DV 中央登记）+ 测试 gap-fill（如有真缺口），**0 或极少产品码**。byte-parity 审计可用对抗 workflow（每 gap 独立 refute-by-default verify + completeness critic，同 P6.3-T08 `wf_c1067212`）。T09 = 收口/汇总设计 + HANDOFF 覆盖式。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.4 进行中 / P6.5 未做）。翻闸前必修下述 BLOCKER：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-T06r-rb] 🔴（写路径，T06 实证）= `rewrite_data_files` 执行半翻闸接线（R-B，推后专门写路径 RFC）**。recon 证伪设计 §5 / D-062 R-A 前提「连接器从 pinned snapshot+WHERE 重规划」：① 连接器 scan SPI 只能按 snapshot/谓词/分区收窄，**无法表达 legacy bin-pack 的「分区内任意文件子集」** → 重规划 over-scan → 破坏 rewrite 正确性；② `FileScanTask` 侧信道（`RewriteGroupTask:117`→`IcebergScanNode.getFileScanTasksFromContext:498`）翻闸后走 `PluginDrivenScanNode` 端到端死；③ SPI 模块边界：`fe-core` 只依赖 `fe-connector-api/-spi`，连接器 `RewriteDataGroup`（裹 iceberg `FileScanTask`/`DataFile`）不能跨进 fe-core；④ multi-sink-per-txn 生命周期须重设计；⑤ bind：`BindSink.bind(UnboundIcebergTableSink):1057` 对 PluginDriven 抛错 → 须改绑 `UnboundConnectorTableSink`→`visitPhysicalConnectorTableSink`；`RewriteGroupTask:175` `instanceof IcebergRewriteExecutor` + executor 选 `instanceof PhysicalIcebergTableSink`；`RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转 → 经通用 `PluginDrivenTransactionManager` 取连接器 REWRITE txn〔T06 ① 已建〕。**用户裁 Option 1**：T06 只做 ① 事务半（dormant，已做），②③④ 推后。

**[P6.4 翻闸阻塞预登记（dispatch 侧）]**：T07 dispatch（PluginDriven→`getProcedureOps()`）须在 P6.6 与 legacy 分支切换同步（dormant 直到翻闸；pre-flip iceberg 是 `IcebergExternalTable` 非 PluginDriven → 走 legacy `IcebergExecuteActionFactory`）。

**[pre-flip 行为偏差（T08 批量中央登记 DV，非翻闸阻塞）]**：(T04) auth 补 / cache 搬 dispatch / 短路多失效 / `executeAction` 加参；**(T05) DV-T05r-where**；**(T06) DV-T06r-{scanpool,zone,rollback}**；**(T07) DV-T07-{where,name-order,exc-contract}**。常见 WHERE/路径零差异、休眠至 P6.6。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 未齐，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，fe-connector-iceberg UT 389/0/1 + fe-core 30/0）。
- **P6.4 = 🟢 进行中**：T01 ✅（recon+设计+三签字 [D-062]）；T02 ✅（`ConnectorProcedureOps` SPI 骨架）；T03 ✅（base+factory+dispatch 骨架 + arg 框架移 `fe-foundation`）；T04 ✅（8 pure-SDK 体 + `RewriteManifestExecutor`，iceberg 444）；T05 ✅（`rewrite_data_files` 规划半 → `connector.iceberg.rewrite` 3 类，iceberg 467）；T06 🟢（用户裁 Option 1 = ① 连接器事务 `WriteOperation.REWRITE` 变体 dormant，iceberg 475/0/1 + api 37/0；②③④ R-B 推后 DV-T06r-rb）；**T07 ✅（dispatch rewire，纯 fe-core·dormant）= 新 adapter `ConnectorExecuteAction` + `ExecuteActionFactory` PluginDriven 分支〔保 legacy〕；fe-core `ConnectorExecuteActionTest` 13/0 + `NereidsParserTest` 73/0；faithfulness wf 0 finding，critic→2 修+2 DV+2 note；0 连接器/BE/pom 改**；T08–T09 未做。**八 commit 待 push**（T01/T02/T03 + arg-framework fe-foundation 化 + T04 + T05 + T06 + T07）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.4-T07（dispatch rewire），1 commit，未 push

**T07**（dispatch rewire：EXECUTE → `getProcedureOps()`，**纯 fe-core**·dormant）：
- **范围**：grep 实证 EXECUTE 派发路反向 `instanceof IcebergExternalTable` **仅** `ExecuteActionFactory` 两处〔`createAction:56`/`getSupportedActions:77`〕，其余 instanceof 全在 INSERT/DELETE/MERGE/rewrite **写**路径（P6.3/T06 域）。**0 连接器 / 0 BE / 0 pom / 0 `CatalogFactory` 改**——纯 fe-core（1 改 `ExecuteActionFactory` + 1 新 `ConnectorExecuteAction` + 1 新测）。
- **设计落点 = adapter（非 inline）**：`createAction` 返回类型 `ExecuteAction` vs 连接器 `getProcedureOps().execute()` 返回 `ConnectorProcedureResult` = 阻抗不匹配 ⇒ 新 fe-core adapter `ConnectorExecuteAction implements ExecuteAction`，`createAction` 的 PluginDriven 分支返它，**`ExecuteActionCommand.run()` 100% 不变**（legacy 路结构性 byte-parity）；adapter 经正常 run() 流复用 `logRefreshTable`+`sendResultSet`（editlog 单一来源，flip-safe）。
- **engine/connector 分工（D-062 §2）**：engine 保 = `validate()` 的 `PrivPredicate.ALTER`（逐字复刻 `BaseExecuteAction.validate` priv 块，**不**含 namedArguments）+ 单行 `CommonResultSet` 包装（`wrapResult`：`ConnectorColumnConverter.convertColumns` + 宽度 `checkState` + 空 schema **或空 rows**→null）+ run() 的 logRefreshTable。connector 保 = arg+body+commit(auth)+cache。priv 严格在任何连接器交互前。
- **异常 re-wrap（byte-parity）**：连接器 `DorisConnectorException`（unchecked）→ adapter catch → `new UserException(msg, e)`（**plain `UserException`，非 `DdlException`**——legacy action body 抛的就是 plain `UserException`，`getMessage()` 同 formatting）→ run() `catch(UserException)` 加 "Failed to execute action:" 前缀字节同；table-not-found→`AnalysisException`（镜像 `visitPhysicalConnectorTableSink:664`）；getProcedureOps null→`DdlException`。
- **dispatch 链**（镜像 `visitPhysicalConnectorTableSink:636-667`）：`catalog=table.getCatalog()` → `getProcedureOps()`（null→throw）→ WHERE 拒 → `buildConnectorSession` → `getMetadata` → `getTableHandle(session, remoteDb, remoteName)`（orElseThrow）→ `execute(session, handle, name, props, null, partitionNames)`。partition 透传。
- **WHERE = 拒（DV-T07-where，fail-loud）**：lowering 推后 R-B；HANDOFF 预授权「暂置空/拒」，选拒（Rule 12）。
- **TDD 13 测**（RED：缺类编译失败 + `DdlException.getMessage` 加 errCode→改 plain `UserException`+`getDetailMessage`）→ GREEN。
- **faithfulness 对抗 `wf_c8256474-c32`**（5 finder：engine/connector-split·legacy-unchanged·exception-parity·result-wrapping·dispatch-completeness + refute-by-default skeptic + completeness critic）= **5 finder 全 0 finding**。critic 6 类（自评全非 dormant blocker）→ **2 当场修**〔① 空-rows→null 形状 faithfulness（连接器 null-row 编码 `(schema,emptyRows)`，legacy null-row→null ⇒ `wrapResult` 加 `getRows().isEmpty()→null`+测）；② priv 测断言 `Exception`→`AnalysisException`+消息——**收紧后实测捕获被旧断言掩盖的 mock NPE**（`ConnectContext.getState()` 未 stub，`ErrorReport.reportCommon` 触发）→ 修测 mock，正是 critic Rule-9 价值〕、**2 DV→T08**〔DV-T07-name-order / DV-T07-exc-contract〕、**2 note 不改**〔`resolveConnectorTableHandle` bypass=有意镜像写路径〔seam protected + sys-table-scan-专用〕/flip-safety grep-gate 非 UT=全 P6 series 惯例〕。
- **验证**（`-Dmaven.build.cache.enabled=false` + clean surefire + 核对 mtime）：fe-core `ConnectorExecuteActionTest` **13/0/0/0** + `NereidsParserTest` **73/0/0/0**（唯一 `ExecuteActionFactory` 引用者无回归）；`BUILD SUCCESS`（exit 0，含 checkstyle validate phase）；checkstyle 0；`check-connector-imports.sh` exit 0；iceberg 仍**不在** `SPI_READY_TYPES`；**0 连接器 / 0 BE / 0 pom / 0 `CatalogFactory` 改**。
- **文档同步**：task 表（T07 行 ✅ + 实现记录）/ PROGRESS（header + P6 行 + 连接器表 475+fe-core 13 + ~85% + dated 日志 bullet）/ connectors/iceberg.md（当前状态 + 完成度 ~85% + T07 progress-log）/ decisions-log（**无新 D**）/ deviations-log（**无新中央 DV**，DV-T07-{where,name-order,exc-contract} 前向引用 → T08 批量登记，同 DV-T05r/T06r 体例）。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/.../connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`〔scan + conflict 双模式〕/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；写 `IcebergWritePlanProvider`/`IcebergConnectorTransaction`〔含 `WriteOperation.REWRITE` 变体，dormant〕/`IcebergWriterHelper`/`IcebergWriteContext`；procedure **`IcebergProcedureOps`**〔dispatch + `runInAuthScope` + post-commit invalidate + session 透 `executeAction`〕 + `action/{BaseIcebergAction, IcebergExecuteActionFactory, 8 pure-SDK actions, RewriteManifestExecutor}`；rewrite `rewrite/{RewriteDataFilePlanner〔规划半，T05〕, RewriteDataGroup, RewriteResult}`。**arg 框架** `NamedArguments`/`ArgumentParsers`/`ArgumentParser` 在 **`fe-foundation`**（`org.apache.doris.foundation.util`）。
- **procedure SPI（已建，`fe-connector-api`）**：`procedure/{ConnectorProcedureOps, ConnectorProcedureResult}` + `Connector.getProcedureOps()` default-null。`handle/WriteOperation` 含 REWRITE。
- **dispatch（T07 已改，fe-core）**：`nereids/.../commands/execute/`：**新 `ConnectorExecuteAction implements ExecuteAction`**〔PluginDriven 派发 adapter：validate=priv / execute=catalog→connector→getProcedureOps→session→metadata→handle→execute + wrapResult〔ConnectorColumnConverter + 宽度 checkState + 空 schema/rows→null〕 + DorisConnectorException→plain UserException re-wrap + WHERE 拒〕；`ExecuteActionFactory`〔`createAction:61` + `getSupportedActions:87` 加 PluginDriven 分支，保 legacy `IcebergExternalTable`〕。`ExecuteActionCommand`/`ExecuteAction`/`BaseExecuteAction` **不变**（legacy byte-parity）。
- **rewrite ②③④（R-B，专门写路径 RFC，非 T07/T08）**：fe-core `RewriteDataFileExecutor`/`RewriteGroupTask`/`RewriteTableCommand`/`IcebergRewriteExecutor` 留 fe-core；翻闸接线见「翻闸阻塞 DV-T06r-rb」。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/action/`（`BaseIcebergAction`+9 action+`IcebergExecuteActionFactory`）+ `rewrite/`（6 文件）+ 通用 dispatch `nereids/.../commands/{ExecuteActionCommand,execute/{ExecuteAction,BaseExecuteAction,ExecuteActionFactory}}`。**现 iceberg EXECUTE/rewrite 仍走 legacy**（连接器 procedure/rewrite dormant：`IcebergExternalTable` 非 PluginDriven，直到 P6.6）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`DependencyResolutionException`/假错**）；offline 加 `-o`；procedure-SPI art = `fe-connector-api`、连接器 = `fe-connector-iceberg`、dispatch = `fe-core`。checkstyle 在 `validate` phase（编译前）跑；`-q` 抑制 BUILD SUCCESS 行，**exit 0 = 成功**（含 checkstyle+surefire）。验证读 surefire **XML**（`<testsuite tests=.. failures=..>`）。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现。**⚠️ 单方法跑会留单测 XML 污染聚合计数**——全量验证前 `rm -f target/surefire-reports/TEST-*.xml` 再 clean 跑。**⚠️ gensrc/version-build 阶段会喷 ANTLR `mismatched input '->'` 噪声（`which` 函数导入错附带），非 javac 错、不影响 exit 0**。
- **iceberg/paimon 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）。**fe-core 用 `test` 即可**（含 `-am`；全量 `-am` 编译上游约 6–7min，给足超时或后台跑）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`）。测试：连接器侧偏好无 Mockito（fail-loud fake + InMemoryCatalog；共享 fixture `RecordingIcebergCatalogOps`/`RecordingConnectorContext`/`action/ActionTestTables`）；fe-core dispatch 侧 UT 用 Mockito（含 `mockito-inline` mockStatic Env/ConnectContext；PluginDriven 表用 `TestablePluginCatalog`-式 `extends PluginDrivenExternalCatalog` override `getConnector`/`buildConnectorSession`；priv 测须 stub `ctx.getState()`→mock `QueryState`，否则 `ErrorReport.reportCommon` NPE）。live-e2e CI-gated（docker），勿谎称跑过。
- **mutation-check（验证测试 pin 了意图，Rule 9/12）**：dormant 写/派发路里「测试是否真 pin 某行」非显然——临时删该行、单跑该测、确认转红、再恢复。T06/T07 各实证一坑（OCC 测不 pin `validateFromSnapshot` / priv 测旧 `Exception.class` 断言掩盖 mock NPE）。
- cwd 跨 Bash 持久；一律绝对路径（Bash heredoc `cd` 会改 cwd → 后续相对路径失效）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。
- message `[refactor](catalog) P6.4 iceberg: <subj>`（纯文档用 `[doc]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。T07 实证：`IcebergProcedureOps` 在 `connector/iceberg/`（非 HANDOFF 说的 `connector/iceberg/procedure/`）；HANDOFF ① 说 createAction「直调 getProcedureOps().execute()」实际须经 adapter（返回类型不匹配）。
- **大文件用 subagent 总结**（playbook §3.1）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md` + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**更新。**DV 中央登记延后到 Tn8 批量**（前向引用 DV-Tnnr-* / DV-T0n-* 在 code + task record）。
- **faithfulness 对抗 workflow 范式**（T07 = `wf_c8256474-c32`，5 finder + refute-by-default skeptic + completeness critic）：每发现独立 skeptic（默认 isReal=false，须引双文件原文），critic 查漏报类别。**T07 经验**：finder 0 finding 时 critic 仍可能给真价值——critic 的「测试能否失败」质疑用 **tightening + 实跑** 验证（T07 收紧 priv 断言后实测捕获被旧 `assertThrows(Exception.class)` 掩盖的 mock NPE）；critic 的 latent-shape 质疑（空-rows）当场修+测。**绝不 overclaim**（Rule 12）。
