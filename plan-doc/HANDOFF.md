# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.4-T09 = 收口/汇总设计 + gate 核 + HANDOFF 覆盖式 ⇒ P6.4 DONE**

**P6.4-T01~T08 全部完成**（均未 push；九 commit 待 push）。**下一 = T09 = P6.4 收口**（同 P6.3-T09 / P6.2-T11 体例，**纯文档·0 产品码**）。仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。

- **T09 内容**：
  1. **写汇总设计** `designs/P6.4-T09-procedure-summary-design.md`（镜像 P6.3-T09 七节）：架构总览 + T01–T08 逐 task 索引 + **procedure SPI 收口核对**〔P6.4 净 **+1 SPI** = `ConnectorProcedureOps`〔`getSupportedProcedures`/`execute` + `ConnectorProcedureResult` + `Connector.getProcedureOps()` default-null〕，对比 P6.2「净 0 新 SPI」/ P6.3「SPI 统一」；arg 框架升 `fe-foundation` 共享〕 + deviation 回指 **DV-045/046/047** + 翻闸阻塞汇总〔DV-045 = rewrite 执行半 R-B = DV-041 写路径同族〕 + 验收门 + 下一阶段（P6.5）。
  2. **faithfulness 对抗验证**（可选，同 P6.3-T09 `wf_9234a18e`）：cluster verifier refute-by-default + completeness critic 核汇总设计的事实/行号（汇总设计若引行号须 recon）。
  3. **gate 核**：iceberg 仍不在 `SPI_READY_TYPES`（= {jdbc,es,trino-connector,max_compute,paimon}）；checkstyle 0；import-gate 0；0 BE。
  4. **HANDOFF 覆盖式** ⇒ **P6.4 DONE**。
- **下一阶段（P6.4 DONE 后）= P6.5 sys-table**：iceberg `$`-后缀 metadata 表（`$snapshots`/`$history`/`$files`/`$manifests`/…），复用 P5-B4 live 机制（连接器 `listSupportedSysTables`/`getSysTableHandle` + fe-core 通用 `PluginDrivenSysExternalTable`；见 [DV-023]/[D-039]），**非** RFC §10 原设计。注意与 P6.4 procedure 的 `snapshots()` 迭代不重叠（那是 SDK `Table` API 内部，不走 MetadataTable）。
- **关键先读**（playbook §3.1）：`deviations-log.md` DV-045/046/047〔T08 已登记〕+ task 表 §P6.4 全 T0x 实现记录（含 T08）+ `designs/P6.3-T09-iceberg-write-summary-design.md`〔汇总设计模板〕+ 连接器 `connector.iceberg.{action/*, rewrite/*, IcebergProcedureOps, IcebergConnectorTransaction}` + fe-core `nereids/.../commands/execute/{ConnectorExecuteAction, ExecuteActionFactory}` + procedure SPI `fe-connector-api/.../procedure/`。
- **节奏**（playbook §5.1 / 7.3）：T09 = 收口/汇总设计 + gate 核 + HANDOFF 覆盖式，纯文档（0 产品码），= **P6.4 DONE**。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.4 进行中 / P6.5 未做）。翻闸前必修下述 BLOCKER：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-045]（= DV-T06r-rb，T08 已中央登记）🔴（写路径，T06 实证）= `rewrite_data_files` 执行半翻闸接线（R-B，推后专门写路径 RFC）**。recon 证伪设计 §5 / D-062 R-A 前提「连接器从 pinned snapshot+WHERE 重规划」：① 连接器 scan SPI 只能按 snapshot/谓词/分区收窄，**无法表达 legacy bin-pack 的「分区内任意文件子集」** → 重规划 over-scan → 破坏 rewrite 正确性；② `FileScanTask` 侧信道（`RewriteGroupTask:117`→`IcebergScanNode.getFileScanTasksFromContext:498`）翻闸后走 `PluginDrivenScanNode` 端到端死；③ SPI 模块边界：`fe-core` 只依赖 `fe-connector-api/-spi`，连接器 `RewriteDataGroup`（裹 iceberg `FileScanTask`/`DataFile`）不能跨进 fe-core；④ multi-sink-per-txn 生命周期须重设计；⑤ bind：`BindSink.bind(UnboundIcebergTableSink):1057` 对 PluginDriven 抛错 → 须改绑 `UnboundConnectorTableSink`→`visitPhysicalConnectorTableSink`；`RewriteGroupTask:175` `instanceof IcebergRewriteExecutor` + executor 选 `instanceof PhysicalIcebergTableSink`；`RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转 → 经通用 `PluginDrivenTransactionManager` 取连接器 REWRITE txn〔T06 ① 已建〕。**用户裁 Option 1**：T06 只做 ① 事务半（dormant，已做），②③④ 推后。

**[P6.4 翻闸阻塞预登记（dispatch 侧）]**：T07 dispatch（PluginDriven→`getProcedureOps()`）须在 P6.6 与 legacy 分支切换同步（dormant 直到翻闸；pre-flip iceberg 是 `IcebergExternalTable` 非 PluginDriven → 走 legacy `IcebergExecuteActionFactory`）。

**[pre-flip 行为偏差（T08 已中央登记 = [DV-046] correctness-bearing + [DV-047] perf-cosmetic）]**：[DV-046] = auth 补〔仅 context!=null〕+ DV-T05r-where〔经 EXECUTE 双闸不可达 dormant〕；[DV-047] = cache 搬 dispatch〔仅 context!=null〕+ 短路多失效 + `executeAction` 加参 + DV-T08-{loadwrap,factory-advertise} + DV-T06r-{scanpool,zone,rollback} + DV-T07-{where,name-order,exc-contract} + PARTITION(*)/null-row/per-conjunct-filter。常见 WHERE/路径零差异、休眠至 P6.6。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 未齐，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，fe-connector-iceberg UT 389/0/1 + fe-core 30/0）。
- **P6.4 = 🟢 进行中**：T01 ✅（recon+设计+三签字 [D-062]）；T02 ✅（`ConnectorProcedureOps` SPI 骨架）；T03 ✅（base+factory+dispatch 骨架 + arg 框架移 `fe-foundation`）；T04 ✅（8 pure-SDK 体 + `RewriteManifestExecutor`，iceberg 444）；T05 ✅（`rewrite_data_files` 规划半 → `connector.iceberg.rewrite` 3 类，iceberg 467）；T06 🟢（用户裁 Option 1 = ① 连接器事务 `WriteOperation.REWRITE` 变体 dormant，iceberg 475；②③④ R-B 推后 = **DV-045**）；T07 ✅（dispatch rewire，纯 fe-core·dormant = 新 adapter `ConnectorExecuteAction` + `ExecuteActionFactory` PluginDriven 分支）；**T08 ✅（parity-UT 审计 + gap-fill + DV 中央登记）= 对抗 byte-parity 审计 wf〔12 finder 28 confirmed + critic 8 跨切；前向 DV 全 accurate〕→ 20 gap-fill UT〔连接器 494/0/1 + fe-core 14/0〕 + DV-045/046/047 登记〔44→47〕；2 测模型坑实证修；mutation-check tx-1 &&→\|\| 实证转红；0 产品码〔唯 1 行注释〕**；T09 未做。**九 commit 待 push**（T01/T02/T03 + arg-framework fe-foundation 化 + T04 + T05 + T06 + T07 + T08）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.4-T08（parity-UT 审计 + gap-fill + DV 中央登记），1 commit，未 push

**T08**（**纯 0 产品码**——唯 1 行 `IcebergProcedureOps` 澄清注释〔未读字段，防审计误判 missed-wiring〕 + 20 gap-fill UT + DV-045/046/047 中央登记）：
- **对抗 byte-parity 审计 wf**（12 area finder：8 procedure + rewrite-planner〔T05〕+ transaction-REWRITE〔T06〕+ dispatch-adapter〔T07〕+ infra〔base/ops〕；每 finding 独立 refute-by-default skeptic + completeness critic）= **28 confirmed/partial utGap + 2 newDeviation + 6 refuted**；**所有前向引用 DV 审计 accurate=True**。6 refuted 全对（单行不变式 pin 在 base `BaseIcebergActionTest:184` 非 per-action；IN conflict-mode pin 在 `IcebergPredicateConverterConflictModeTest`；per-conjunct filter 结果等价）。**critic 8 跨切 layering 漏报**（finder 各看一片漏的接缝不变式）→ 全采纳：factory createAction/getSupportedActions 9-vs-8 不一致〔→DV-T08-factory-advertise + canary 测〕/ DV-T05r-where 经 EXECUTE 双闸不可达〔→DV-046 cross-link〕/ NULLABILITY 无 end-to-end round-trip 测〔→fe-core 双极性测〕/ 新 "Failed to load iceberg table" 串〔→DV-T08-loadwrap + 测〕/ **auth-add+cache 仅 `context!=null`〔DV 措辞修=非「无条件」〕** / null-row 编码 / `PARTITION(*)` 不对称 / captured-once。
- **20 gap-fill UT**（连接器无 Mockito + `ActionTestTables`/`Recording*` fixture；fe-core Mockito）：① schema 完整性（多数 action 测只断 `get(0)`→补全列名·类型·nullability·宽度 vs legacy 字节）；② error 串字节（expire 负 older_than / publish cherrypick 失败前缀〔真 ancestor-cherrypick `CherrypickAncestorCommitException`〕 / rewrite_manifests **双-wrap 两层**〔`wrapsCurrentSnapshotFailure` 外层 + `executorWrapsFailureWithInnerPrefix` 内层——action 自身先探 `currentSnapshot()` 故单 table-double 只触发外层，加 executor 直测补内层〕 / 单行 checkState 消息 / partition·WHERE 拒文案）；③ bug-for-bug execute 路（set_current 无-commit 短路 history 不变双分支 / rewrite_manifests spec_id 双臂 / expire deleteWith 分类）；④ infra/dispatch（auth-scope 短路仍失效 / body-fail 不失效 / loadTable-fail 串 / fe-core 列 type+nullability round-trip 双极性 / factory canary / planner 多合取 AND-flatten）。
- **🟡 2 测模型坑实证修（Rule 12 不 overclaim）**：① **expire deleteWith**——初设 `manifests>0` 假设错；实跑 InMemoryCatalog `[0,0,0,0,2,0]`〔退 2 快照=删 2 manifest-**LIST**〔snap-*.avro〕、0 manifest 文件〔数据仍被保留快照引用〕、0 data/delete/stats〕→ `assertEquals(["0","0","0","0","2","0"])` 钉确定值（manifest↔list 分支互换→红）。② **rewrite spec_id 漏 `validate()`**——`NamedArguments.getInt` 读 `parsedValues`（仅 `validate()` 填充），测漏 validate→getInt 返 null→filter no-op→spec_id 全 `[3,0]`（**非产品 bug**，实证 `getInt`→`parsedValues` 依赖 validate）→ 修=validate 先于 execute + 非配 spec_id=1 先跑〔短路无 commit、留 pristine〕。
- **mutation-check（Rule 9/12，HANDOFF 强制 dormant 写路径）**：tx-1 `commitRewriteTxn` 的 `filesToDelete.isEmpty() && filesToAdd.isEmpty()` 改 `||` → 单跑 `IcebergConnectorTransactionTest` **唯 `rewriteDeleteOnlyStillCommitsReplace` 转红**〔delete-only rewrite 被误跳→快照不变→`assertNotEquals` 失败〕→ 证测真 pin AND 闸（非被 iceberg 固有不变式掩盖，**别于 T06 OCC 坑**）→ 已 revert。
- **DV 三层中央登记**（`deviations-log.md` 44→47，镜像 P6.3-T08 DV-041..044 / P6.2-T11 DV-038..040）：**DV-045**〔🔴 BLOCKER = `rewrite_data_files` 执行半翻闸阻塞，R-B 推后专门写路径 RFC，与 DV-041 写路径阻塞同族〕/ **DV-046**〔correctness-bearing = auth-add Kerberos + DV-T05r-where〔经 EXECUTE 双闸不可达 dormant〕〕/ **DV-047**〔perf-cosmetic/behaviour-equiv 批 = cache-to-dispatch〔context!=null〕·session 参·DV-T08-loadwrap·DV-T08-factory-advertise·DV-T06r-{scanpool,zone,rollback}·DV-T07-{where,name-order,exc-contract}·PARTITION(*)·null-row·per-conjunct filter〕。**无新 D**。
- **验证**（`-Dmaven.build.cache.enabled=false` + clean surefire + 核对 mtime）：fe-connector-iceberg `package -Dassembly.skipAssembly=true` **494/0/0/1**（475→494，+19 测，含 expire/rewrite 实证修 + executor 内层前缀测）；fe-core `ConnectorExecuteActionTest` **14/0/0/0**（13→14，+nullable 双极性 round-trip）；`BUILD SUCCESS`；checkstyle 0；`check-connector-imports.sh` exit 0；iceberg 仍**不在** `SPI_READY_TYPES`；**0 BE / 0 pom / 0 `CatalogFactory` 改**（唯 1 行 `IcebergProcedureOps` 注释）。
- **文档同步**：task 表（T08 行 ✅ + 实现记录）/ PROGRESS（header + 连接器表 494 + fe-core 14 + dated 日志 bullet）/ connectors/iceberg.md（当前状态 + 完成度 ~88% + T08 progress-log）/ decisions-log（**无新 D**）/ deviations-log（**DV-045/046/047 中央登记**）。

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
