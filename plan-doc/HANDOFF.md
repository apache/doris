# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [tasks/P4](./tasks/P4-maxcompute-migration.md) → **[P4-T05/T06 翻闸设计](./tasks/designs/P4-T05-T06-cutover-design.md)（✅ 已签字，实现源——本文 §下一步只是它的摘要，实现前通读它）** → [P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md) + [P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)（dormant 边界）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-07（**T05 + T06a 均已 commit**；T06a = 写接线 + UT 套件 + 全守门绿。下一 = **T06b flip**）
- **本 session 主题**：写 **T06a UT 套件**（设计 §7）→ 全守门 → commit `[P4-T06a]`。① 6 测试文件（2 扩展 + 4 新；15 新用例，1 因无 creds skip）覆盖 W-a..W-d/G1–G5 + R-004 part-1（隔离，CI）/part-2（live，用户跑）；② **揪出并验证**前一 session 跳过的 UT 门——W-d 的 `putTxnById` 本可破已有 `PluginDrivenTransactionManagerTest`，核实真 `Env` 单例裸测可建（5/5 仍绿，无回归）；③ 全守门绿（见「✅ 本 session 完成项」末）。
- **分支**：`catalog-spi-05`。**T06a 已 commit**（11 接线 + 6 测试 + 本 HANDOFF，一个 `[P4-T06a]` commit）。T05 已 commit（`2534d76`+`67e0e5a`）。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（沿用，勿提交）。
- **Batch 状态**：A ✅ + B ✅；**C：T05 ✅ commit / T06a ✅ commit（代码+UT 全绿）/ T06b ⏳**；下一 = **T06b flip**（`SPI_READY_TYPES += "max_compute"` + 删 CatalogFactory case :146-149 + doc-sync 5 步 + decisions-log）。**T06a↔flip 中间不可部署**（compat 已注册但 factory 仍 legacy）。

---

## ✅ 本 session 完成项

**① T05 — 已 commit（2 commit，gate 全绿 + UT 9/9）**：GSON 三注册（catalog `:397` / **db `:452`** / table `:472`）atomic 迁 `registerCompatibleSubtype`→`PluginDriven*` + 删 3 unused import；`PluginDrivenExternalTable.getEngine/getEngineTableTypeName` 加 `case "max_compute"`（核 legacy 等价：engine=null / typeName=MAX_COMPUTE_EXTERNAL_TABLE）；`legacyLogTypeToCatalogType` 仅注释。**db `:452` 是设计 ordered TODO 漏项**（漏迁=翻闸后 `buildTableInternal:44` cast `PluginDrivenExternalCatalog`→`MaxComputeExternalCatalog` ClassCastException），用户签字折入。UT `PluginDrivenExternalTableEngineTest` +2 例。commit `67e0e5a`(docs[D-026+T05 注记]) + `2534d76`(`[P4-T05]`)。详见设计 §3.4 / decisions-log D-026 校正。**复核另判非问题**：`getMetaCacheEngine`→"default"（假阳性，plugin 走连接器 initSchema + "default" 桶同 es/jdbc/trino，`MaxComputeExternalMetaCache` 仅 legacy 引用=Batch-D 死码）；`getMysqlType`→"BASE TABLE"（同 ES 既定，ES 迁移已 ship 无 override）。

**② T06a 写接线 — 代码全部实现、gate 全绿、未 commit、UT 待写**（11 文件 +246/−24，见「📦 WIP 清单」）：
  - **W-a(G1)**：`ConnectorSession.setCurrentTransaction` default(throw) + `ConnectorSessionImpl`（volatile `currentTransaction` 字段 + setter + `getCurrentTransaction` override）+ `PluginDrivenTableSink.getConnectorSession()` getter。
  - **W-b(D-1)**：`ConnectorWriteOps.usesConnectorTransaction()` default false + `MaxComputeConnectorMetadata` override true。
  - **W-c(G2)**：`PluginDrivenInsertExecutor` 重构——`ensureConnectorSetup()` 惰性幂等 helper；`beginTransaction()` override（setup → usesConnectorTransaction? `connectorTx=writeOps.beginTransaction(session)`+`txnId=((PluginDrivenTransactionManager)transactionManager).begin(connectorTx)` : super）；`finalizeSink()` override（connectorTx!=null 时 `sink.getConnectorSession().setCurrentTransaction(connectorTx)` 后 super）；`beforeExec()`（connectorTx!=null 早返）；`transactionType()`→connectorTx!=null 返 MAXCOMPUTE。
  - **W-d(G3)**：`PluginDrivenTransactionManager.begin(connectorTx)` 加 `putTxnById`；commit(**try/finally**)/rollback(finally) 加 `removeTxnById`。
  - **G4/G5(D-3)**：`UnboundConnectorTableSink` 加 `staticPartitionKeyValues` 字段/ctor/getters（镜像 `UnboundMaxComputeTableSink`）；`UnboundTableSinkCreator` 3 个 plugin 分支透传；`InsertIntoTableCommand`(plugin 分支填 ctx) + `InsertOverwriteTableCommand`(加 plugin 分支 setOverwrite+staticSpec) 填 `PluginDrivenInsertCommandContext`。
  - **复核修 2 处**：commit `try/finally removeTxnById`（防 commit 抛时全局泄漏）；`PluginDrivenTableSink.getExplainString` 加 `writeConfig==null` 守卫（防 plan-provider 模式 EXPLAIN NPE，T04 遗留、翻闸后可达）。

**③ recon + 对抗复核结论（勿重做）**：
  - 6-agent recon：T06a 所有锚点 vs 设计 **"none" drift**（确认 `GlobalExternalTransactionInfoMgr.putTxnById(long,Transaction)`、`PluginDrivenTransaction implements Transaction`、`TransactionType.MAXCOMPUTE` 存在、`PluginDrivenExternalCatalog` 用 `PluginDrivenTransactionManager`、empty-insert `runInternal:694` 早返）。
  - 4-lens 对抗复核：**MC happy-path 正确**（关键：`finalizeSink` 的 `sink.getConnectorSession()` == translate 期 session(`PhysicalPlanTranslator:658`) == planWrite 读的同一对象）；**jdbc/es/trino 无回归正确**（setup 提前=幂等无副作用、finalizeSink 对其 no-op、bindViaWritePlanProvider 对 jdbc 不跑）；**已修 2 真问题**（见上）；**判非问题/不改**：commit 失败不 rollback connectorTx（既有 remove-then-commit 序、`close()` 仍跑、out of W-d scope）、`putTxnById` 非原子 + RuntimeException（既有共享码、id 单调唯一不可触发）、empty-insert "blocker"（**假警**——空插入 `runInternal` 早返、全程不执行）。
  - gate（2 次：初版 + 修后）：fe-core/-api/-maxcompute compile BUILD SUCCESS + checkstyle 0 + import-gate 0（真实 EXIT 核验；坑7：后台通知 exit code 不可信）。

**④ T06a UT 套件 — 写完、全守门绿、随接线一并 commit `[P4-T06a]`**（6 文件：2 扩展 + 4 新）：
  - `ConnectorSessionImplTest`(+3)：W-a/G1 `set/getCurrentTransaction` round-trip（空→绑同一实例→null 解绑）。
  - `PluginDrivenTransactionManagerTest`(+4)：W-d/G3 全局注册 + commit/rollback 注销，含 **commit 抛异常 finally 仍注销**。`RecordingConnectorTransaction` 加 `failOnCommit`。
  - `PluginDrivenInsertExecutorTest`(新,4)：W-c/G2 顺序——beginTransaction 开+注册；**finalizeSink 在 planWrite 前把 txn 绑到 sink 的 session**（recording provider 在 planWrite 时读 `getCurrentTransaction` 证实）；beforeExec 对 txn-model 早返；transactionType=MAXCOMPUTE。**构造坑**：7-arg ctor 建 Coordinator 无法裸构造 → 用 Objenesis（`Mockito.mock(CALLS_REAL_METHODS)`）仅跳 ctor + `Deencapsulation` 注入私有字段（含 final `transactionManager`，`FieldReflection` 对非静态 final 可 setAccessible 写）；断言全跑真实码。
  - `PluginDrivenTableSinkBindingTest`(新,2)：G4/G5 **消费端**——`bindViaWritePlanProvider` 把 overwrite + 静态分区 spec 透传进 `ConnectorWriteHandle`。生产端（command 填 ctx）按用户决定**延到翻闸后手动 smoke**（无现成集成 harness，建之过重/易脆）。
  - `OdpsClassloaderIsolationTest`(新,1，fe-connector-maxcompute 首个测试，无 mockito)：R-004 part-1，自带 child-first `URLClassLoader`（fe-connector-maxcompute 看不到 fe-core 的 `ChildFirstClassLoader`；本 loader 全隔离=生产策略超集）载 ODPS SDK + `createClient`(假 AK/SK，离线) → 断无 NoClassDefFound/ClassCast + 双 loader 类不同（SDK 按 plugin 隔离）。
  - `OdpsLiveConnectivityTest`(新,1)：R-004 part-2，`assumeTrue` 环境变量门（`MC_ENDPOINT`/`MC_PROJECT`/`MC_ACCESS_KEY`/`MC_SECRET_KEY`），CI 自动 skip、**用户跑**（翻闸完成门）。
  - **守门全绿（真实 EXIT/计数核）**：fe-core 新 UT 29-0-0、maxcompute 2-0（1 skip）；回归 `InsertIntoTableCommandTest` 3-0-0 + `PluginDrivenTableSinkTest` 1-0-0；checkstyle 0×3（`fe/pom.xml:162` 含 test 源）；import-gate 0。

---

## 🚧 下一 session = T06b flip（唯一 live-switch，commit `[P4-T06b]`）

> **T06a 已 commit**（11 接线 + 6 测试 + 本 HANDOFF，一个 `[P4-T06a]`）。**接线 + UT 全守门绿、已 4-lens 复核，勿重做/勿重 recon。** 下一 = 翻闸。每 commit 独立（用户定时机），flip 末提、孤立。

**P4-T06b 步骤**：
1. `CatalogFactory.SPI_READY_TYPES += "max_compute"`(:52) + 删 `case "max_compute"`(:146-149) + 清对应 import。
2. doc-sync 5 步 + decisions-log（D-026 已记设计；翻闸 commit 时补 **2 SPI 新增**=`ConnectorSession.setCurrentTransaction` + `ConnectorWriteOps.usesConnectorTransaction` → 01-spi-rfc §20 E11；T06a 复核已修的 `PluginDrivenTableSink.getExplainString` `writeConfig==null` NPE 守卫亦记一笔）。**翻闸完成 = 用户跑 R-004 part-2（`OdpsLiveConnectivityTest`，设 4 个 `MC_*` 环境变量）绿**。

> ⚠️ **T06a↔flip 中间不可部署**（compat 已注册但 factory 仍 legacy）。T06a/T06b 紧邻落、勿中间部署。

> **Batch C 后**：D（清 ~19 反向引用 + 删 `datasource/maxcompute/` + 验 `MCInsertExecutor` 死代码 OQ-1，**入口先完整 re-grep** OQ-3）→ E（连接器测试基线 P4-T10 + PR P4-T11）。

### 📦 `[P4-T06a]` commit 内容（已落，供参考）

```
fe-connector-api:        ConnectorSession.java · ConnectorWriteOps.java
fe-connector-maxcompute: MaxComputeConnectorMetadata.java
                         + test: OdpsClassloaderIsolationTest · OdpsLiveConnectivityTest
fe-core:                 ConnectorSessionImpl.java · PluginDrivenTableSink.java ·
                         PluginDrivenInsertExecutor.java · PluginDrivenTransactionManager.java ·
                         UnboundConnectorTableSink.java · UnboundTableSinkCreator.java ·
                         InsertIntoTableCommand.java · InsertOverwriteTableCommand.java
                         + test: ConnectorSessionImplTest · PluginDrivenTransactionManagerTest ·
                           PluginDrivenInsertExecutorTest · PluginDrivenTableSinkBindingTest
```
未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak` 勿提交。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **commit 协议红线**（沿用）：`TMCCommitData` 反序列化必 `TBinaryProtocol`（`MaxComputeConnectorTransaction.addCommitData:110-120` 已落）。T06 sink/BE 契约不变。
2. **写生命周期顺序（T06 关键）**：`InsertIntoTableCommand.initPlan` = **translate（建 sink+其 connectorSession，PhysicalPlanTranslator:658）→ `beginTransaction():354` → `finalizeSink():355`（→bindDataSink→planWrite 读 **sink 的** session.getCurrentTransaction）**。故 connectorTx 必在 step2 建、step3 前绑入 **sink 的 session**（非 executor 的）。executor 与 sink 各有一 session，txn 经 W-a 引用共享。
3. **别重开 T03/T04**：Approach A locked（planWrite 一处建 ODPS session+setWriteSession+盖 txn_id/write_session_id，**fail-loud** 若 session 无 txn——`MaxComputeWritePlanProvider:199`）。本设计接线 *到* 它。
4. **block_id 不在 planWrite**（运行期 T03 `allocateWriteBlockRange`）；`session_id`(field1) 不用；`partition_columns`(14) 取 ODPS 表列（DV-012）。
5. **import-gate**（坑5）：连接器禁 `org.apache.doris.(common|catalog|datasource|qe|analysis|nereids|planner)`；异常 `DorisConnectorException`；允许 `thrift.*` + `connector.api.*`。**fe-core seam 文件不受 import-gate**（`PluginDrivenInsertExecutor`/`PluginDrivenTableSink`/`PluginDrivenTransactionManager`/`ConnectorSessionImpl`/Unbound*/Insert*Command 全在 fe-core）。
6. **maven 必绝对 `-f` + `-pl :artifactId`**（坑6）：改 fe-core **必带 `:fe-core`**（`-am` 不自动带）；T06 还须 `:fe-connector-api`（动 SPI）。裸名被当相对路径 → reactor not found。
7. **读真实 exit code**（坑7）：命令尾 `echo "MVN_EXIT=$?" >> log`，grep `BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|Tests run:` 核；**勿信**后台 task-notification 的「exit code」。
8. **checkstyle**（坑8）：`CustomImportOrder`（doris.* 字母序 → 第三方 → java.*，组间空行、组内字母序大小写敏感）；`UnusedImports`/`RedundantImport`；`LineLength` 120。
9. （沿用）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只扫 `*/src/main/java`。
10. **ODPS API jar**（坑10）：写 session 类在 odps-sdk-table-api；`PartitionSpec`/`TableIdentifier` 在 odps-sdk-commons；`maxcompute.version`。client 建在 `MCConnectorClientFactory`（需 `mc.endpoint`/`mc.default.project`/auth）。
11. **R-004 两分**：① classloader 隔离正确性（无 creds，CI 可跑，UT）≠ ② live 连通（creds，用户跑）。真风险=单例污染，①即足；②确认端到端。仓内**无** ODPS creds/harness（`FakeConnectorPluginTest` 不走隔离 classloader）。
12. **TransactionType.MAXCOMPUTE 已存**（enum）；泛型 executor 对 txn-model 返 MAXCOMPUTE = profiling-only、MC 唯一 adopter 期正确，第 2 个 txn-model 连接器来时再议。

---

## 📂 关键文件锚点

```
设计（实现源，勿重做）： tasks/designs/P4-T05-T06-cutover-design.md（✅签字；§3 T05 / §4 T06 / §5 决策 / §8 ordered TODO）
T03/T04 设计：           tasks/designs/P4-T03-write-txn-design.md · P4-T04-write-plan-design.md（dormant 槽）

T05 靶（fe-core）：  GsonUtils:397/:472 · PluginDrivenExternalTable.getEngine:196-215/getEngineTableTypeName:218-231
T06a 靶：
  SPI：   ConnectorSession(+setCurrentTransaction) · ConnectorWriteOps(+usesConnectorTransaction) [fe-connector-api]
  fe-core：ConnectorSessionImpl(+字段/override) · PluginDrivenTableSink(+getConnectorSession) ·
          PluginDrivenInsertExecutor(beginTransaction/finalizeSink/beforeExec/transactionType 重构) ·
          PluginDrivenTransactionManager.begin(connectorTx):71-77(+putTxnById) ·
          UnboundConnectorTableSink(+静态分区) · UnboundTableSinkCreator:66-110 ·
          InsertIntoTableCommand:564-598 · InsertOverwriteTableCommand:407-418
  连接器： MaxComputeConnectorMetadata(+usesConnectorTransaction override) · 新 OdpsClassloaderIsolationTest(R-004)
T06b 靶：  CatalogFactory:52(SPI_READY_TYPES) + 删 :146-149 case
保留（image 兼容，勿删）： TableIf.MAX_COMPUTE_EXTERNAL_TABLE:220 · InitCatalogLog.MAX_COMPUTE:41

legacy（港的源，勿改/Batch D 删）： datasource/maxcompute/* · nereids/.../insert/MCInsertExecutor · MCInsertCommandContext

守门命令（T06 改 SPI+fe-core，必带 :fe-connector-api,:fe-core）：
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute,:fe-connector-api,:fe-core -am \
    -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -DskipTests compile   # 后台
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute,:fe-connector-api,:fe-core \
    -Dmaven.build.cache.enabled=false checkstyle:check
  bash tools/check-connector-imports.sh                           # 从 repo 根跑
```

---

## 🔴 开放问题（沿用）

- **OQ-1**：翻闸后 INSERT/INSERT OVERWRITE 是否完全不再经 `MCInsertExecutor`（→ cast `MCTransaction` 死代码）？**Batch D / P4-T08 验**。
- **OQ-3**：反向引用穷举（gson/enum/metacache 注册站未穷举）——**Batch D / P4-T07 入口先完整 re-grep**。
- **R-004**：part-1（隔离 UT）入 T06a；part-2（live 连通）我写、用户跑（翻闸完成门）。
- ~~OQ-2~~ ✅ 已解（P4-T04 Approach A）；~~OQ-4~~ ✅ 已定（P4-T02 不建自有 cache）。

---

## 🧠 给下一个 agent 的 meta 建议

- **先通读 [翻闸设计](./tasks/designs/P4-T05-T06-cutover-design.md)**（已 verified + 签字），勿重做 recon。先 T05（小、安全、dormant）再 T06。
- **dormant→live 接线是 T06 核心**（5 gap G1–G5）：G1 session 绑 txn + G3 全局注册 + G5 binding 填 ctx——三者缺一翻闸即断（planWrite 已 fail-loud）。
- **改 SPL/fe-core ⇒ 守门 `-pl` 必带 `:fe-connector-api,:fe-core`**（坑6）；读真实 BUILD/MVN_EXIT/CS_EXIT（坑7）。
- **每 commit 独立**（用户定时机）；flip（T06b）末提、孤立。
