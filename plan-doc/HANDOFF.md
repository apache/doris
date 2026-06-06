# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [tasks/P4](./tasks/P4-maxcompute-migration.md) → **[P4-T05/T06 翻闸设计](./tasks/designs/P4-T05-T06-cutover-design.md)（✅ 已签字，实现源——本文 §下一步只是它的摘要，实现前通读它）** → [P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md) + [P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)（dormant 边界）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**P4-T05 翻闸接线实现完成**；dormant、gate-green、**未 commit**——用户定时机）
- **本 session 主题**：**实现 T05**（机械、dormant）。GSON 三注册 atomic 迁 compat + 引擎名 case + UT。**4-agent 对抗复核揪出设计 ordered TODO 漏 GSON DB `:452`**（漏迁=翻闸后 `MaxComputeExternalDatabase.buildTableInternal:44` cast `PluginDrivenExternalCatalog`→`MaxComputeExternalCatalog` 抛 ClassCastException），用户签字折入 T05；另 2 告警（`getMetaCacheEngine` / `getMysqlType`）经核判非问题（详见下 / 设计 §3.4）。
- **分支**：`catalog-spi-05`。本场改：fe-core 代码 3 文件（`GsonUtils` / `PluginDrivenExternalTable` / `PluginDrivenExternalCatalog`）+ test 1（`PluginDrivenExternalTableEngineTest` +2 例）+ 文档（设计 §3.4 / decisions-log D-026 校正 / PROGRESS / P4 task / 本 HANDOFF）。**未 commit**（连同上一 design session 的未提交文档改动，一并待用户定 commit 结构）。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（沿用，勿提交）。
- **Batch 状态**：A ✅ + B ✅（gate 关 dormant）；**C：T05 ✅ gate-green（待 commit）/ T06 ⏳**；下一 = **T06a（写接线 dormant）→ T06b（flip）**。

---

## ✅ 本 session 完成项（P4-T05 实现，dormant、gate-green、待 commit）

- **代码（fe-core，3 main + 1 test）**：
  - `GsonUtils`：三 GSON 注册 atomic 迁 `registerCompatibleSubtype`→`PluginDriven*`——catalog `:397` / **db `:452`** / table `:472`；删 3 个 unused `maxcompute.*` import。
  - `PluginDrivenExternalTable.getEngine()` + `getEngineTableTypeName()` 加 `case "max_compute"`（返 `MAX_COMPUTE_EXTERNAL_TABLE.toEngineName()`=null / `.name()`=`"MAX_COMPUTE_EXTERNAL_TABLE"`——**已核 legacy 等价**：legacy MC 表未 override，base `getType().toEngineName()`=null（toEngineName 无 MC case）/ `getType().name()`=`"MAX_COMPUTE_EXTERNAL_TABLE"`）。
  - `PluginDrivenExternalCatalog.legacyLogTypeToCatalogType`：仅加注释（默认分支已出 `"max_compute"`，**不加 case**）。
  - `PluginDrivenExternalTableEngineTest`：+2 max_compute 例（engine=null / typeName=MAX_COMPUTE_EXTERNAL_TABLE），匹配该文件既有 Mockito 风格（设计 §7「no mockito」针对 T06 新文件）；**9/9 绿**。
- **关键校正（用户签字折入 T05）**：ordered TODO 仅列 catalog+table，**漏 GSON DB `:452`**（`MaxComputeExternalDatabase`）；漏迁=翻闸后 `MaxComputeExternalDatabase.buildTableInternal:44` cast `PluginDrivenExternalCatalog`→`MaxComputeExternalCatalog` 抛 `ClassCastException`（es/jdbc/trino 三注册齐迁、legacy DB 类已删=必走 compat）。
- **4-agent 对抗复核（read-only Explore）**：2 PASS（GSON 完整性=无其他注册站 / label 唯一性=atomic replace 正确）+ 2 FAIL 经核**判非问题**：
  - `getMetaCacheEngine`→"default"=**假阳性**（plugin 路径经连接器 `initSchema` 取 schema、走 "default" 桶同 es/jdbc/trino；`MaxComputeExternalMetaCache` 仅 legacy `MaxComputeExternalTable:71,122` 引用=Batch-D 死码；分区经连接器 P4-T02）。
  - `getMysqlType`→"BASE TABLE"=**同 ES 既定行为**（`ES_EXTERNAL_TABLE` 亦不在 `toMysqlType` switch，迁后 null→"BASE TABLE" 已 ship 无 override）。
  - dormancy「新建 MC catalog 不能序列化」=**既载中间态 caveat**（其"留 registerSubtype"修法错=撞 duplicate-label IAE）。
- **守门全绿**：fe-core compile BUILD SUCCESS / checkstyle 0 / import-gate 0 / UT 9-0-0（真实 BUILD/MVN_EXIT/CS_EXIT 核验；坑7：后台通知 exit code 不可信，已读日志确认）。

---

## 🚧 下一 session = 实现 T06（先 T06a，再 T06b）

> **通读 [设计文档](./tasks/designs/P4-T05-T06-cutover-design.md) §3.4（T05 实现注记）/ §4 / §8 ordered TODO**，本节仅摘要。每 commit 独立（用户定时机）。

**P4-T05 ✅ 完成**（dormant、gate-green、**待 commit**）——见上「本 session 完成项」+ 设计 §3.4。
> ⚠️ T05 后、flip 前=中间不可部署态（compat subtype 已注册但 factory 仍 legacy；P2 batch-B precedent）。**T05 已落但未 commit**——下一 session 接 T06，二者紧邻落、勿中间部署。

**P4-T06a（写接线，全 additive/dormant-safe，commit `[P4-T06a]`）**：
4. **W-a（G1）**：`ConnectorSession.setCurrentTransaction` default + `ConnectorSessionImpl`（`volatile ConnectorTransaction` 字段 + setter + `@Override getCurrentTransaction`）+ `PluginDrivenTableSink.getConnectorSession()` getter（字段 :114 现无 getter）。
5. **W-b（D-1）**：`ConnectorWriteOps.usesConnectorTransaction()` default false + `MaxComputeConnectorMetadata` override true。
6. **W-c（G2）**：`PluginDrivenInsertExecutor` 重构——连接器/session/writeOps setup 提到 `beginTransaction()` 开头；txn-model：`connectorTx=writeOps.beginTransaction(execSession)`→`txnId=((PluginDrivenTransactionManager)transactionManager).begin(connectorTx)`；`finalizeSink` override 在 `super.finalizeSink` 前 `((PluginDrivenTableSink)sink).getConnectorSession().setCurrentTransaction(connectorTx)`；`beforeExec` 对 txn-model `if(connectorTx!=null) return`（跳 beginInsert）；`transactionType()`→MAXCOMPUTE。（`doBeforeCommit:108`/`onFail:140` 已 guard insertHandle!=null，MC 为 null 自动跳过。）
7. **W-d（G3）**：`PluginDrivenTransactionManager.begin(connectorTx)` 加 `GlobalExternalTransactionInfoMgr.putTxnById`；核 commit/rollback `removeTxnById`（缺则补，镜像 `AbstractExternalTransactionManager:42-54`）。
8. **§4.2（G4/G5，D-3）**：`UnboundConnectorTableSink` 加 `staticPartitionKeyValues`(+ctor) + `UnboundTableSinkCreator:66-110` 透传；`InsertIntoTableCommand:567-598`（镜像 MC 分支 :564-581）+ `InsertOverwriteTableCommand:407-418`（加 plugin-driven 分支 `setOverwrite(true)`+`setStaticPartitionSpec`）填 `PluginDrivenInsertCommandContext`（被 `PluginDrivenTableSink.bindViaWritePlanProvider:212-224` 读）。
9. **R-004 part-1**（classloader 隔离 UT，无 creds，CI 可跑）：仿 `ConnectorPluginManager`+`ChildFirstClassLoader` 载插件、建 ODPS client（`MCConnectorClientFactory`），断无 `NoClassDefFoundError`/`ClassCastException`/单例污染。**part-2**（live 连通，需 creds）我写、**用户跑**（env/system property 传 creds，勿提交）。
10. UT（设计 §7）+ 守门 `-pl :fe-connector-maxcompute,:fe-connector-api,:fe-core -am` compile + checkstyle + import-gate。

**P4-T06b（flip，唯一 live-switch，commit `[P4-T06b]`）**：
11. `CatalogFactory.SPI_READY_TYPES += "max_compute"`(:52) + 删 `case "max_compute"`(:146-149) + import。
12. doc-sync 5 步 + decisions-log（D-026 已记设计；impl 时补 2 SPI → 01-spi-rfc §20 E11）。**翻闸完成 = 用户跑 R-004 part-2 绿**。

> **Batch C 后**：D（清 ~19 反向引用 + 删 `datasource/maxcompute/` + 验 `MCInsertExecutor` 死代码 OQ-1，**入口先完整 re-grep** OQ-3）→ E（连接器测试基线 P4-T10 + PR P4-T11）。

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
