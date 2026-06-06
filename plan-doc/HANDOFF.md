# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [tasks/P4](./tasks/P4-maxcompute-migration.md)（批次计划）→ [写 RFC](./tasks/designs/connector-write-spi-rfc.md)（§5.5 写-plan-provider / §6 data flow / §12 P4 TODO）+ [P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md)（T03 已落，T04 紧接）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**P4-T03 实现 · Batch B 启动**）
- **本 session 主题**：**P4-T03 连接器写/事务 SPI 完成**（Batch B 启，gate 关、dormant、零 live 风险）。新建 `MaxComputeConnectorTransaction`（港 legacy `MCTransaction` 写生命周期）+ `MaxComputeConnectorMetadata.beginTransaction`，over W4 委派。**两 fork 用户签字 [D-024]**；偏差 [DV-011]。守门全绿。
- **分支**：`catalog-spi-05`。**本场代码 4 文件**（新 `MaxComputeConnectorTransaction` + `MaxComputeConnectorMetadata.beginTransaction` + SPI `ConnectorSession.allocateTransactionId` + fe-core `ConnectorSessionImpl` override）+ doc-sync。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（沿用，勿提交）。**commit 时机由用户定**。
- **Batch A（DDL+分区）= 全完成**（T01 ✅ + T02 ✅）；**Batch B 启**（T03 ✅，T04 待做）。

---

## ✅ 本 session 完成项（P4-T03，gate 关、dormant、零 live 风险）

- **新建 `MaxComputeConnectorTransaction implements ConnectorTransaction`**（港 legacy `MCTransaction` 写生命周期）：
  1. `addCommitData(byte[])`：`TDeserializer(new TBinaryProtocol.Factory())`→`TMCCommitData`→累积（**commit 协议红线：必 TBinaryProtocol**）。
  2. block 分配：`supportsWriteBlockAllocation()`=true + `allocateWriteBlockRange(sid,count)`（校验 count>0 / writeSessionId 已设+匹配 + CAS `nextBlockId` + 上限）。
  3. `commit()`：港 `finishInsert`（`appendCommitMessages` Base64+ObjectInputStream→`WriterCommitMessage`；restore session by `writeSessionId`+`tableIdentifier`+`settings` → `session.commit(msgs)`）。
  4. `rollback`（log no-op，session 自过期）/ `close`（no-op）/ `getUpdateCnt`（Σ rowCount）/ `getTransactionId`。
- **`MaxComputeConnectorMetadata.beginTransaction(session)`** → `new MaxComputeConnectorTransaction(session.allocateTransactionId())`。
- **两 fork [D-024]（用户签字）**：
  - **Fork 1（txn id）**：新增 SPI `ConnectorSession.allocateTransactionId()`（default 抛；fe-core `ConnectorSessionImpl` override → `Env.getCurrentEnv().getNextId()`）；MC 经它分配。**尊重 [D-015]/U3「连接器分配」**，补 id-less 连接器机制。id = Doris 全局 txn_id（与 sink / `GlobalExternalTransactionInfoMgr` 一致）。E11 登记。
  - **Fork 2（T03/T04 边界）**：ODPS 写 session 创建**挪 T04 planWrite**（`ConnectorWriteHandle` 带 overwrite+writeContext，顺解 OQ-2）。**T03 = 纯事务容器**：`writeSessionId`/`tableIdentifier`/`settings` 是**槽 + `setWriteSession()` setter**，由 T04 填。
- **偏差 [DV-011]（R12 不静默）**：import-gate 禁 `org.apache.doris.common.*` → block 上限 `Config.max_compute_write_max_block_count`(20000)→连接器常量 `MAX_BLOCK_COUNT=20000L`（丢 fe.conf 可调性）；`throws UserException`→`DorisConnectorException`（unchecked）。
- **认知**：**JDBC 仅半样板**（impl `ConnectorWriteOps` no-op insert，**无 `ConnectorTransaction`**）；**MC 是首个有状态事务（block 分配）adopter**。BE→FE 回调已 W3/W6 泛化（`FrontendServiceImpl:3694` getTxnById→`allocateWriteBlockRange`；`CommitDataSerializer`→`addCommitData`）。
- **守门全绿**（真实 EXIT 核验）：`-pl :fe-connector-maxcompute,:fe-core -am` compile **BUILD SUCCESS/MVN_EXIT=0** + checkstyle **0**（连接器 + api + fe-core）+ import-gate **0**。
- **测试（R12 不静默）**：T03 **无新单测**——按 P4 计划连接器测试基线延至 **P4-T10**（write-txn golden：`TBinaryProtocol` round-trip、block-alloc CAS/上限/mismatch、getUpdateCnt Σ）。T03 gate = compile + checkstyle + import-gate（与计划一致，非静默跳过）。

---

## 🚧 下一 session = P4-T04 写计划（Batch B 续，gate 关、dormant）

**第一步 = P4-T04。** ⚠️ **T04 未 recon 逐行定稿** —— 先 recon 再写。建议首步（精读，offset+limit）：
1. 读 **SPI 写-plan 面**：`fe-connector-api` `ConnectorWritePlanProvider.planWrite(session, handle)→ConnectorSinkPlan` + `ConnectorWriteHandle`（`getTableHandle`/`getColumns`/`isOverwrite`/`getWriteContext`）+ `ConnectorSinkPlan`（包 opaque `TDataSink`）+ `Connector.getWritePlanProvider`（default null）。
2. 读 **W5 接线**：`PhysicalPlanTranslator.visitPhysicalConnectorTableSink` + `planner/PluginDrivenTableSink`（W5 在其上 layer `planWrite()`，见 [DV-009]）；**W5 留空 writeContext**——OQ-2 重建点。
3. 读 **legacy 源**：`MCTransaction.beginInsert`（建 ODPS 写 session：`TableWriteSessionBuilder` + static/dynamic partition + overwrite + ArrowOptions；港入连接器写侧）+ `MaxComputeTableSink`（fe-core，sink config-read：endpoint/project/credentials/partition）+ `MCInsertExecutor.beforeExec`（runtime txn_id/write_session_id 注入路径，OQ-2 需重建）。
4. 读 **thrift**：`TMaxComputeTableSink`（`gensrc/thrift/DataSinks.thrift:586`，18 字段：`write_session_id`(15)/`block_id_start`(8)/`block_id_count`(9)/`static_partition_spec`(10)/`partition_columns`(14)/`txn_id`(18)/`properties`(16)）。
5. 然后实做：`MaxComputeConnectorProvider/DorisConnector.getWritePlanProvider` + `planWrite`（**创建 ODPS 写 session** → 经 `session.getCurrentTransaction()` 拿 `MaxComputeConnectorTransaction` → `setWriteSession(writeSessionId, tableIdentifier, settings)` 绑定 → 产 `TMaxComputeTableSink`）+ `supportsInsert`/`getWriteConfig`/`beginInsert`/`finishInsert`（insert 机制面）。**独立 commit `[P4-T04]`**。
6. 守门：连接器 compile + checkstyle + import-gate（**`-pl :fe-connector-maxcompute`**，坑 6）；若加测则 golden。

> **OQ-2 = T04 核心难点**（见坑 2 / [DV-009]）：W5 留**空** writeContext；runtime `txn_id`/`write_session_id` 原经 `MCInsertExecutor.beforeExec` 注入，plugin-driven 写侧需**重建**该路径填 `TMaxComputeTableSink`。`ConnectorWriteHandle.isOverwrite()`+`getWriteContext()` 是 overwrite/静态分区 context 入口（但 `PluginDrivenInsertCommandContext` 现仅继承 overwrite、无静态分区 → 静态分区注入需补，见坑 2）。
> **写 session 依赖**：T03 的 `commit()`/block-alloc 依赖 T04 经 `setWriteSession` 填 `writeSessionId`/`tableIdentifier`/`settings`。`EnvironmentSettings` 现仅 `MaxComputeScanPlanProvider` 建（line 157）→ T04 需复用/抽出；`maxFieldSize` 见 `MCConnectorProperties.MAX_FIELD_SIZE`（默认 8388608）。
> **Batch A+B 全绿 + R-004 防御测过 才进 C（翻闸，唯一 live 切点）**。**别回头改 W-phase / 别先翻闸**。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **legacy 是「删」非「搬」**：连接器**已有**读侧等价 + DDL（T01）+ 分区（T02）+ **写/事务（T03）已港**。剩 **写计划（T04，含 ODPS 写 session 创建）** 一块；`datasource/maxcompute/` 在 cutover（Batch D）**删除**。
2. **写-context 注入 = T04 核心难点（OQ-2）**：见上「下一 session」。`TMaxComputeTableSink` 字段：txn_id:18 / write_session_id:15 / block_id:8,9 / static_partition_spec:10。见 [DV-009]。
3. **commit 协议红线**：`addCommitData` 反序列化 `TMCCommitData` 必 `TBinaryProtocol`（单点 `CommitDataSerializer`）。**T03 已落**（`MaxComputeConnectorTransaction.addCommitData`）；T04 sink/BE 契约不变。
4. **`getTxnById` 抛异常非返 null**（`GlobalExternalTransactionInfoMgr`）——W3 已修（`FrontendServiceImpl:3694` 经它查 txn 调 `allocateWriteBlockRange`，零 instanceof）。**翻闸期接线注意**：executor 调 `beginTransaction`→`begin(connectorTx)` + 全局注册 = Batch C（保 T03/T04 dormant、不破 JDBC/ES）。
5. **import-gate 禁 `org.apache.doris.common.*`**（含 `Config`/`UserException`）：T04 sink-build 同样不可用 fe-core `Config`；连接器异常用 `DorisConnectorException`。允许 `org.apache.doris.thrift.*`（含 `TMaxComputeTableSink`/`TMCCommitData`）。
6. **maven 必绝对 `-f` + `-pl :artifactId`**：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute ...`；裸名被当**相对路径** → `Could not find the selected project in the reactor`。**改了 fe-core（如 T03 的 `ConnectorSessionImpl`）须把 `:fe-core` 一并入 `-pl`**（连接器 `-am` 不会带 fe-core）。
7. **读真实 exit code**：命令尾 `echo "MVN_EXIT=$?" >> log`，grep `BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|Tests run:` 核；**勿信**后台 task-notification 的「exit code」。
8. **checkstyle**：`CustomImportOrder`（`org.apache.doris.*` 字母序 → 第三方 → `java.*`，组间空行、组内字母序大小写敏感）；`UnusedImports`/`RedundantImport`；`LineLength` 120；test 源也扫、禁 static import。无 `IllegalCatch`/`IllegalThrows`（`catch (Exception)` / `throws Exception` 可用）。
9. （沿用）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只扫 `*/src/main/java`、只禁 connector→fe-core 单向。
10. **ODPS API jar 认准**：`PartitionSpec` 在 odps-sdk-commons；写 session 类（`TableWriteSessionBuilder`/`TableBatchWriteSession`/`WriterCommitMessage`/`EnvironmentSettings`(`com.aliyun.odps.table.enviroment`，注意拼写)）在 **odps-sdk-table-api**（`maxcompute.version=0.53.2-public`）。javap 核 API（T03 已核写 session builder）。

---

## 📂 关键文件锚点

```
P4 计划：  tasks/P4-maxcompute-migration.md（5 批/11 task；Batch A ✅ + B-T03 ✅，下一 T04）
写 RFC：   tasks/designs/connector-write-spi-rfc.md（§5.5 写-plan-provider / §6 data flow / §7 三写者映射 / §12 P4 TODO）
T03 设计： tasks/designs/P4-T03-write-txn-design.md（事务容器 + 两 fork + dormant 边界）
决策：     decisions-log.md D-024（T03 两 fork）/ D-023（full adopter）/ D-022（写 SPI）/ D-015（id 连接器分配）
偏差：     deviations-log.md DV-011（block 上限常量 + 异常类型）/ DV-009（W5 planWrite layer）

Batch B 靶（写计划，T04）：
  SPI 面：  fe-connector-api ConnectorWritePlanProvider / ConnectorSinkPlan / ConnectorWriteHandle；Connector.getWritePlanProvider
  W 接线：  PhysicalPlanTranslator.visitPhysicalConnectorTableSink + planner/PluginDrivenTableSink（W5 layer planWrite）
  legacy：  MCTransaction.beginInsert（建 ODPS 写 session）/ MaxComputeTableSink（sink config-read）/ MCInsertExecutor.beforeExec（txn_id/session_id 注入，OQ-2）
  thrift：  gensrc/thrift/DataSinks.thrift:586 TMaxComputeTableSink（18 字段）
  连接器：  新 planWrite + getWritePlanProvider；写 session 经 MaxComputeConnectorTransaction.setWriteSession 绑定（T03 已留 setter）

已完成锚点（勿重做）：
  DDL（T01）：MaxComputeConnectorMetadata createTable/dropTable/createDatabase/dropDatabase + MCTypeMapping.toMcType
  分区（T02）：MaxComputeConnectorMetadata listPartitionNames/listPartitions/listPartitionValues
  写/事务（T03）：MaxComputeConnectorTransaction（addCommitData[TBinaryProtocol]/block-alloc/commit/rollback/getUpdateCnt）
                + MaxComputeConnectorMetadata.beginTransaction + ConnectorSession.allocateTransactionId（SPI 加面）+ ConnectorSessionImpl override

翻闸点（Batch C，recon §5 已 pin）：
  CatalogFactory:52 SPI_READY_TYPES + 删 :146 case；GsonUtils ~405/~478 registerCompatibleSubtype；
  PluginDrivenExternalTable.getEngine ~203-231 + legacyLogTypeToCatalogType ~347；
  + executor 接线（beginTransaction→begin(connectorTx)）+ GlobalExternalTransactionInfoMgr 注册（T03/T04 dormant 的 live 化）

守门命令（连接器模块名 = fe-connector-maxcompute；改 fe-core 加 :fe-core）：
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute[,:fe-core] -am \
    -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -DskipTests compile   # 编译（-am 慢，建议后台）
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute[,:fe-core] \
    -Dmaven.build.cache.enabled=false checkstyle:check
  bash tools/check-connector-imports.sh                           # 从 repo 根跑
```

---

## 🔴 开放问题（Batch 入口前确认）

- **OQ-1**：翻闸后 `InsertIntoTableCommand`/`InsertOverwriteTableCommand` 是否完全不再经 `MCInsertExecutor`（→ cast `MCTransaction` 成死代码）？**Batch B/D 验**。
- **OQ-2**：write-context（overwrite/静态分区 + runtime txn_id/write_session_id）填充路径重建——**T04 核心**（见坑 2 / [DV-009]）。`PluginDrivenInsertCommandContext` 现仅继承 overwrite、无静态分区。
- **OQ-3**：反向引用穷举（category-C 注册站 gson/enum/metacache 未穷举）——**Batch D / P4-T07 入口先完整 re-grep**。
- ~~**OQ-4**：连接器自有 cache~~ **✅ 已定（P4-T02）**：不建，直取 ODPS。

---

## 🧠 给下一个 agent 的 meta 建议

- **Batch A 全完成 + Batch B-T03 已完成、守门绿**；**直接启 P4-T04 写计划**（Batch B 续，gate 关）；**别回头改 W-phase/T03 / 别先翻闸**。
- **T04 ≠ 直接 copy legacy**：先读 SPI 写-plan 面 + W5 接线 + legacy `beginInsert`/`MaxComputeTableSink`/`MCInsertExecutor` + thrift `TMaxComputeTableSink` 再设计；**OQ-2 注入路径是真难点**。
- **T04 关键接线**：planWrite 创建 ODPS 写 session 后，经 `session.getCurrentTransaction()` 拿 `MaxComputeConnectorTransaction` 并 `setWriteSession(...)` 绑定（T03 已留槽+setter）。
- **写路径红线**：① commit 载荷 `TBinaryProtocol`（坑 3，T03 已落）；② write-context 重建（坑 2/OQ-2，T04）；③ import-gate 禁 `common.*`（坑 5）。
- **每批独立 commit**（用户定时机）；守门 maven **`-pl :fe-connector-maxcompute`**（改 fe-core 加 `:fe-core`，坑 6），读真实 BUILD/MVN_EXIT/CS_EXIT，勿信后台「exit code」通知。
- **A+B 全绿 + R-004 防御测过 → 才进 C（翻闸，唯一 live 切点）**。
